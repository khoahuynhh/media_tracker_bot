# src/agents.py
"""
Multi-Agent System for the Media Tracker Bot.
Phiên bản này sẽ cải thiện lại các prompt chi tiết để đảm bảo chất lượng phân tích,
đồng thời giữ lại yêu cầu output dạng JSON để hệ thống hoạt động bền bỉ.
"""

import asyncio
import json
import logging
import gc
import httpx
import re
import ast
import time
import random
import os
import inspect
import requests
import unicodedata
import contextlib
import urllib.parse

from datetime import datetime, date
from typing import List, Dict, Optional, Any, Tuple, Final, Sequence, Iterable
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    stop_after_delay,
    wait_random_exponential,
    before_sleep_log,
)
from urllib.parse import (
    urlparse,
    urlunparse,
    parse_qsl,
    urlencode,
    unquote,
    urljoin,
    parse_qs,
    quote,
    quote_plus,
)
from bs4 import BeautifulSoup
from asyncio import Semaphore
from collections import OrderedDict, defaultdict
from itertools import islice

# Import Agno
from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.models.groq import Groq

# from agno.models.google import Gemini  # Temporarily disabled until google-genai is properly installed
from agno.tools.crawl4ai import Crawl4aiTools
from agno.tools.googlesearch import GoogleSearchTools
from ddgs import DDGS

# Import modules
from openai import APITimeoutError
from .parsing import ArticleParser
from .event import event_bus, decision_bus, fallback_lock_by_session
from .models import (
    MediaSource,
    Article,
    IndustrySummary,
    OverallSummary,
    CompetitorReport,
    CrawlResult,
    CrawlConfig,
    BotStatus,
    ContentCluster,
    KeywordManager,
)
from .configs import CONFIG_DIR, settings
from .cache_manager import SafeCacheManager
from .task_state import task_manager
from .proxy import PROXIES

logger = logging.getLogger(__name__)

PROVIDER_MODEL_MAP = {
    "openai": {"default": "gpt-4o-mini", "report": "gpt-4o-mini"},
    "groq": {"default": "llama-3.1-70b-versatile", "report": "llama-3.1-70b-versatile"},
    "gemini": {"default": "gemini-2.0-flash", "report": "gemini-2.0-flash"},
}

http_client = httpx.AsyncClient(
    http2=True,
    timeout=50.0,
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
    transport=httpx.AsyncHTTPTransport(retries=2),
)


def get_llm_model(provider: Optional[str], model_id: Optional[str]) -> Any:
    status = settings.get_api_key_status()
    chosen = (provider or status["default_provider"] or "openai").lower()

    # NEW: ưu tiên model_id truyền vào; nếu không có thì lấy từ ENV (DEFAULT_MODEL_ID)
    mid = (
        model_id
        or os.getenv("DEFAULT_MODEL_ID")
        or PROVIDER_MODEL_MAP.get(chosen, PROVIDER_MODEL_MAP["openai"])["default"]
    )

    if chosen == "groq":
        return Groq(id=mid)
    elif chosen == "gemini":
        # return Gemini(id=mid)  # Temporarily disabled until google-genai is properly installed
        logger.warning(
            "Gemini provider requested but not available, falling back to OpenAI"
        )
        return OpenAIChat(id=mid, http_client=http_client)
    else:
        return OpenAIChat(id=mid, http_client=http_client)


# API Errors
def _configured_providers_in_order(
    settings, preferred: str | None = None
) -> tuple[list[str], bool]:
    st = settings.get_api_key_status()
    # hàm bạn đã có
    enabled_ok = {
        "openai": bool(st.get("openai_configured"))
        and getattr(settings, "openai_enabled", True),
        "groq": bool(st.get("groq_configured"))
        and getattr(settings, "groq_enabled", True),
        "gemini": bool(st.get("google_configured"))
        and getattr(settings, "google_enabled", True),
    }
    if preferred:
        p = preferred.lower()
        return [p] if enabled_ok.get(p) else []
    return [p for p, ok in enabled_ok.items() if ok]


def _map_llm_error(err: Exception) -> tuple[str, str]:
    sc = getattr(getattr(err, "response", None), "status_code", None)
    code = None
    try:
        body = getattr(err, "response", None)
        if body is not None and hasattr(body, "json"):
            j = body.json()
            code = (j.get("error") or {}).get("code")
    except Exception:
        pass

    if sc in (401, 403):
        return "PROVIDER_AUTH", "API key invalid/expired"

    if sc == 429:
        if (code or "").lower() == "insufficient_quota":
            return "PROVIDER_NO_QUOTA", "Insufficient quota"
        return "PROVIDER_RATE_LIMIT", "Rate limit"

    if isinstance(err, (asyncio.TimeoutError, httpx.ReadTimeout, httpx.ConnectTimeout)):
        return "PROVIDER_TIMEOUT", "Timeout"

    return "PROVIDER_ERROR", str(err)


# ---- LLM concurrency guard (per-event-loop) ----
# Keyed by (provider, loop_id) to avoid cross-loop binding errors
_LLM_SEMAPHORES: dict[tuple[str, int], tuple[asyncio.Semaphore, int]] = {}


def _sem_for(provider: str) -> asyncio.Semaphore:
    cap = int(os.getenv("LLM_MAX_CONCURRENCY", "2"))
    loop = asyncio.get_running_loop()
    key = (provider, id(loop))
    entry = _LLM_SEMAPHORES.get(key)
    if entry is None or entry[1] != cap:
        sem = asyncio.Semaphore(cap)
        _LLM_SEMAPHORES[key] = (sem, cap)
        return sem
    return entry[0]


# Timeout cấu hình cho mỗi call LLM
_LLM_REQ_TIMEOUT = float(os.getenv("LLM_REQUEST_TIMEOUT", "120"))

# Number of full provider rounds before aborting.
# 0 or negative means unlimited (previous behavior).
_LLM_MAX_PROVIDER_ROUNDS = int(os.getenv("LLM_MAX_PROVIDER_ROUNDS", "0"))


def ddgs_search_text(query: str, max_results=15):
    """
    Search tool chung (không ràng buộc domain).
    Dùng random UA + proxy để tránh cache/block.
    """
    ua = _random_ua()
    proxy = random.choice(PROXIES) if PROXIES else None
    # regions = ["vi-vn", "us-en", "sg-en"]
    regions = ["us-en"]
    region = random.choice(regions)

    backends = [
        "bing",
        # "brave",
        # "duckduckgo",
        # "html",
        # "mojeek",
        # "mullvad_brave",
        # "mullvad_google",
        # "yandex",
        # "yahoo",
        # "wikipedia",
    ]
    random.shuffle(backends)
    for be in backends:
        try:
            with DDGS(timeout=60) as ddg:
                rows = list(
                    ddg.text(
                        query,
                        max_results=max_results,
                        region=region,
                        safesearch="off",
                        backend="auto",
                        timelimit="y",
                    )
                )
            if rows:
                logger.info(
                    "[DDGS] backend=%s rows=%d q=%r ua=%s proxy=%s region=%s",
                    be,
                    len(rows),
                    query,
                    ua["User-Agent"],
                    proxy,
                    region,
                )
                return rows
            time.sleep(0.8 + random.random() * 0.8)
        except Exception as e:
            logger.warning("⚠️ DDGS backend=%s error=%s proxy=%s", be, e, proxy)
            time.sleep(0.8 + random.random() * 0.8)

    return []


class GoogleSearchWithDelay(GoogleSearchTools):
    def run(self, query: str):
        time.sleep(random.uniform(2.5, 5.0))  # Delay tự nhiên
        return super().run(query)


def google_cse_search_article(
    query: str,
    num: int = 10,
    keywords: Optional[List[str]] = None,
    *,
    debug: bool = True,
    dump_json_path: Optional[str] = None,  # nếu muốn ghi ra file JSON để soi
):
    """
    - Nếu `keywords` có, chỉ giữ item mà SNIPPET chứa ≥1 keyword (accent-insensitive).
    - Nếu `debug=True`, in thêm thống kê & vài dòng mẫu (title/link/snippet rút gọn).
    - Nếu `dump_json_path` được truyền, ghi toàn bộ kết quả filtered ra file JSON.
    """
    api_key = os.getenv("GOOGLE_API_KEY_SEARCH")
    if not api_key:
        logger.error("ENV GOOGLE_API_KEY_SEARCH is not set.")
        return []

    cx = os.getenv("CSE_CX") or "16c863775f52f42cd"
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": max(1, min(int(num or 10), 10)),
        "hl": "vi",
    }

    # Warn if agent forgot to pass keywords (will return unfiltered results)
    if not keywords:
        logger.warning(
            "[CSE] keywords is None/empty; returning UNFILTERED results | query=%r",
            query,
        )

    def _strip_accents(s: str) -> str:
        try:
            return "".join(
                c
                for c in unicodedata.normalize("NFD", s)
                if unicodedata.category(c) != "Mn"
            )
        except Exception:
            return s or ""

    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Google CSE request failed: %s", e, exc_info=True)
        return []

    if not isinstance(data, dict) or "error" in data:
        logger.warning(
            "Google CSE returned error or invalid payload: %r",
            data.get("error") if isinstance(data, dict) else type(data),
        )
        return []

    items = data.get("items", []) or []
    normalized = [
        {
            "title": it.get("title"),
            "link": it.get("link"),
            "snippet": it.get("snippet"),
            "displayLink": it.get("displayLink"),
            # "htmlSnippet": it.get("htmlSnippet"),  # nếu muốn debug sâu hơn
        }
        for it in items
    ]

    filtered = normalized
    if keywords:
        try:
            kw_iter = [keywords] if isinstance(keywords, str) else (keywords or [])
            kw_list = [str(k).strip() for k in kw_iter if str(k).strip()]
            kw_norm = [_strip_accents(k).lower() for k in kw_list]

            def _normalize(s: str) -> str:
                try:
                    s = "".join(
                        c
                        for c in unicodedata.normalize("NFD", s)
                        if unicodedata.category(c) != "Mn"
                    )
                except Exception:
                    s = s or ""
                s = s.lower()
                s = re.sub(r"[^a-z0-9\s]", " ", s)  # chỉ để lại a-z0-9 và space
                return re.sub(r"\s+", " ", s).strip()

            def _contains_any(title: str, snippet: str, kw_norm: list[str]) -> bool:
                base = _normalize((title or "") + " " + (snippet or ""))
                for k in kw_norm:
                    if re.search(rf"(?<!\w){re.escape(k)}(?!\w)", base):
                        return True
                return False

            # filtered = [
            #     it
            #     for it in normalized
            #     if _contains_any(
            #         it.get("title") or "", it.get("snippet") or "", kw_norm
            #     )
            # ]
        except Exception:
            logger.warning(
                "[CSE] snippet filter failed; returning unfiltered results.",
                exc_info=True,
            )
            filtered = normalized

    # optional: dump filtered to JSON file for offline inspection
    if debug and dump_json_path:
        try:
            with open(dump_json_path, "w", encoding="utf-8") as f:
                json.dump(filtered, f, ensure_ascii=False, indent=2)
            logger.debug("💾 Dumped filtered results to %s", dump_json_path)
        except Exception:
            logger.warning(
                "Failed to dump filtered results to %s", dump_json_path, exc_info=True
            )

    logger.info(
        "✅ [CSE] results=%d filtered=%d query=%r",
        len(normalized),
        len(filtered),
        query,
    )
    return filtered


class CSEArticleAgent:
    """
    Agent chuyên xử lý kết quả từ google_cse_search_article:
    - Gọi CSE với truy vấn "site:<domain> <keywords>"
    - Lọc theo domain, khử link hub và link trùng lặp
    - Tải HTML (httpx) + fallback Playwright nếu cần
    - Trích ngày, tiêu đề nhanh; nếu đủ dữ liệu thì parse nhanh, nếu không thì fallback LLM prompt ngắn
    - Trả về CrawlResult với các Article hợp lệ nằm trong khoảng thời gian yêu cầu
    """

    def __init__(
        self,
        model: Any,
        config: CrawlConfig,
        parser: ArticleParser,
        session_id: Optional[str] = None,
        check_pause_or_cancel: Optional[callable] = None,
    ):
        self.model = model
        self.config = config
        self.parser = parser
        self.session_id = session_id
        self.check_pause_or_cancel = check_pause_or_cancel or (lambda: None)
        # lightweight agent for fallback parsing
        self.agent = Agent(
            name="CSEArticleParser",
            role="Parse and extract Vietnamese news article",
            tools=[Crawl4aiTools(max_length=2000)],
            model=self.model,
            show_tool_calls=True,
            markdown=True,
        )

    @staticmethod
    def _same_domain(link: str, domain: str) -> bool:
        try:
            host = urlparse(link).netloc.lower().lstrip("www.").lstrip("m.")
            dom = re.sub(r"^https?://", "", domain or "").split("/")[0].lower()
            dom = dom.lstrip("www.").lstrip("m.")
            return host.endswith(dom)
        except Exception:
            return False

    async def run(
        self,
        media_source: MediaSource,
        keywords: List[str],
        start_date: datetime,
        end_date: datetime,
        industry_name: Optional[str] = None,
    ) -> CrawlResult:
        await _maybe_await(self.check_pause_or_cancel)

        domain = media_source.domain
        domain_url = domain
        if domain_url and not domain_url.startswith("http"):
            domain_url = f"https://{domain_url}"

        month = start_date.strftime("%m")
        year = start_date.strftime("%Y")
        keywords_str = ", ".join([k for k in (keywords or []) if k])
        query = f"site:{media_source.domain} {keywords_str}".strip()

        logger.info(
            f"[CSEArticleAgent] Query: {query} | domain={media_source.domain} mới nhất"
        )

        try:
            items = google_cse_search_article(query, num=20, keywords=keywords)
            try:
                for i, it in enumerate((items or [])[:5], 1):
                    logger.info(
                        "  [%d] %s\n      %s\n      %s",
                        i,
                        (it or {}).get("title"),
                        (it or {}).get("link"),
                        (it or {}).get("snippet"),
                    )
                # Also dump full items (truncated) for debugging
                try:
                    _payload = json.dumps(items or [], ensure_ascii=False, indent=2)
                    logger.info(
                        "[CSEArticleAgent] CSE raw items (truncated):\n%s",
                        _payload[:4000],
                    )
                except Exception:
                    pass
            except Exception:
                pass
        except Exception as e:
            logger.warning(
                f"[CSEArticleAgent] google_cse_search_article error: {e}", exc_info=True
            )
            items = []

        # Build snippet - > date map from CSE items
        def _parse_vi_date_to_iso(text: str | None) -> str | None:
            if not text:
                return None
            m = _VN_DATE_RE.search(text)
            if m:
                try:
                    d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
                    return f"{y:04d}-{mo:02d}-{d:02d}"
                except Exception:
                    pass
            m2 = _VN_DATE_RE_TEXT.search(text)
            if m2:
                try:
                    d, mo, y = (
                        int(m2.group("d")),
                        int(m2.group("m")),
                        int(m2.group("y")),
                    )
                    return f"{y:04d}-{mo:02d}-{d:02d}"
                except Exception:
                    pass
            return None

        snippet_date_map: dict[str, tuple[str, str]] = {}
        snippet_title_map: dict[str, str] = {}
        snippet_map: dict[str, str] = {}

        def _normalize_text(s: str | None) -> str:
            if not s:
                return ""
            s = unicodedata.normalize("NFD", s)
            s = "".join(c for c in s if unicodedata.category(c) != "Mn")
            s = s.lower()
            s = re.sub(r"[^a-z0-9\s]", " ", s)
            return re.sub(r"\s+", " ", s).strip()

        def _kw_in_text(kw: str, text: str) -> bool:
            tn = _normalize_text(text)
            kn = _normalize_text(kw)
            return (
                bool(kn) and re.search(rf"(?<!\w){re.escape(kn)}(?!\w)", tn) is not None
            )

        def _brands_from_snippet(
            link: str, keywords: list[str]
        ) -> tuple[list[str], bool]:
            sn = snippet_map.get(link) or snippet_title_map.get(link) or ""
            matched = [kw for kw in (keywords or []) if _kw_in_text(kw, sn)]
            return list(dict.fromkeys(matched))  # dedup, giữ thứ tự

        links = []
        for it in items or []:
            link = (it or {}).get("link")
            if not link:
                continue
            if not self._same_domain(link, media_source.domain):
                continue
            # NEW: loại hub/tag links (đã xử lý ở HubCrawlTool)
            if is_tag_hub_url(link, keywords):
                logger.info("[CSEArticleAgent] skip hub link from CSE: %s", link)
                continue
            sn = (it or {}).get("snippet") or ""
            tt = (it or {}).get("title") or ""
            if sn:
                snippet_map[link] = sn
            iso = _parse_vi_date_to_iso(sn)
            if iso:
                snippet_date_map[link] = (iso, sn or "")
            if tt:
                snippet_title_map[link] = tt
            links.append(link)

        # Dedup while preserving order
        seen = set()
        links = [l for l in links if not (l in seen or seen.add(l))]

        # Log snippet-date map summary
        try:
            logger.info(
                "[CSEArticleAgent] snippet_date_map entries=%d",
                len(snippet_date_map),
            )
            for i, (lk, (iso, sn)) in enumerate(list(snippet_date_map.items())[:5], 1):
                logger.info("  [%d] %s -> %s | snip: %s", i, lk, iso, (sn or "")[:160])
        except Exception:
            pass

        logger.info(
            "[CSEArticleAgent] domain=%s | picked %d links from CSE",
            media_source.domain,
            len(links),
        )
        for i, l in enumerate(links[:10], 1):
            logger.info("  (link %d) %s", i, l)

        if not links:
            return CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url=domain_url or "",
                articles_found=[],
                crawl_status="failed",
                error_message="No CSE links for this domain/keywords",
                crawl_duration=0.0,
            )

        sem = Semaphore(4)
        start_t = time.monotonic()

        async def _retry_process_link(
            process_link, link: str, max_retries: int = 2
        ) -> list:
            for attempt in range(max_retries + 1):
                try:
                    parsed = await process_link(link)
                    if parsed:
                        return parsed
                    raise RuntimeError("Parser returned empty list")
                except Exception as e:
                    if attempt >= max_retries:
                        logger.warning(
                            f"[CSEArticleAgent] Parse fail {link}: {e} (exhausted retries)"
                        )
                        return []
                    delay = min(2**attempt, 8) + random.random() * 0.5
                    logger.info(
                        f"[CSEArticleAgent][retry] parse {link} attempt {attempt + 1}/{max_retries} in {delay:.1f}s: {e}"
                    )
                    await asyncio.sleep(delay)

        async def process_link(link: str):
            await _maybe_await(self.check_pause_or_cancel)
            try:
                dom = urlparse(link).netloc
                await DOMAIN_LIMITER.enter(link)
                r = await http_client.get(
                    link,
                    timeout=15,
                    follow_redirects=True,
                    headers={
                        **_random_ua(),
                        **_rand_headers(),
                        "Referer": f"https://{dom}/",
                    },
                )
            finally:
                DOMAIN_LIMITER.release(dom)

            content_type = r.headers.get("content-type", "")
            art_html = (
                r.text if (r.status_code == 200 and "text/html" in content_type) else ""
            )

            logger.info(
                "[CSEArticleAgent] GET %s -> %s | ct=%s | len=%d",
                link,
                getattr(r, "status_code", "-"),
                content_type,
                len(r.text or ""),
            )

            if (
                (not art_html)
                or (len(art_html) < MIN_HTML_LEN_HEUR)
                or _looks_blocked(art_html)
            ):
                art_html = await crawl_with_playwright(link)

            meta = extract_dates_rule_based(art_html, link)
            title_rb = extract_title_rule_based(art_html)
            cse_title = snippet_title_map.get(link)

            parsed = None

            # Strict mode quick-path: use HTML date or CSE snippet date with best available title, avoid LLM
            try:
                sn_iso0, sn_src0 = snippet_date_map.get(link, (None, None))
                meta_date = meta.get("published_iso")  # Ngày từ meta
                chosen_iso0 = meta_date or (
                    sn_iso0 if getattr(self, "strict_snippet_date", True) else None
                )

                # So sánh ngày từ meta và snippet, lấy ngày nhỏ hơn
                if meta_date and sn_iso0:
                    meta_date_parsed = _parse_date_soft(meta_date)
                    sn_iso0_parsed = _parse_date_soft(sn_iso0)

                    # Chọn ngày nào nhỏ hơn (cũ hơn)
                    if meta_date_parsed and sn_iso0_parsed:
                        chosen_iso0 = min(
                            meta_date_parsed, sn_iso0_parsed
                        ).isoformat()  # Lấy ngày nhỏ hơn

                # chosen_iso0 = meta.get("published_iso") or (
                #     sn_iso0 if getattr(self, "strict_snippet_date", True) else None
                # )
                used_title0 = title_rb or cse_title
                logger.info(
                    "[CSEArticleAgent] Decision for %s: chosen_iso=%s | title=%r",
                    link,
                    chosen_iso0,
                    (used_title0 or "")[:120],
                )
                if chosen_iso0 and used_title0:
                    summary0 = (
                        _quick_summary_from_html(art_html)
                        if art_html
                        else (sn_src0 or "")
                    )
                    matched_brands = _brands_from_snippet(link, keywords)
                    quick_json0 = json.dumps(
                        {
                            "Tiêu đề": used_title0,
                            "Ngày phát hành": chosen_iso0,
                            "Nguồn trích ngày": meta.get("source_published_text")
                            or (sn_src0 or ""),
                            "Tóm tắt": summary0,
                            "Link": link,
                            "nhan_hang": matched_brands,
                        },
                        ensure_ascii=False,
                    )
                    try:
                        logger.info(
                            "[CSEArticleAgent] quick_json0 for %s (truncated):\n%s",
                            link,
                            (quick_json0 or "")[:1200],
                        )
                    except Exception:
                        pass
                    parsed = self.parser.parse(quick_json0, media_source, industry_name)
                    logger.info(f"Parsed output for {link}: {parsed}")
                    # Enrich with provided keywords to help ProcessorAgent detect brands
                    try:
                        arr = parsed if isinstance(parsed, list) else [parsed]
                        for a in arr:
                            if getattr(a, "keywords_found", None) in (None, []):
                                a.keywords_found = list(keywords or [])
                            if getattr(a, "nhan_hang", None) in (None, []):
                                a.nhan_hang = [
                                    kw
                                    for kw in (keywords or [])
                                    if isinstance(kw, str) and kw[:1].isupper()
                                ]
                    except Exception:
                        pass
            except Exception:
                parsed = None

            # Quick path using CSE snippet date if HTML metadata missing
            try:
                if not meta.get("published_iso") and title_rb and len(art_html) >= 1500:
                    sn_iso, sn_src = snippet_date_map.get(link, (None, None))
                    if sn_iso:
                        matched_brands = _brands_from_snippet(link, keywords)
                        quick_json2 = json.dumps(
                            {
                                "Tiêu đề": title_rb,
                                "Ngày phát hành": sn_iso,
                                "Nguồn trích ngày": sn_src or "",
                                "Tóm tắt": _quick_summary_from_html(art_html),
                                "Link": link,
                                "nhan_hang": matched_brands,
                            },
                            ensure_ascii=False,
                        )
                        try:
                            logger.info(
                                "[CSEArticleAgent] quick_json2 for %s (truncated):\n%s",
                                link,
                                (quick_json2 or "")[:1200],
                            )
                        except Exception:
                            pass
                        parsed = self.parser.parse(
                            quick_json2, media_source, industry_name
                        )
                        try:
                            arr = parsed if isinstance(parsed, list) else [parsed]
                            for a in arr:
                                if getattr(a, "keywords_found", None) in (None, []):
                                    a.keywords_found = list(keywords or [])
                                if getattr(a, "nhan_hang", None) in (None, []):
                                    a.nhan_hang = [
                                        kw
                                        for kw in (keywords or [])
                                        if isinstance(kw, str) and kw[:1].isupper()
                                    ]
                        except Exception:
                            pass
            except Exception:
                parsed = None

            def _is_valid_article(obj) -> bool:
                if not obj:
                    return False
                if isinstance(obj, list):
                    if not obj:
                        return False
                    obj = obj[0]
                try:
                    t = (
                        getattr(obj, "cum_noi_dung_chi_tiet", None)
                        or getattr(obj, "tieu_de", None)
                        or getattr(obj, "Tiêu đề", None)
                    )
                    d = getattr(obj, "ngay_phat_hanh", None) or getattr(
                        obj, "Ngày phát hành", None
                    )
                    l = getattr(obj, "link_bai_bao", None) or getattr(obj, "Link", None)
                    return bool(t and d and l)
                except Exception:
                    return False

            if meta.get("published_iso") and title_rb and len(art_html) >= 1500:
                matched_brands = _brands_from_snippet(link, keywords)
                quick_json = json.dumps(
                    {
                        "Tiêu đề": title_rb,
                        "Ngày phát hành": meta["published_iso"],
                        "Nguồn trích ngày": meta.get("source_published_text") or "",
                        "Tóm tắt": _quick_summary_from_html(art_html),
                        "Link": link,
                        "nhan_hang": matched_brands,
                    },
                    ensure_ascii=False,
                )
                try:
                    parsed = self.parser.parse(quick_json, media_source, industry_name)
                except Exception:
                    parsed = None

            if not _is_valid_article(parsed):
                if meta.get("published_iso"):
                    prompt = HubCrawlTool.build_prompt_with_known_date(
                        self=None,
                        link=link,
                        known_date_iso=meta["published_iso"],
                        known_date_source=meta.get("source_published_text") or "",
                        known_title=title_rb,
                    )
                elif link in snippet_date_map:
                    sn_iso, sn_src = snippet_date_map.get(link, (None, None))
                    missing_title = not (title_rb or cse_title)
                    html_short = not art_html or len(art_html) < MIN_HTML_LEN_RESULT
                    if getattr(self, "strict_snippet_date", True) and not (
                        missing_title or html_short
                    ):
                        # Avoid LLM when quick parse produced a valid article
                        if _is_valid_article(parsed):
                            return parsed
                    prompt = HubCrawlTool.build_prompt_with_known_date(
                        self=None,
                        link=link,
                        known_date_iso=(sn_iso or ""),
                        known_date_source=sn_src or "",
                        known_title=(title_rb or cse_title),
                    )
                else:
                    prompt = self._build_cse_prompt(link, start_date, end_date)

                resp = await self.agent.arun(prompt, session_id=self.session_id)
                text = await _get_response_text(resp)
                try:
                    logger.info(
                        "[CSEArticleAgent] LLM response for %s:\n%s",
                        link,
                        (text or "")[:1200],
                    )
                except Exception:
                    pass
                parsed = self.parser.parse(text, media_source, industry_name)
                try:
                    arr = parsed if isinstance(parsed, list) else [parsed]
                    for a in arr:
                        if getattr(a, "keywords_found", None) in (None, []):
                            a.keywords_found = list(keywords or [])
                        if getattr(a, "nhan_hang", None) in (None, []):
                            a.nhan_hang = [
                                kw
                                for kw in (keywords or [])
                                if isinstance(kw, str) and kw[:1].isupper()
                            ]
                except Exception:
                    pass

            return parsed

        tasks = []
        for link in links[:10]:

            async def worker(l=link):
                async with sem:
                    return await _retry_process_link(process_link, l, max_retries=2)

            tasks.append(asyncio.create_task(worker()))

        parsed_lists = await asyncio.gather(*tasks)
        all_articles = [a for lst in parsed_lists for a in lst]

        def to_date(x):
            if isinstance(x, datetime):
                return x.date()
            if isinstance(x, date):
                return x
            if isinstance(x, str):
                try:
                    return datetime.fromisoformat(x).date()
                except Exception:
                    m = _VN_DATE_RE.search(x)
                    if m:
                        d, mo, y = (
                            int(m.group("d")),
                            int(m.group("m")),
                            int(m.group("y")),
                        )
                        return date(y, mo, d)
            return None

        def in_range(a: Article):
            pub = to_date(a.ngay_phat_hanh)
            return pub is not None and start_date.date() <= pub <= end_date.date()

        filtered = [a for a in all_articles if in_range(a)]
        duration = time.monotonic() - start_t

        return CrawlResult(
            source_name=media_source.name,
            source_type=media_source.type,
            url=domain_url or "",
            articles_found=filtered,
            crawl_status="success" if filtered else "failed",
            error_message="" if filtered else "No valid articles within date range",
            crawl_duration=duration,
        )

    def _build_cse_prompt(
        self, link: str, start_date: datetime, end_date: datetime
    ) -> str:
        date_filter = f"từ ngày {start_date.strftime('%Y-%m-%d')} đến ngày {end_date.strftime('%Y-%m-%d')}"
        return f"""
        Truy cập URL: {link} và phân tích bài báo. TRẢ VỀ DUY NHẤT MỘT OBJECT JSON theo schema dưới đây (không markdown, không giải thích):

        {{
        "Tiêu đề": "Tiêu đề đầy đủ của bài viết",
        "Ngày phát hành": "YYYY-MM-DD hoặc null",
        "Nguồn trích ngày": "Chuỗi ngày/giờ NGUYÊN VĂN bạn tìm thấy trong nội dung (ví dụ: '31/07/2025 11:00 (GMT+7)')",
        "Ngày cập nhật": "YYYY-MM-DD hoặc null",
        "Tóm tắt": "≤ 100 từ: sự kiện chính, các bên liên quan, kết quả/tác động",
        "Link": "{link}"
        }}

        QUY TẮC LẤY NGÀY (BẮT BUỘC):
        1) CHỈ CHẤP NHẬN ngày nằm TRONG nội dung bài viết hoặc metadata của CHÍNH trang bài viết:
           - JSON-LD Article.datePublished / dateCreated
           - meta[property=article:published_time] / article:modified_time
           - thẻ <time datetime="...">
        2) TUYỆT ĐỐI KHÔNG dùng ngày từ header/top bar/menu/footer/sidebar/breadcrumb hoặc phần giao diện (UI) của trang.
        3) Nếu có nhiều mốc (đăng/cập nhật), ƯU TIÊN "ngày đăng gốc" (published). "Ngày cập nhật" chỉ điền khi thực sự tìm thấy mốc cập nhật trong nội dung.
        4) Chỉ nhận bài trong khoảng {date_filter}. Nếu ngày phát hành ngoài khoảng, vẫn trả JSON nhưng "Ngày phát hành" PHẢI là đúng ngày bạn tìm thấy (không tự thay đổi).
        5) Định dạng ngày bắt buộc: YYYY-MM-DD.
        6) NẾU KHÔNG TÌM THẤY ngày hợp lệ theo (1)-(2) THÌ ĐẶT "Ngày phát hành": null. TUYỆT ĐỐI KHÔNG dùng ngày hiện tại hoặc bịa ngày.
        """


def google_cse_search(domain, industry_name, keywords):
    API_KEY = os.getenv("GOOGLE_API_KEY_SEARCH")
    if not API_KEY:
        logger.error("❌ GOOGLE_API_KEY_SEARCH chưa được set.")
        return None

    # Chuẩn hóa domain cho site: query
    norm_domain = re.sub(r"^https?://", "", domain).split("/")[0].lower()
    norm_domain = norm_domain.lstrip("www.")

    # Tăng khả năng bắt hub: thêm OR cho tag/tags/chu-de
    q_parts = (
        [f"site:{norm_domain}", industry_name] + list(keywords) + ["tin tức mới nhất"]
    )
    query = " ".join(filter(None, q_parts))

    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": API_KEY, "cx": "16c863775f52f42cd", "q": query, "num": 10}

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("❌ Google CSE lỗi: %s", e)
        return None

    items = data.get("items", [])
    if not items:
        logger.warning("❌ Không tìm thấy kết quả: %r", data)
        return None

    # --- helpers ---
    NUM_RE = re.compile(r"(\d{2,})")  # lấy số >= 2 chữ số để tránh noise

    def extract_numeric_id(u: str) -> int:
        try:
            path = urlparse(u).path
            nums = NUM_RE.findall(path)
            return int(nums[-1]) if nums else -1  # lấy số cuối trong path
        except Exception:
            return -1

    def kw_hits(u: str) -> int:
        lu = u.lower()
        return sum(1 for kw in keywords if kw.lower() in lu)

    def is_same_domain(u: str) -> bool:
        try:
            host = urlparse(u).netloc.lower()
        except Exception:
            return False
        host = host.lstrip("www.").lstrip("m.")
        return host.endswith(norm_domain)

    # Lọc candidates: đúng domain + là hub + có keyword trong URL
    candidates: list[str] = []
    for it in items:
        link = it.get("link", "")
        if not link:
            continue
        if not is_same_domain(link):
            continue
        lu = link.lower()
        if is_tag_hub_url(lu, keywords):
            candidates.append(link)
            logger.debug("CSE candidate: %s", link)

    if not candidates:
        logger.warning("❌ Không có hub hợp lệ trong kết quả CSE.")
        return None

    # Chọn theo: có số? → ID lớn → số keyword khớp → prefer /tags/ → URL ngắn
    def hub_sort_key(u: str):
        lu = u.lower()
        uid = extract_numeric_id(lu)
        has_num = 1 if uid >= 0 else 0
        prefer_tags = 1 if "/tags/" in lu else 0
        return (has_num, uid, kw_hits(lu), prefer_tags, -len(u))

    pick = sorted(set(candidates), key=hub_sort_key, reverse=True)[0]
    logger.info("✅ Chọn hub từ CSE: %s", pick)
    return pick


# DDGS Helpers
def _norm_host(h):
    h = h.lower()
    return h[4:] if h.startswith("www.") else h


def clean_duck_href(href: str) -> str | None:
    if not href:
        return None

    if "zhihu.com/tardis/" in href or "zhihu.com/question/" in href:
        return None

    # 1) Relative -> absolute
    href = urljoin("https://duckduckgo.com", href)

    p = urlparse(href)
    # 2) DuckDuckGo redirect: lấy URL thật từ query
    if p.netloc.endswith("duckduckgo.com") and p.path.startswith(("/l/", "/r/")):
        q = parse_qs(p.query)
        for key in ("uddg", "u", "rut"):
            if q.get(key):
                href = unquote(q[key][0])
                p = urlparse(href)
                break

    # 3) Google Translate proxy -> lấy param 'u'
    if p.netloc.endswith("googleusercontent.com") and p.path.startswith("/translate"):
        q = parse_qs(p.query)
        if q.get("u"):
            href = unquote(q["u"][0])
            p = urlparse(href)

    # 4) Gọn AMP/mobile
    path = p.path.replace("/amp/", "/").replace("/amp", "/")
    host = p.netloc.lower()
    if host.startswith("www."):
        host = host[4:]

    # 5) Lắp lại URL đã chuẩn hoá
    return urlunparse((p.scheme or "https", host, path, "", p.query, ""))


# --- Anti-block constants & helpers ---
BLOCK_PATTERNS = (
    "access denied",
    "forbidden",
    "blocked",
    "captcha",
    "cloudflare",
    "attention required",
    "unusual traffic",
    "verify you are a human",
    "challenge",
    "bot detection",
)


def _rand_headers() -> dict:
    # “browsery” headers: Accept/Language + Sec-Fetch
    return {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def _looks_blocked(text: str | None) -> bool:
    if not text:
        return True
    low = text.lower()
    return any(k in low for k in BLOCK_PATTERNS)


def _random_ua() -> dict:
    """Sinh User-Agent ngẫu nhiên cho mỗi query"""
    return {
        "User-Agent": (
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            f"Chrome/{random.randint(118, 124)}.0.{random.randint(1000,9999)}.100 Safari/537.36"
        )
    }


class DomainPolicy:
    def __init__(self, max_concurrent: int = 2, min_gap_sec: float = 1.2):
        self.sem = asyncio.Semaphore(max_concurrent)
        self.min_gap = float(min_gap_sec)
        self.last_t = 0.0


class DomainLimiter:
    def __init__(self):
        self._policies: dict[str, DomainPolicy] = {}

    def policy(self, domain: str) -> DomainPolicy:
        if domain not in self._policies:
            # mặc định: 2 luồng/host, gap ~0.8s giữa các request
            self._policies[domain] = DomainPolicy(2, 0.8)
        return self._policies[domain]

    async def enter(self, url: str):
        d = urlparse(url).netloc
        pol = self.policy(d)
        await pol.sem.acquire()
        # enforce gap
        now = time.monotonic()
        wait = pol.min_gap - (now - pol.last_t)
        if wait > 0:
            await asyncio.sleep(wait + 0.1 * wait)  # thêm tí jitter
        pol.last_t = time.monotonic()
        return d  # trả domain để caller release sau

    def release(self, domain: str):
        self._policies[domain].sem.release()


DOMAIN_LIMITER = DomainLimiter()
# ---------------------------------------


# Use for HubCrawlTool
def search_domain_duckduckgo(
    domain: str,
    industry_name: str,
    keywords: list[str],
    max_results: int,
    query_type: str | None = None,
):
    """
    Tìm kiếm hub/article link trong domain cụ thể, chạy đủ 2 query:
      1) site:{domain} {industry_name} {keywords} {suffix}
      2) site:{domain} {keywords} {suffix}

    Trả về: (acc_rows, hub_links, backend, query_joined)
      - acc_rows: danh sách kết quả DDG gộp (đã làm sạch href)
      - hub_links: các URL hub (tags/chu-de/tu-khoa...) duy nhất, ưu tiên đúng domain và có chứa keyword
      - backend: chuỗi tên backend (ví dụ "duckduckgo")
      - query_joined: chuỗi nối các query đã chạy (để log/debug)
    """
    salt = random.randint(1000, 9999)
    suffixes = ["tag", "tags", "tin tức mới nhất"]  # có thể mở rộng
    if query_type == "tag":
        suffix = "tag"
    elif query_type == "news":
        suffix = "tin tức mới nhất"
    else:
        suffix = random.choice(suffixes)

    # chạy đúng 2 query như yêu cầu
    queries = [
        f"site:{domain} {industry_name} {' '.join(keywords)} {suffix}",
        f"site:{domain} {' '.join(keywords)} {suffix}",
    ]
    region = "us-en"
    ua = _random_ua()
    proxy = random.choice(PROXIES) if PROXIES else None

    backends = ["duckduckgo", "google"]
    random.shuffle(backends)

    wanted_host = _norm_host(domain)
    acc_rows: list[dict] = []
    seen_clean: set[str] = set()

    def _same_domain(u: str) -> bool:
        try:
            host = _norm_host(urlparse(u).netloc)
        except Exception:
            return False
        return host == wanted_host

    for be in backends:
        try:
            with DDGS(timeout=60) as ddg:
                for q in queries:
                    rows = list(
                        ddg.text(
                            q,
                            region=region,
                            max_results=max_results,
                            safesearch="off",
                            backend="auto",
                            timelimit="y",
                        )
                    )
                    if not rows:
                        continue

                    for r in rows:
                        href = clean_duck_href(r.get("href"))
                        if not href or href in seen_clean:
                            continue
                        # giữ lại href đã làm sạch để dùng về sau
                        r = dict(r)
                        r["href"] = href
                        acc_rows.append(r)
                        seen_clean.add(href)

            if acc_rows:
                # ✅ LOG & trả về acc_rows (sửa bug trước kia bị return 'rows' cuối)
                logger.info(
                    "[DDG] domain=%s backend=%s rows=%d queries=%r ua=%s proxy=%s region=%s",
                    domain,
                    be,
                    len(acc_rows),
                    " | ".join(queries),
                    ua["User-Agent"],
                    proxy,
                    region,
                )
                # Tách hub links từ acc_rows
                hubs = []
                seen_hub = set()
                for r in acc_rows:
                    u = r["href"]
                    if (
                        _same_domain(u)
                        and is_tag_hub_url(u, keywords)
                        and u not in seen_hub
                    ):
                        hubs.append(u)
                        seen_hub.add(u)
                return acc_rows, hubs, be, " | ".join(queries)

            time.sleep(0.8 + random.random() * 0.8)  # jitter
        except Exception as e:
            logger.warning("⚠️ DDG backend=%s error=%s proxy=%s", be, e, proxy)
            time.sleep(0.8 + random.random() * 0.8)

    return [], [], None, " | ".join(queries)


def get_hub_links_for_domain(
    domain: str,
    keywords: list[str],
    industry_name: str,
    max_results: int = 30,
    limit: int | None = 3,
) -> tuple[list[str], bool]:
    """
    Tìm nhiều hub cho 1 domain bằng cách chạy đủ 2 kiểu query ('tag' và 'news'),
    gom & xếp hạng hub giống logic pick_from_rows trong get_first_search_link.
    Trả về danh sách hub URLs (đã dedup), ưu tiên /tags/ + nhiều keyword.
    """
    wanted_host = _norm_host(domain)
    NUM_RE = re.compile(r"(\d{2,})")  # bỏ noise 1 chữ số

    def extract_numeric_id(u: str) -> int:
        try:
            path = urlparse(u).path
        except Exception:
            return -1
        nums = NUM_RE.findall(path)
        return int(nums[-1]) if nums else -1  # lấy số cuối trong path

    def same_domain(u: str) -> bool:
        try:
            host = _norm_host(urlparse(u).netloc)
        except Exception:
            return False
        return host == wanted_host

    def rank_hub(u: str):
        lu = u.lower()
        uid = extract_numeric_id(lu)
        has_num = 1 if uid >= 0 else 0
        kw_hits = sum(1 for kw in keywords if kw.lower() in lu)
        prefer_tags = 1 if "/tags/" in lu else 0
        # sort desc theo: có số id, id, số keyword khớp, prefer /tags/, và URL ngắn
        return (has_num, uid, kw_hits, prefer_tags, -len(u))

    def scan_rows(rows: list[dict]) -> tuple[list[str], list[str]]:
        """Trả về (same_domain_urls, hub_candidates) từ 1 mẻ rows."""
        same_dom, hubs = [], []
        for r in rows or []:
            raw = r.get("href")
            href = clean_duck_href(raw)
            if not href:
                continue
            if not same_domain(href):
                continue
            same_dom.append(href)
            if is_tag_hub_url(href, keywords):
                hubs.append(href)
        return same_dom, hubs

    all_hubs: list[str] = []
    seen_hubs: set[str] = set()
    seen_same_domain = False

    # chạy đủ 2 kiểu query như get_first_search_link làm
    for qtype in ("tag", "news"):
        pack = search_domain_duckduckgo(
            domain=domain,
            industry_name=industry_name,
            keywords=list(keywords),
            max_results=max_results,
            query_type=qtype,
        )

        # Tương thích 2 phiên bản search_domain_duckduckgo:
        # - cũ: (rows, be, used_q)
        # - mới (đề xuất): (acc_rows, hubs_from_func, be, used_q)
        rows: list[dict] = []
        hubs_from_func: list[str] = []
        be = used_q = None

        if isinstance(pack, tuple):
            if len(pack) == 3:
                rows, be, used_q = pack
            elif len(pack) == 4:
                rows, hubs_from_func, be, used_q = pack
            else:
                rows = pack[0] if pack else []
        else:
            rows = pack or []

        logger.info(
            "[get_hub_links_for_domain] qtype=%s backend=%s rows=%d hubs_from_func=%d queries=%r",
            qtype,
            be,
            len(rows) if rows else 0,
            len(hubs_from_func),
            used_q,
        )

        # Ưu tiên dùng chính các hub mà hàm search đã nhận diện được (nếu có)
        for h in hubs_from_func:
            if same_domain(h) and is_tag_hub_url(h, keywords) and h not in seen_hubs:
                all_hubs.append(h)
                seen_hubs.add(h)

        # Sau đó quét lại toàn bộ rows bằng logic pick_from_rows cũ
        same_dom, hubs = scan_rows(rows)
        seen_same_domain |= bool(same_dom)
        for h in hubs:
            if h not in seen_hubs:
                all_hubs.append(h)
                seen_hubs.add(h)

    # Nếu chưa có hub nào: fallback CSE
    if not all_hubs:
        if seen_same_domain:
            logger.warning(
                "⚠️ Đã thấy URL cùng domain nhưng chưa có hub hợp lệ → thử CSE"
            )
        else:
            logger.warning("⚠️ Chưa thấy URL cùng domain → thử CSE")
        cse = google_cse_search(domain, industry_name, keywords)
        if cse:
            all_hubs = [cse]

    # Xếp hạng & dedup (đã dedup rồi nhưng cứ chắc ăn)
    all_hubs = sorted(set(all_hubs), key=rank_hub, reverse=True)

    if limit is not None and limit > 0:
        all_hubs = all_hubs[:limit]

    logger.info(
        "[get_hub_links_for_domain] ✅ found=%d (limit=%s) → %s",
        len(all_hubs),
        limit,
        all_hubs,
    )
    return all_hubs


def get_first_search_link(
    domain: str, keywords: list[str], industry_name: str
) -> str | None:
    wanted_host = _norm_host(domain)
    found_domain_but_no_hub = False

    def pick_from_rows(rows: list[dict]) -> tuple[str | None, bool]:
        """Trả về (link, has_same_domain)"""
        same_domain: list[str] = []
        hubs: list[str] = []

        NUM_RE = re.compile(r"(\d{2,})")  # bỏ qua số 1 chữ số (noise)

        def extract_numeric_id(u: str) -> int:
            try:
                path = urlparse(u).path
            except Exception:
                return -1
            nums = NUM_RE.findall(path)
            return int(nums[-1]) if nums else -1  # lấy số cuối trong path

        for r in rows:
            raw = r.get("href")
            href = clean_duck_href(raw)
            if not href:
                continue
            host = _norm_host(urlparse(href).netloc)
            if host != wanted_host:
                continue

            same_domain.append(href)

            # Hub hợp lệ = có /tag|/tags|/chu-de + có keyword trong URL
            if is_tag_hub_url(href, keywords):
                hubs.append(href)

        has_same_domain = bool(same_domain)

        # Ưu tiên hub hợp lệ
        if hubs:

            def hub_sort_key(u: str):
                lu = u.lower()
                uid = extract_numeric_id(lu)
                has_num = 1 if uid >= 0 else 0
                kw_hits = sum(1 for kw in keywords if kw.lower() in lu)
                prefer_tags = 1 if "/tags/" in lu else 0
                return (has_num, uid, kw_hits, prefer_tags, -len(u))

            # unique + sort giảm dần theo key
            hubs = sorted(set(hubs), key=hub_sort_key, reverse=True)
            return hubs[0], has_same_domain

        # Nếu không có hub thì trả None
        # Nếu không có hub: lấy link cùng domain có chứa keyword (ưu tiên URL ngắn)
        if has_same_domain:

            def _kw_hits(u: str) -> int:
                lu = u.lower()
                return sum(1 for kw in keywords if (kw or "").lower() in lu)

            kw_only = [u for u in same_domain if _kw_hits(u) > 0]
            if kw_only:
                pick = sorted(
                    kw_only, key=lambda u: (_kw_hits(u), -len(u)), reverse=True
                )[0]
                return pick, has_same_domain

        return None, has_same_domain

    # Danh sách các query type để thử luân phiên
    query_types = ["tag", "news"]

    # --- Retry loop ---
    max_retry = 2
    for attempt in range(max_retry):
        # Thay query
        query_type = query_types[attempt % len(query_types)]
        rows, be, used_q = search_domain_duckduckgo(
            domain,
            industry_name,
            list(keywords),
            max_results=30,
            query_type=query_type,
        )

        logger.info(
            "[get_first_search_link] try #%d backend=%s query=%r rows=%d",
            attempt + 1,
            be,
            used_q,
            len(rows) if rows else 0,
        )

        if rows:
            for i, r in enumerate(rows, 1):
                raw = r.get("href")
                href = clean_duck_href(raw)
                logger.info("  [%d] raw=%s | cleaned=%s", i, raw, href)

            link, has_same_domain = pick_from_rows(rows)
            if link:
                return link

            # Nếu có link cùng domain nhưng không phải hub → dừng
            if has_same_domain:
                found_domain_but_no_hub = True

        # Nếu chưa tìm thấy link nào đúng domain → retry
        delay = 2 * (2**attempt) + random.random()
        if found_domain_but_no_hub:
            logger.warning(
                "⚠️ Đã thấy domain nhưng chưa tìm thấy hub, chờ %.1fs rồi thử lại (attempt %d/%d)...",
                delay,
                attempt + 1,
                max_retry,
            )
        else:
            logger.warning(
                "⚠️ Chưa tìm thấy link nào cùng domain, chờ %.1fs rồi thử lại...", delay
            )

        time.sleep(delay)

    # --- Sau 3 lần retry ---
    if found_domain_but_no_hub:
        logger.warning(
            "⚠️ Đã tìm thấy domain %s sau %d lần nhưng không có hub hợp lệ → bỏ qua",
            domain,
            max_retry,
        )
    else:
        logger.warning(
            "⚠️ Không tìm thấy hub hợp lệ trong %d lần thử, fallback Google CSE: %s",
            max_retry,
            domain,
        )
        return google_cse_search(domain, industry_name, keywords)


class PlaywrightPool:
    _instance = None

    @classmethod
    def instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._started = False
        self._pw = None
        self._browser = None
        self._sem = asyncio.Semaphore(3)  # tổng số page song song
        self._storage_state: dict[str, dict] = {}  # per-domain storage_state
        self._loop = None  # event loop that owns Playwright objects

    async def start(self):
        if self._started:
            return
        # Bind to current running loop
        self._loop = asyncio.get_running_loop()
        # Recreate per-loop semaphores to avoid cross-loop binding
        self._sem = asyncio.Semaphore(2)
        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-blink-features=AutomationControlled",
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
            ],
        )
        self._started = True

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()
        self._started = False
        self._browser = None
        self._pw = None
        self._loop = None

    async def fetch(
        self, url: str, timeout_ms: int = 12000, referer: str | None = None
    ) -> str:
        # Ensure Playwright objects belong to current loop; recreate if loop switched
        cur_loop = asyncio.get_running_loop()
        if getattr(self, "_loop", None) is not None and self._loop is not cur_loop:
            with contextlib.suppress(Exception):
                await self.close()
        await self.start()
        if not self._pw or not self._browser:
            # Browser was closed unexpectedly → restart
            await self.close()
            await self.start()
        domain = urlparse(url).netloc
        # Ensure browser connection is healthy; recreate on stale connection
        try:
            if (
                hasattr(self._browser, "is_connected")
                and not self._browser.is_connected()
            ):
                await self.close()
                await self.start()
        except Exception:
            with contextlib.suppress(Exception):
                await self.close()
            await self.start()
        await DOMAIN_LIMITER.enter(url)
        try:
            async with self._sem:
                storage_state = self._storage_state.get(domain)
                ua = _random_ua()

                async def _new_context():
                    return await self._browser.new_context(
                        viewport={"width": 1366, "height": 768},
                        user_agent=ua["User-Agent"],
                        storage_state=storage_state,
                        extra_http_headers={
                            **_rand_headers(),
                            **({"Referer": referer} if referer else {}),
                        },
                        locale="vi-VN",
                    )

                try:
                    context = await _new_context()
                except Exception:
                    with contextlib.suppress(Exception):
                        await self.close()
                    await self.start()
                    context = await _new_context()
                # default timeouts để đề phòng
                context.set_default_navigation_timeout(timeout_ms)
                context.set_default_timeout(int(timeout_ms * 1.2))

                await context.add_init_script("""/* stealth như cũ */""")

                page = await context.new_page()

                # chặn tài nguyên nặng
                async def _route(route):
                    rt = route.request.resource_type
                    if rt in ("image", "media", "font"):
                        return await route.abort()
                    await route.continue_()

                await context.route("**/*", _route)

                blocked = False
                html = ""

                try:
                    # 1) Lần 1: DOMContentLoaded (+ nhịp chờ ngẫu nhiên ngắn)
                    resp = await page.goto(
                        url, timeout=timeout_ms, wait_until="domcontentloaded"
                    )
                    status = resp.status if resp else None
                    if status and status >= 400:
                        raise RuntimeError(f"HTTP {status}")

                    await page.wait_for_timeout(300 + int(200 * random.random()))

                    # Heuristic: cố gắng chờ thấy ≥3 anchor ứng viên bài viết
                    try:
                        await page.wait_for_function(
                            """() => {
                            const sels = [
                                'article a[href]', '.story a[href]', '.post a[href]',
                                'a[href*="/tin-"]', 'a[href*="/news"]'
                            ];
                            const hrefs = new Set();
                            for (const s of sels) {
                                for (const a of Array.from(document.querySelectorAll(s))) {
                                if (a.href && a.href.startsWith('http')) hrefs.add(a.href);
                                }
                            }
                            return hrefs.size >= 3;
                            }""",
                            timeout=1200,
                        )
                    except Exception:
                        pass

                    # CF/captcha?
                    if (
                        await page.locator(
                            "div#challenge-form, div#challenge-container, iframe[title*=captcha]"
                        ).count()
                        > 0
                    ):
                        blocked = True

                    # LẦN ĐỌC 1
                    html = await page.content()

                    anchors_count = await page.evaluate(
                        """
                        () => {
                            const sels = [
                            'article a[href]', '.story a[href]', '.post a[href]',
                            'a[href*="/tin-"]', 'a[href*="/news"]',
                            'a[href*="/bai-"]', 'a[href*="/post-"]', 'a[href*="/202"]'
                            ];
                            const hrefs = new Set();
                            for (const s of sels) {
                            for (const a of Array.from(document.querySelectorAll(s))) {
                                if (a.href && a.href.startsWith('http')) hrefs.add(a.href);
                            }
                            }
                            return hrefs.size;
                        }
                        """
                    )
                    if anchors_count < 3:
                        await page.wait_for_timeout(500)
                        html = await page.content()

                    # 2) Opportunistic wait → nhớ ĐỌC LẠI
                    if len(html) < MIN_HTML_LEN_OPPORT:
                        try:
                            await page.wait_for_load_state(
                                "networkidle", timeout=NETWORKIDLE_MS
                            )
                        except Exception:
                            pass
                        # LẦN ĐỌC 2 (SAU networkidle)
                        html = await page.content()

                    # 3) Nếu vẫn ngắn → điều hướng lại với networkidle → ĐỌC LẠI
                    if len(html) < MIN_HTML_LEN_HEUR:
                        try:
                            resp2 = await page.goto(
                                url,
                                timeout=min(NAV2_TIMEOUT_MS, ATTEMPT_NAV_MS),
                                wait_until="networkidle",
                            )
                            status2 = resp2.status if resp2 else None
                            if status2 and status2 >= 400:
                                raise RuntimeError(f"HTTP {status2}")

                            await page.wait_for_selector(
                                "body", timeout=BODY_SELECTOR_TIMEOUT_MS
                            )
                            # thêm một nhịp rất ngắn để top-story JS hoàn tất
                            await page.wait_for_timeout(400)

                            # LẦN ĐỌC 3 (SAU lần goto thứ 2)
                            html = await page.content()
                        except Exception:
                            # giữ nguyên html hiện có để các khâu sau quyết định
                            pass

                    # Heuristic blocked
                    if len(html) < MIN_HTML_LEN_HEUR:
                        body_text = await page.text_content("body") or ""
                        if _looks_blocked(body_text):
                            blocked = True

                finally:
                    try:
                        if not blocked:
                            self._storage_state[domain] = await context.storage_state()
                    except Exception:
                        pass
                    await context.close()

                if blocked:
                    pol = DOMAIN_LIMITER.policy(domain)
                    pol.min_gap = min(2.5, pol.min_gap * 1.5)  # cooldown
                    raise RuntimeError("Blocked/challenge detected")

                if not html or len(html) < MIN_HTML_LEN_RESULT:
                    raise RuntimeError("No/too-short content")

                return html
        except Exception as e:
            logger.error(f"Playwright fetch failed: {e}")
            await self.close()  # reset để lần sau fetch sẽ tự start lại
            raise
        finally:
            DOMAIN_LIMITER.release(domain)


OVERALL_BUDGET_MS = int(os.getenv("PLAYWRIGHT_OVERALL_BUDGET_MS", "35000"))
ATTEMPT_NAV_MS = int(os.getenv("PLAYWRIGHT_NAV_TIMEOUT_MS", "12000"))
BODY_SELECTOR_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_BODY_WAIT_MS", "2000"))
NETWORKIDLE_MS = int(os.getenv("PLAYWRIGHT_NETWORKIDLE_MS", "4000"))
MIN_HTML_LEN_OPPORT = int(os.getenv("PLAYWRIGHT_MIN_HTML_LEN_OPPORT", "1200"))
MIN_HTML_LEN_HEUR = int(os.getenv("PLAYWRIGHT_MIN_HTML_LEN_BLOCK_HEUR", "800"))
MIN_HTML_LEN_RESULT = int(os.getenv("PLAYWRIGHT_MIN_HTML_LEN_RESULT", "1000"))
NAV2_TIMEOUT_MS = int(os.getenv("PLAYWRIGHT_NAV2_TIMEOUT_MS", "10000"))


@retry(
    reraise=True,
    stop=(stop_after_attempt(3) | stop_after_delay(OVERALL_BUDGET_MS / 1000)),
    wait=wait_random_exponential(multiplier=1, max=8),
    retry=retry_if_exception_type((TimeoutError, RuntimeError, Exception)),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def crawl_with_playwright(url: str, referer: str | None = None) -> str:
    # Không nuốt exception — để tenacity retry
    start = time.perf_counter()
    remaining = lambda: max(
        1, int(OVERALL_BUDGET_MS - (time.perf_counter() - start) * 1000)
    )

    html = await PlaywrightPool.instance().fetch(
        url,
        timeout_ms=min(ATTEMPT_NAV_MS, remaining()),
        referer=referer,
    )
    if not html or len(html) < 1000:
        # ép retry nếu nội dung quá ngắn
        raise RuntimeError("Empty/short HTML")
    return html


# ---- Infinite scroll helper cho listing ----
LOAD_MORE_TEXTS = [
    "Xem thêm",
    "Xem thêm tin tức",
    "Hiển thị thêm",
    "Tải thêm",
    "Xem thêm tin tức",
    "Xem tiếp",
    "Tải thêm bài viết",
    "Hiển thị bài viết khác",
    "Load more",
    "See more",
    "Trang tiếp",
]

LOAD_MORE_SELECTORS = (
    [f"button:has-text('{t}')" for t in LOAD_MORE_TEXTS]
    + [f"a:has-text('{t}')" for t in LOAD_MORE_TEXTS]
    + [
        ".btn-more",
        ".load-more",
        ".btn-load-more",
        ".view-more",
        "button.view-more",
        "a.view-more",
        "button.load-more",
        "a.load-more",
        "div.view-more button",
        "[data-test=load-more]",
        "[aria-label='Xem thêm']",
    ]
)

# Một vài selector item phổ biến (có thể override)
DEFAULT_ITEM_SELECTORS = [
    "article a[href]",
    ".article a[href]",
    ".story a[href]",
    ".post a[href]",
    "li a[href]",
    ".list a[href]",
    ".card a[href]",
    "a[href*='/tin-']",
    "a[href*='/news']",
    "a[href*='-post']",
    "a.box-category-link-with-avatar[href]",
]


async def _close_common_overlays(page):
    # đóng cookie banner / modal nếu có
    candidates = page.locator(
        ".cookie-banner, .cc-window, .modal-backdrop, .popup, .overlay"
    )
    try:
        if await candidates.count() > 0 and await candidates.first.is_visible():
            # thử nút close
            close_btn = candidates.locator(
                "button:has-text('OK'), button:has-text('Đồng ý'), .close, [aria-label='Close']"
            )
            if await close_btn.count() > 0 and await close_btn.first.is_visible():
                await close_btn.first.click(force=True)
            else:
                # ẩn overlay bằng JS như biện pháp cuối
                await candidates.evaluate_all(
                    "els => els.forEach(el => el.style.display='none')"
                )
    except Exception:
        pass


async def _scroll_by_mouse(
    page, container_selector: Optional[str] = None, px: int = 1600
):
    # lăn chuột để kích hoạt lazy-load (độ tin cậy cao hơn scrollTo)
    if container_selector:
        await page.locator(container_selector).hover(timeout=2000)
    await page.mouse.wheel(0, px)


async def _scroll_to_bottom(page, container_selector: Optional[str] = None):
    if container_selector:
        await page.evaluate(
            """
            (sel) => {
              const el = document.querySelector(sel);
              if (!el) return;
              el.scrollTop = el.scrollHeight;
            }
        """,
            container_selector,
        )
    else:
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")


async def _activate_parent_tab_or_expand(page, btn):
    # thử click các header/tab/accordion gần đó nếu tồn tại
    try:
        parent = await btn.element_handle()
        if not parent:
            return
        await page.evaluate(
            """
        (el) => {
          function findClickableAncestor(n, depth=5){
            let cur = n; let d=0;
            while (cur && d<depth){
              if (cur.matches?.('[role="tab"], .tab, .accordion-header, .collapse-toggle, [aria-controls]')) return cur;
              cur = cur.parentElement; d++;
            }
            return null;
          }
          const anc = findClickableAncestor(el);
          if (anc){ anc.click(); }
        }
        """,
            parent,
        )
    except Exception:
        pass


async def _try_click_load_more(page, selectors: Iterable[str], idle_ms: int) -> bool:
    for sel in selectors:
        btn = page.locator(sel).first
        try:
            if await btn.count() == 0:
                continue

            await btn.wait_for(state="attached", timeout=1200)

            # 1) cuộn vào giữa viewport (không chờ dài)
            try:
                await btn.scroll_into_view_if_needed(timeout=1200)
            except PWTimeout:
                # kích tab/accordion rồi cuộn lại
                await _activate_parent_tab_or_expand(page, btn)
                try:
                    await btn.scroll_into_view_if_needed(timeout=800)
                except Exception:
                    pass

            # 2) nếu vẫn chưa visible → tự cuộn container + hover
            visible = await btn.is_visible()
            if not visible:
                # tìm container có thể cuộn gần nhất
                try:
                    h = await btn.element_handle()
                    if h:
                        await page.evaluate(
                            """
                        (el) => {
                          function getScrollableAncestor(n){
                            let cur = el = n; let i=0;
                            while (cur && i<6){
                              const s = getComputedStyle(cur);
                              if (/(auto|scroll)/.test(s.overflowY) && cur.scrollHeight > cur.clientHeight) return cur;
                              cur = cur.parentElement; i++;
                            }
                            return document.scrollingElement || document.documentElement;
                          }
                          const sc = getScrollableAncestor(el);
                          el.scrollIntoView({block:'center', inline:'nearest'});
                        }
                        """,
                            h,
                        )
                        await page.wait_for_timeout(200)
                        visible = await btn.is_visible()
                except Exception:
                    pass

            # 3) click: chuẩn → force → JS
            try:
                if visible:
                    await btn.click(timeout=1200)
                else:
                    await btn.click(force=True, timeout=1200)
            except Exception:
                try:
                    handle = await btn.element_handle()
                    if handle:
                        await handle.evaluate("el => el.click()")
                    else:
                        continue
                except Exception:
                    continue

            await page.wait_for_timeout(idle_ms)
            return True

        except Exception:
            continue
    return False


async def _count_unique_links(
    page, item_selectors: list[str], same_host_of: Optional[str]
) -> int:
    hrefs: set[str] = set()
    for css in item_selectors:
        try:
            links = await page.eval_on_selector_all(
                css, "els => els.map(e => e.href).filter(Boolean)"
            )
        except Exception:
            links = []
        for h in links or []:
            h = h.strip()
            if not h.startswith(("http://", "https://")):
                continue
            if same_host_of:
                host0 = urllib.parse.urlparse(same_host_of).netloc
                hostx = urllib.parse.urlparse(h).netloc
                if hostx and hostx != host0:
                    continue
            hrefs.add(h)
    # Nếu không match gì, fallback a[href]
    if not hrefs:
        links = await page.eval_on_selector_all(
            "a[href]", "els => els.map(e => e.href).filter(Boolean)"
        )
        for h in links or []:
            h = h.strip()
            if h.startswith(("http://", "https://")):
                if same_host_of:
                    host0 = urllib.parse.urlparse(same_host_of).netloc
                    hostx = urllib.parse.urlparse(h).netloc
                    if hostx and hostx != host0:
                        continue
                hrefs.add(h)
    return len(hrefs)


async def crawl_infinite_listing(
    url: str,
    max_rounds: int = 12,
    idle_ms: int = 700,
    container_selector: Optional[
        str
    ] = None,  # nếu trang cuộn trong panel riêng, truyền vào đây
    item_selectors: Optional[
        list[str]
    ] = None,  # danh sách selector để đếm bài viết mới
    extra_load_more_selectors: Optional[
        list[str]
    ] = None,  # nếu bạn có selector riêng cho site
) -> str:
    item_selectors = item_selectors or DEFAULT_ITEM_SELECTORS
    load_more_selectors = list(LOAD_MORE_SELECTORS)
    if extra_load_more_selectors:
        load_more_selectors = extra_load_more_selectors + load_more_selectors

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page()
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)

        await _close_common_overlays(page)

        last_count = await _count_unique_links(page, item_selectors, same_host_of=url)

        for round_idx in range(max_rounds):
            clicked = await _try_click_load_more(page, load_more_selectors, idle_ms)
            if not clicked:
                # không có nút → thử kịch bản infinite scroll (mouse wheel + scrollTo)
                await _scroll_by_mouse(page, container_selector, px=1600)
                await page.wait_for_timeout(idle_ms)
                await _scroll_to_bottom(page, container_selector)
                await page.wait_for_timeout(idle_ms)

            # Chờ mạng “dịu” một chút nhưng không phụ thuộc networkidle
            try:
                await page.wait_for_load_state("networkidle", timeout=2500)
            except PWTimeout:
                # nhiều site dùng long-polling → bỏ qua
                pass

            # cuộn thêm vài nhịp nhỏ để kích lazy-load (IntersectionObserver)
            for _ in range(2):
                await _scroll_by_mouse(page, container_selector, px=1000)
                await page.wait_for_timeout(min(idle_ms, 500))

            # Đếm xem có thêm bài mới không
            count = await _count_unique_links(page, item_selectors, same_host_of=url)
            if count <= last_count:
                # thử thêm 1 vòng “lăn chuột” nữa trước khi dừng
                await _scroll_by_mouse(page, container_selector, px=2000)
                await page.wait_for_timeout(idle_ms)
                count = await _count_unique_links(
                    page, item_selectors, same_host_of=url
                )

            print(f"[round {round_idx+1}] items: {count} (was {last_count})")
            if count <= last_count:
                print("Không thấy item mới → dừng.")
                break
            last_count = count

        html = await page.content()
        await browser.close()
        return html


# Optional: Fallback function cho các site khó
async def robust_crawl(url: str) -> str:
    """Crawl với fallback mechanism"""
    try:
        return await crawl_with_playwright(url)
    except Exception as e:
        print(f"🎯 Playwright failed, trying fallback for {url}: {e}")
        # Thêm fallback logic ở đây (requests + cloudscraper, etc.)
        raise  # Hoặc implement fallback


# --- START: Helpers function ---
async def _maybe_await(fn):
    if inspect.iscoroutinefunction(fn):
        return await fn()
    res = fn()
    if inspect.isawaitable(res):
        return await res
    return res


async def _get_response_text(resp):
    if hasattr(resp, "content"):
        return resp.content
    if hasattr(resp, "text"):
        return resp.text
    # stream
    if hasattr(resp, "__aiter__"):
        chunks = []
        async for part in resp:
            chunks.append(getattr(part, "content", str(part)))
        return "".join(chunks)
    return str(resp)


# Bắt dd/mm/yyyy (có thể kèm giờ)
_VN_DATE_RE = re.compile(
    r"(?P<d>\d{1,2})[/-](?P<m>\d{1,2})[/-](?P<y>\d{4})(?:\s+(?P<h>\d{1,2}):(?P<min>\d{2}))?"
)

# Vietnamese textual month formats: "13 thg 6, 2025" or "13 tháng 6, 2025"
_VN_DATE_RE_TEXT = re.compile(
    r"(?P<d>\d{1,2})\s*(?:thg|tháng)\s*(?P<m>\d{1,2})\s*,?\s*(?P<y>\d{4})",
    re.IGNORECASE,
)


# --- Thêm regex & map mới để bắt nhiều định dạng hơn ---
_ISO_YMD_ANY_RE = re.compile(r"(?P<y>\d{4})[-/\.](?P<m>\d{1,2})[-/\.](?P<d>\d{1,2})")
_DMY_ANY_RE = re.compile(r"(?P<d>\d{1,2})[-/\.](?P<m>\d{1,2})[-/\.](?P<y>\d{4})")

_MONTHS_EN = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_EN_TEXT_MD_RE = re.compile(
    r"(?i)(?P<m>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+(?P<d>\d{1,2})(?:st|nd|rd|th)?\s*,?\s*(?P<y>\d{4})"
)
_EN_TEXT_DM_RE = re.compile(
    r"(?i)(?P<d>\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(?P<m>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s*,?\s*(?P<y>\d{4})"
)


def _to_iso(y: int, m: int, d: int) -> str | None:
    if not (1 <= m <= 12):
        return None
    if not (1 <= d <= 31):
        return None
    return f"{y:04d}-{m:02d}-{d:02d}"


def _month_from_name(name: str) -> int | None:
    return _MONTHS_EN.get(name.strip().lower())


def _norm_iso(s: str | None) -> str | None:
    """
    Chuẩn hoá chuỗi ngày bất kỳ về YYYY-MM-DD (bỏ phần giờ/tz).
    Hỗ trợ: ISO (kể cả T, Z, tz), dd/mm/yyyy, '13 thg 6, 2025', 'June 13, 2025', '13 June 2025'.
    """
    if not s:
        return None
    s = str(s).strip()
    # 1) Bóc ISO ở bất kỳ vị trí nào (cả '2025-06-13T10:05:00+07:00')
    m = _ISO_YMD_ANY_RE.search(s)
    if m:
        y, mo, d = int(m.group("y")), int(m.group("m")), int(m.group("d"))
        return _to_iso(y, mo, d)
    # 2) dd/mm/yyyy hoặc dd-mm-yyyy
    m = _DMY_ANY_RE.search(s)
    if m:
        d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
        return _to_iso(y, mo, d)
    # 3) Việt: '13 thg 6, 2025' / '13 tháng 6 2025'
    m = _VN_DATE_RE_TEXT.search(s)
    if m:
        d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
        return _to_iso(y, mo, d)
    # 4) EN: 'June 13, 2025'
    m = _EN_TEXT_MD_RE.search(s)
    if m:
        mo = _month_from_name(m.group("m"))
        d = int(m.group("d"))
        y = int(m.group("y"))
        if mo:
            return _to_iso(y, mo, d)
    # 5) EN: '13 June 2025'
    m = _EN_TEXT_DM_RE.search(s)
    if m:
        mo = _month_from_name(m.group("m"))
        d = int(m.group("d"))
        y = int(m.group("y"))
        if mo:
            return _to_iso(y, mo, d)
    # 6) fallback cũ: VN dd/mm/yyyy trong chuỗi bất kỳ
    m = _VN_DATE_RE.search(s)
    if m:
        d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
        return _to_iso(y, mo, d)
    return None


# -------- JSON-LD WALK (đệ quy, hỗ trợ @graph, @type là list) --------
_JSONLD_ARTICLE_TYPES = {"newsarticle", "article", "report", "blogposting"}


def _walk_ldjson(x, out: dict):
    if isinstance(x, dict):
        t = x.get("@type")
        types = []
        if isinstance(t, str):
            types = [t.lower()]
        elif isinstance(t, list):
            types = [str(it).lower() for it in t]

        if any(tt in _JSONLD_ARTICLE_TYPES for tt in types):
            dp = x.get("datePublished") or x.get("dateCreated")
            dm = x.get("dateModified")
            pub_iso = _norm_iso(dp)
            mod_iso = _norm_iso(dm)
            if pub_iso and not out.get("published_iso"):
                out["published_iso"] = pub_iso
                out["source_published_text"] = dp
            if mod_iso and not out.get("modified_iso"):
                out["modified_iso"] = mod_iso

        # duyệt sâu @graph và các field lồng nhau
        for k, v in x.items():
            if k == "@graph":
                _walk_ldjson(v, out)
            elif isinstance(v, (dict, list)):
                _walk_ldjson(v, out)

    elif isinstance(x, list):
        for it in x:
            _walk_ldjson(it, out)


# -------- EXTRACT DATES (RULE-BASED) --------
def extract_dates_rule_based(html: str, url: str):
    """
    Trả về:
      {
        'published_iso': 'YYYY-MM-DD' | None,
        'modified_iso':  'YYYY-MM-DD' | None,
        'source_published_text': 'chuỗi gốc' | None
      }
    Ưu tiên: JSON-LD -> meta (article/og/microdata) -> <time> -> text quanh tiêu đề -> full text.
    """
    soup = BeautifulSoup(html, "html.parser")
    article_scope = (
        soup.select_one(
            "article, main, .article, .article-detail, .content-detail, .post, .story, .detail"
        )
        or soup
    )
    head = soup.find("head") or soup

    # Try to extract from the specific span class you mentioned
    date_span = article_scope.find(
        "span", class_="sc-longform-header-date block-sc-publish-time"
    )
    if date_span:
        date_text = date_span.get_text(strip=True)
        cand = _norm_iso(date_text)
        if cand:
            return {
                "published_iso": cand,
                "modified_iso": None,
                "source_published_text": date_text,
            }

    # 1) JSON-LD (đệ quy, hỗ trợ @graph, @type list)
    try:
        for s in head.find_all("script", type="application/ld+json"):
            txt = s.string or ""
            if not txt.strip():
                continue
            try:
                data = json.loads(txt)
            except Exception:
                continue
            out = {}
            _walk_ldjson(data, out)
            if out.get("published_iso"):
                return {
                    "published_iso": out.get("published_iso"),
                    "modified_iso": out.get("modified_iso"),
                    "source_published_text": out.get("source_published_text"),
                }
    except Exception:
        pass

    # 2) OpenGraph/Article meta + Microdata itemprop
    meta_candidates_pub = [
        ("property", "article:published_time"),
        ("name", "pubdate"),
        ("name", "date"),
        ("itemprop", "datePublished"),
        ("property", "og:pubdate"),  # hiếm
    ]
    meta_candidates_mod = [
        ("property", "article:modified_time"),
        ("property", "og:updated_time"),
        ("itemprop", "dateModified"),
    ]
    pub_iso = None
    source_pub = None

    for attr, val in meta_candidates_pub:
        el = head.find("meta", {attr: val})
        if el and el.get("content"):
            cand = _norm_iso(el.get("content"))
            if cand:
                pub_iso = cand
                source_pub = el.get("content")
                break
    mod_iso = None
    for attr, val in meta_candidates_mod:
        el = head.find("meta", {attr: val})
        if el and el.get("content"):
            cand = _norm_iso(el.get("content"))
            if cand:
                mod_iso = cand
                break
    if pub_iso:
        return {
            "published_iso": pub_iso,
            "modified_iso": mod_iso,
            "source_published_text": source_pub,
        }

    # 3) <time datetime="..."> (lấy cái đầu hợp lệ)
    for t in soup.find_all("time"):
        dt = t.get("datetime") or t.get_text(" ", strip=True)
        cand = _norm_iso(dt)
        if cand:
            return {
                "published_iso": cand,
                "modified_iso": None,
                "source_published_text": t.get_text(" ", strip=True) or dt,
            }

    # 4) Text meta quanh tiêu đề (mở rộng selector VN)
    meta_selectors = [
        ".article__meta",
        ".article-meta",
        ".meta",
        ".date",
        ".ldo-meta",
        "span.time",
        ".details__meta",
        ".details__time",
        ".time-count",
        ".publish-date",
        ".box-date",
        ".article__time",
        ".post-meta",
        ".breadcrumb_time",
        ".sapo .time",
        ".meta-date",
        ".tuongthuat_time",
    ]
    meta_blocks = []
    for sel in meta_selectors:
        meta_blocks += [el.get_text(" ", strip=True) for el in soup.select(sel)]
    joined = " | ".join([b for b in meta_blocks if b])

    # a) dd/mm/yyyy, dd-mm-yyyy trong khối meta
    m = _DMY_ANY_RE.search(joined)
    if m:
        d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
        pub_iso = _to_iso(y, mo, d)
        if pub_iso:
            return {
                "published_iso": pub_iso,
                "modified_iso": None,
                "source_published_text": joined[m.start() : m.end()],
            }

    # b) Việt chữ: "13 thg 6, 2025" / "13 tháng 6 2025"
    m2 = _VN_DATE_RE_TEXT.search(joined)
    if m2:
        d, mo, y = int(m2.group("d")), int(m2.group("m")), int(m2.group("y"))
        pub_iso = _to_iso(y, mo, d)
        if pub_iso:
            return {
                "published_iso": pub_iso,
                "modified_iso": None,
                "source_published_text": joined[m2.start() : m2.end()],
            }

    # 5) Last resort: quét full text (nặng)
    try:
        full_txt = soup.get_text(" ", strip=True)
    except Exception:
        full_txt = ""

    # a) ISO/DMY trong full text
    m = _ISO_YMD_ANY_RE.search(full_txt) or _DMY_ANY_RE.search(full_txt)
    if m:
        if m.re is _ISO_YMD_ANY_RE:
            y, mo, d = int(m.group("y")), int(m.group("m")), int(m.group("d"))
        else:
            d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
        pub_iso = _to_iso(y, mo, d)
        if pub_iso:
            return {
                "published_iso": pub_iso,
                "modified_iso": None,
                "source_published_text": full_txt[m.start() : m.end()],
            }

    # b) Việt/Anh textual trong full text
    for rex in (_VN_DATE_RE_TEXT, _EN_TEXT_MD_RE, _EN_TEXT_DM_RE):
        m = rex.search(full_txt)
        if m:
            if rex is _VN_DATE_RE_TEXT:
                d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
            else:
                mo = _month_from_name(m.group("m"))
                d = int(m.group("d"))
                y = int(m.group("y"))
            if mo:
                pub_iso = _to_iso(y, mo, d)
                if pub_iso:
                    return {
                        "published_iso": pub_iso,
                        "modified_iso": None,
                        "source_published_text": full_txt[m.start() : m.end()],
                    }

    return {"published_iso": None, "modified_iso": None, "source_published_text": None}


def extract_title_rule_based(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    candidates: list[str] = []

    # 0) JSON-LD: lấy headline/name nếu có (để làm fallback sớm)
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
        except Exception:
            continue

        def _walk(x):
            if isinstance(x, dict):
                t = x.get("@type")
                types = (
                    [t.lower()]
                    if isinstance(t, str)
                    else [str(i).lower() for i in t] if isinstance(t, list) else []
                )
                if any(
                    tt in {"newsarticle", "article", "blogposting", "report"}
                    for tt in types
                ):
                    for k in ("headline", "name", "alternativeHeadline"):
                        v = x.get(k)
                        if isinstance(v, str) and v.strip():
                            candidates.append(v.strip())
                for v in x.values():
                    _walk(v)
            elif isinstance(x, list):
                for it in x:
                    _walk(it)

        _walk(data)

    # 1) og:title (thường là bản sạch nhất)
    meta = soup.find("meta", {"property": "og:title"})
    if meta and meta.get("content"):
        candidates.append(meta["content"].strip())

    # 2) twitter:title
    tw = soup.find("meta", {"name": "twitter:title"})
    if tw and tw.get("content"):
        candidates.append(tw["content"].strip())

    # 3) itemprop headline/name (microdata)
    for attr in ("headline", "name"):
        node = soup.find(attrs={"itemprop": attr})
        if node:
            if node.get("content"):
                candidates.append(node["content"].strip())
            txt = node.get_text(" ", strip=True)
            if txt:
                candidates.append(txt)

    # 4) h1/h2 có class “title”
    sel_list = [
        "h1[itemprop=headline]",
        "h1.article-title",
        "h1.title",
        "h1.post-title",
        ".article-title h1",
        ".title-detail",
        "h1#title",
        "h1",
        "h2.article-title",
        "h2.title",
    ]
    for sel in sel_list:
        for el in soup.select(sel):
            txt = el.get_text(" ", strip=True)
            if txt:
                candidates.append(txt)

    # 5) <title> của trang (thường dính suffix brand)
    if soup.title and soup.title.get_text(strip=True):
        candidates.append(soup.title.get_text(strip=True))

    # --- Làm sạch & chọn ứng viên tốt nhất ---
    import html as _html

    def _clean_title(t: str) -> str:
        t = _html.unescape(t)
        t = re.sub(r"\s+", " ", t).strip()

        # Tách theo các dấu ngăn (| - – — • : _), ưu tiên phần có vẻ là nội dung, bỏ phần “brand”
        parts = re.split(r"\s[\|\-–—•:_]\s", t)
        if len(parts) > 1:
            bad = re.compile(r"(?i)\b(Báo|Online|News|Tin tức|Trang chủ|Home)\b")
            good = [p for p in parts if len(p) >= 15 and not bad.search(p)]
            if good:
                t = max(good, key=len)
            else:
                t = parts[0]  # mặc định lấy vế trái
        # Gỡ phần brand trong ngoặc ở cuối
        t = re.sub(r"\s*\((?:Báo|Online|News).*\)$", "", t, flags=re.I)
        return t.strip()

    # khử trùng & dedup (case-insensitive)
    seen = set()
    cleaned = []
    for raw in candidates:
        c = _clean_title(raw)
        if not c:
            continue
        key = c.lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(c)

    if not cleaned:
        return None

    # Chấm điểm đơn giản: độ dài vừa phải, không toàn caps, không câu hoàn chỉnh
    def _score(s: str) -> int:
        score = 0
        L = len(s)
        if 25 <= L <= 120:
            score += 3
        elif 10 <= L <= 160:
            score += 2
        if not s.isupper():
            score += 1
        if re.search(r"(?i)\b(trang chủ|home|tin tức|news|404|not found)\b", s):
            score -= 3
        if re.search(r"[\.!?]$", s):
            score -= 1  # có vẻ là câu, không phải tiêu đề
        return score

    cleaned.sort(key=_score, reverse=True)
    # Ưu tiên ứng viên có điểm >=2, nếu không thì lấy ứng viên đầu
    return cleaned[0] if _score(cleaned[0]) >= 2 else cleaned[0]


async def validate_and_normalize_link(
    raw_url: str, allowed_domain: str, timeout=10
) -> str | None:
    """Trả về URL đã chuẩn hoá nếu hợp lệ, ngược lại None."""
    try:
        r = await http_client.get(
            raw_url, follow_redirects=True, timeout=timeout, headers=_random_ua()
        )
        if r.status_code != 200:
            return None
        ctype = r.headers.get("Content-Type", "")
        if "text/html" not in ctype:
            return None

        final_url = str(r.url)

        u = urlparse(final_url)
        # Bỏ tracking params (mở rộng)
        drop_keys = {"fbclid", "gclid", "yclid", "mc_cid", "mc_eid", "ref", "ref_src"}
        q = [
            (k, v)
            for k, v in parse_qsl(u.query, keep_blank_values=True)
            if not (k.lower().startswith("utm_") or k.lower() in drop_keys)
        ]
        final_url = urlunparse(u._replace(query=urlencode(q, doseq=True)))

        # Canonical nếu có
        soup = BeautifulSoup(r.text, "html.parser")
        can = soup.find("link", rel=lambda x: x and "canonical" in x.lower())
        if can and can.get("href"):
            final_url = can["href"].strip()

        # Check domain cuối cùng
        host = urlparse(final_url).netloc.lower().lstrip("www.")
        allowed = allowed_domain.lower().lstrip("www.")
        if not (host == allowed or host.endswith("." + allowed)):
            return None

        # (tuỳ chọn) basic sanity: phải có <h1> hoặc og:title
        if not (soup.find("h1") or soup.find("meta", {"property": "og:title"})):
            return None

        return final_url
    except Exception:
        return None


# Check hub link helpers
_TAG_HUB_RE = re.compile(
    r"(?:/(?:tags?|chu-de|tu-khoa|tag|tim-kiem)/)|(?:/tu-khoa/[^/?#]+-tag\d+(?:\.tpo)?(?:/|$))"
)


def is_tag_hub_url(u: str, keywords: list[str]) -> bool:
    try:
        path = urlparse(u).path.lower()
    except Exception:
        path = (u or "").lower()

    # Chuẩn hoá keyword (bỏ dấu, lowercase, thay space bằng "-")
    def _norm(s: str) -> str:
        return (
            "".join(
                c
                for c in unicodedata.normalize("NFD", s or "")
                if unicodedata.category(c) != "Mn"
            )
            .lower()
            .replace(" ", "-")
        )

    path_clean = path.strip("/")

    # 1) Trường hợp hub truyền thống (tags/tag/chu-de/tu-khoa)
    if _TAG_HUB_RE.search(path):
        for kw in keywords:
            if _norm(kw) in path_clean:
                return True

    # 2) Trường hợp heuristic hub keyword ngắn (vd: /loc-troi.html)
    for kw in keywords:
        if _norm(kw) in path_clean:
            if len(path_clean) <= 30 and not re.search(r"\d{4,}", path_clean):
                return True

    return False


def _quick_summary_from_html(html: str, max_chars: int = 360) -> str:
    soup = BeautifulSoup(html or "", "html.parser")
    # lấy các đoạn p có độ dài > 100 ký tự, ưu tiên phần đầu
    paras = [
        p.get_text(" ", strip=True)
        for p in soup.select("article p, .article p, .detail p, p")
    ]
    paras = [t for t in paras if t and len(t) > 80]
    text = " ".join(paras[:3]) if paras else ""
    return (text[:max_chars] + "…") if len(text) > max_chars else text


# --- Date helpers for hub-link filtering ---
_DATE_PATTERNS = [
    r"/(?P<y>20\d{2})[/-](?P<m>0?[1-9]|1[0-2])[/-](?P<d>0?[1-9]|[12]\d|3[01])/",  # /YYYY/MM/DD or /YYYY-M-D
    r"/(?P<d>0?[1-9]|[12]\d|3[01])[/-](?P<m>0?[1-9]|1[0-2])[/-](?P<y>20\d{2})/",  # /DD/MM/YYYY
    r"/(?P<y>20\d{2})(?P<m>0[1-9]|1[0-2])(?P<d>0[1-9]|[12]\d|3[01])/",  # /YYYYMMDD
]


def _parse_date_soft(s: str) -> date | None:
    s = (s or "").strip()
    # ISO first
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            pass
    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        return datetime.strptime(m.group(1), "%Y-%m-%d").date()
    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def date_from_url_path(path: str) -> date | None:
    for pat in _DATE_PATTERNS:
        m = re.search(pat, path or "")
        if m:
            return date(int(m["y"]), int(m["m"]), int(m["d"]))
    return None


def date_from_anchor_context(a) -> date | None:
    t = a.find("time")
    if t:
        d = _parse_date_soft(t.get("datetime") or t.text)
        if d:
            return d
    steps = 0
    for parent in a.parents:
        steps += 1
        if steps > 4:
            break  # limit for perf
        t = parent.find("time")
        if t:
            d = _parse_date_soft(t.get("datetime") or t.text)
            if d:
                return d
        for attr in ("data-date", "data-published", "data-time", "datetime"):
            if parent.has_attr(attr):
                d = _parse_date_soft(parent.get(attr))
                if d:
                    return d
        # Try common date/time classes within the parent
        try:
            cand = parent.find(
                lambda tag: (
                    tag.name in ("span", "div", "time", "p")
                    and any(
                        cls
                        for cls in (tag.get("class") or [])
                        if isinstance(cls, str)
                        and any(
                            k in cls.lower()
                            for k in (
                                "date",
                                "time",
                                "publish",
                                "posted",
                                "ngay",
                                "thoi-gian",
                            )
                        )
                    )
                )
            )
            if cand:
                d = _parse_date_soft(cand.get_text(" ", strip=True))
                if d:
                    return d
        except Exception:
            pass
    return None


# --- END: helpers function ---
class AgentManager:
    _instance: Optional["AgentManager"] = None

    @classmethod
    def get_instance(cls) -> "AgentManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        # Map (session_id, provider) -> asyncio.Task
        self._tasks: Dict[Tuple[str, str], asyncio.Task] = {}
        # Map (session_id, provider) -> cancel token/cờ tự nguyện (nếu worker cần)
        self._tokens: Dict[Tuple[str, str], asyncio.Event] = {}

    def get_token(self, session_id: str, provider: str) -> asyncio.Event:
        key = (session_id, provider)
        tok = self._tokens.get(key)
        if tok is None:
            tok = asyncio.Event()
            self._tokens[key] = tok
        return tok

    # Gọi khi bạn khởi chạy một provider
    def register_provider_task(
        self, session_id: str, provider: str, task: asyncio.Task
    ):
        key = (session_id, provider)
        self._tasks[key] = task
        # tạo token cho cooperative cancel (nếu worker check token)
        self._tokens.setdefault(key, asyncio.Event())
        logger.info("Registered provider task: %s %s", session_id, provider)

        def _cleanup(_):
            # Task kết thúc thì dọn registry
            self._tasks.pop(key, None)
            self._tokens.pop(key, None)
            logger.info("Cleaned up provider task: %s %s", session_id, provider)

        task.add_done_callback(_cleanup)

    def get_cancel_event(self, session_id: str, provider: str) -> asyncio.Event:
        # Worker có thể gọi hàm này để lấy event kiểm tra trong vòng lặp
        return self._tokens.setdefault((session_id, provider), asyncio.Event())

    def cancel_provider(self, session_id: str, provider: str) -> bool:
        """
        Hủy provider đang chạy cho session. Hỗ trợ cả hard-cancel (task.cancel)
        và soft-cancel (đặt event để worker tự thoát).
        """
        key = (session_id, provider)
        task = self._tasks.get(key)
        if not task:
            return False

        # Đặt soft-cancel flag trước (cooperative)
        token = self._tokens.get(key)
        if token and not token.is_set():
            token.set()
            logger.info(
                "Set cancel flag for provider %s (session %s)", provider, session_id
            )

        # Hard-cancel nếu task vẫn chưa kết thúc
        if not task.done() and not task.cancelled():
            task.cancel()
            logger.info(
                "Cancelled asyncio task for provider %s (session %s)",
                provider,
                session_id,
            )
        return True

    # Tuỳ bạn: hủy tất cả provider trong một session
    def cancel_all_providers(self, session_id: str):
        keys = [k for k in self._tasks.keys() if k[0] == session_id]
        for _, provider in keys:
            self.cancel_provider(session_id, provider)

    def is_running(self, session_id: str, provider: str) -> bool:
        """Trả về True nếu provider còn task đang chạy."""
        return (session_id, provider) in self._tasks and not self._tasks[
            (session_id, provider)
        ].done()


class LLMUserFallbackMixin:
    async def _arun_with_user_fallback(
        self,
        prompt: str,
        *,
        session_id: str,
        preferred_provider: str | None = None,
        preferred_model: str | None = None,
    ):
        providers = _configured_providers_in_order(settings, preferred_provider)
        asked = set()  # tránh hỏi trùng 1 provider/model trong cùng lượt
        _timeout_retry_counter = {}
        if not providers:
            raise RuntimeError("No LLM provider configured")

        # Number of provider rounds completed (for optional cap)
        rounds = 0

        while True:
            for i, prov in enumerate(list(providers)):
                model_id = (
                    preferred_model
                    if prov == (preferred_provider or "").lower() and preferred_model
                    else PROVIDER_MODEL_MAP.get(prov, PROVIDER_MODEL_MAP["openai"])[
                        "default"
                    ]
                )
                try:
                    token = AgentManager.get_instance().get_token(session_id, prov)

                    # Nếu đã bị hủy trước khi gọi
                    if token.is_set():
                        raise asyncio.CancelledError()
                    # yêu cầu class có self._create_agent() và self.agent
                    self.model = get_llm_model(prov, model_id)
                    self._create_agent()
                    # Tạo task + đăng ký để cancel cứng được qua AgentManager
                    async with _sem_for(prov):
                        task = asyncio.create_task(
                            self.agent.arun(prompt, session_id=session_id)
                        )
                        AgentManager.get_instance().register_provider_task(
                            session_id, prov, task
                        )
                        try:
                            return await asyncio.wait_for(
                                task, timeout=_LLM_REQ_TIMEOUT
                            )
                        except asyncio.TimeoutError:
                            task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await task
                            raise

                except Exception as e:
                    code, msg = _map_llm_error(e) or ("PROVIDER_ERROR", str(e))
                    remaining = providers[i + 1 :]

                    # ✅ TIMEOUT / RATE LIMIT: giữ nguyên provider, retry im lặng (không popup)
                    if code in ("PROVIDER_TIMEOUT", "PROVIDER_RATE_LIMIT"):
                        key = (session_id, prov, model_id)
                        tries = _timeout_retry_counter.get(key, 0)
                        if tries < 3:  # tuỳ chỉnh
                            _timeout_retry_counter[key] = tries + 1
                            backoff = min(2**tries, 8)  # 1s, 2s, 4s, cap 8s
                            logger.info(
                                f"[LLM retry] {prov}({model_id}) {code} → retry {tries+1}/3 in {backoff}s"
                            )
                            await asyncio.sleep(backoff)
                            # xếp lại list để lần kế vẫn chạy provider hiện tại (không switch)
                            providers = providers[: i + 1] + [prov] + providers[i + 1 :]
                            continue
                        else:
                            # Hết số lần retry → bỏ qua provider này, chuyển tự động sang provider kế TIẾP (không popup)
                            logger.warning(
                                f"[LLM retry] {prov}({model_id}) exhausted retries → try next provider silently"
                            )
                            continue

                    # ✅ HẾT QUOTA: mới popup hỏi chuyển
                    if code == "PROVIDER_NO_QUOTA":
                        async with fallback_lock_by_session[session_id]:
                            if (prov, model_id) in asked:
                                continue
                            asked.add((prov, model_id))

                            if not remaining:
                                await event_bus.publish(
                                    session_id,
                                    {
                                        "type": "provider_error",
                                        "provider": prov,
                                        "model": model_id,
                                        "code": code,
                                        "message": msg,
                                        "final": True,
                                        "next_options": [],
                                    },
                                )
                                break

                            next_choice = remaining[0]
                            await event_bus.publish(
                                session_id,
                                {
                                    "type": "propose_switch",
                                    "from_provider": prov,
                                    "from_model": model_id,
                                    "message": msg,
                                    "code": code,
                                    "next_options": remaining,
                                    "suggested": next_choice,
                                },
                            )

                            decision = await decision_bus.wait(session_id, timeout=15.0)

                        if not decision:
                            # không phản hồi → auto thử provider kế
                            continue
                        act = (decision.get("action") or "").lower()
                        if act == "abort":
                            raise RuntimeError("User aborted run")
                        if act == "switch":
                            picked = (decision.get("provider") or next_choice).lower()
                            if picked in remaining:
                                providers = (
                                    providers[: i + 1]
                                    + [picked]
                                    + [x for x in remaining if x != picked]
                                )
                            continue

                    # 🔐 Các lỗi khác (AUTH/ERROR chung): không popup; mặc định chuyển provider kế (hoặc bạn muốn giữ nguyên thì đổi lại)
                    logger.warning(
                        f"[LLM] {prov}({model_id}) error={code}: {msg} → try next provider"
                    )
                    continue

            logger.info("🔁 Hoàn thành 1 vòng provider, nghỉ 5s rồi thử lại...")
            # Completed one full provider round; enforce optional cap
            rounds += 1
            if _LLM_MAX_PROVIDER_ROUNDS > 0 and rounds >= _LLM_MAX_PROVIDER_ROUNDS:
                raise RuntimeError(
                    f"Exceeded LLM provider rounds: {_LLM_MAX_PROVIDER_ROUNDS}. Aborting."
                )

            await asyncio.sleep(5)


# Sentinel để _retry_process_link() trả về và vòng lặp theo hub nhận biết để break
CUT_HUB: Final = object()


class _HubCutoff(Exception):
    """Tín hiệu dừng xử lý HUB hiện tại (ví dụ gặp bài cũ hơn start_date)."""

    __slots__ = ("url", "found_date", "threshold", "reason")

    def __init__(
        self,
        url: str,
        found_date: Optional[date] = None,  # ngày lấy được từ bài
        threshold: Optional[date] = None,  # mốc cần cắt (start_date.date())
        reason: str = "older-than-window",
    ) -> None:
        super().__init__(url, found_date, threshold, reason)
        self.url = url
        self.found_date = found_date
        self.threshold = threshold
        self.reason = reason

    def __str__(self) -> str:
        return (
            f"_HubCutoff(reason={self.reason}, url={self.url}, "
            f"found_date={self.found_date}, threshold={self.threshold})"
        )


class HubCrawlTool(LLMUserFallbackMixin):
    def __init__(
        self,
        model,
        config,
        session_id,
        parser: ArticleParser,
        check_pause_or_cancel: Optional[callable] = None,
    ):
        self.model = model
        self.session_id = session_id
        self.config = config
        self.parser = parser
        self.check_pause_or_cancel = check_pause_or_cancel or (lambda: None)
        self._create_agent()

    def _create_agent(self):
        self.agent = Agent(
            name="HubCrawler",
            role="Web Crawler và Content Extractor",
            tools=[Crawl4aiTools(max_length=2000)],
            model=self.model,
            instructions="""
            Bạn là một chuyên gia phân tích nội dung báo chí trực tuyến, đặc biệt thành thạo trong việc nhận diện và xử lý các định dạng ngày tháng đa dạng trong văn bản báo chí.

            Nhiệm vụ của bạn là đọc và hiểu nội dung bài báo trong khoảng thời gian được chỉ định, sau đó trích xuất chính xác các thông tin sau:
            - Tiêu đề bài báo  
            - Ngày phát hành bài báo
            - Tóm tắt nội dung chính  
            - Đường dẫn gốc của bài viết

            Chỉ cung cấp dữ liệu ở định dạng JSON theo yêu cầu. Không giải thích thêm bất kỳ điều gì khác.
            """,
            show_tool_calls=True,
            markdown=True,
        )

    async def run(
        self,
        media_source: MediaSource,
        keywords: List[str],
        start_date: datetime,
        end_date: datetime,
        industry_name: Optional[str] = None,
    ) -> CrawlResult:
        def _norm(s: str) -> str:
            return "".join(
                c
                for c in unicodedata.normalize("NFD", s or "")
                if unicodedata.category(c) != "Mn"
            ).lower()

        def _build_industry_variants(industry_name: Optional[str]) -> set[str]:
            if not industry_name:
                return set()
            base = _norm(industry_name)
            if not base:
                return set()

            ind = {base}
            IND_SYNONYMS = {
                "sua": {"sữa", "sua", "uht", "sua tiet trung", "sữa tiệt trùng"},
                "dau an": {"dau an", "dau-an", "dauan", "cooking oil", "edible oil"},
                "gia vi": {"gia vi", "gia-vi", "seasoning", "spices", "condiment"},
                "gao": {"gao", "gao-ngu-coc", "gao va ngu coc", "gao-ngu-coc", "rice"},
                "ngu coc": {
                    "ngu coc",
                    "ngu-coc",
                    "cereal",
                    "cereals",
                    "grain",
                    "grains",
                },
                "homecare": {
                    "homecare",
                    "home-care",
                    "cham soc nha cua",
                    "cham-soc-nha-cua",
                    "ve sinh nha cua",
                    "ve-sinh-nha-cua",
                },
                "home care": {
                    "home care",
                    "home-care",
                    "homecare",
                    "cham soc nha cua",
                    "cham-soc-nha-cua",
                    "ve sinh nha cua",
                    "ve-sinh-nha-cua",
                },
            }
            for key, variants in IND_SYNONYMS.items():
                if key in base:
                    ind |= {_norm(v) for v in variants}
            return ind

        def _get_current_page_num(u: str) -> int:
            p = urlparse(u)
            if (
                "tapchikinhtetaichinh.vn" in p.netloc
                and "search_enginer.html" in p.path
            ):
                qs = parse_qs(p.query)  # ✅ dùng p.query
                brsr = int(qs.get("BRSR", ["0"])[0])
                per = int(qs.get("per_page", ["10"])[0])  # hoặc DEFAULT_PER_PAGE
                return brsr // per + 1
            elif p.netloc.endswith("vietnamnet.vn") and p.path.startswith("/tim-kiem"):
                m = re.search(r"-p(\d+)(?:\.html)?$", p.path, re.IGNORECASE)
                if m:
                    try:
                        return int(m.group(1)) + 1  # p1 -> 2, p2 -> 3, ...
                    except Exception:
                        pass

            patterns = [
                r"[?&]page=(\d+)",
                r"[?&]p=(\d+)",
                r"[?&]pi=(\d+)",
                r"[?&]trang=(\d+)",
                # path styles
                r"/page/(\d+)(?=[/?#]|$)",
                r"/trang/(\d+)(?=[/?#]|$)",
                # trang-<num>.htm(l) with optional query/fragment
                r"/trang-(\d+)\.html?(?=[/?#]|$)",  # <-- mới: match .htm và .html
                r"/trang-(\d+)(?=[/?#]|$)",  # cho trường hợp không có đuôi nhưng có ? hoặc #
                r"/p(\d+)(?=[/?#]|$)",
                r"-p(\d+)(?=\.html?(?=[/?#]|$)|[/?#]|$)",  # mở rộng cho .htm/.html và query
                r"/trang-(\d+)\.chn(?=[/?#]|$)",
            ]

            for pat in patterns:
                m = re.search(pat, u, re.IGNORECASE)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        pass
            return 1  # mặc định coi là trang 1

        def _next_page_numbered_html(current_url: str, next_no: int) -> str | None:
            """
            Hỗ trợ mọi site có pattern: .../(trang|page|p)-<N>.html (hoặc .htm)
            Trả URL trang kế tiếp bằng cách thay số trong path. Không đụng query.
            """
            p = urlparse(current_url)
            # match đúng đuôi .html/.htm, Nằm ở CUỐI path
            m = re.search(
                r"(.*?/(?:trang|page|p|pi)-)(\d+)(\.html?)$", p.path, re.IGNORECASE
            )
            if not m:
                return None
            new_path = f"{m.group(1)}{next_no}{m.group(3)}"
            # GIỮ nguyên query nếu có? Tuỳ bạn. Thường dạng này không cần query -> xoá cho sạch:
            return urlunparse((p.scheme, p.netloc, new_path, p.params, "", p.fragment))

        PREFERRED_KEYS = ("p", "page", "pi", "trang")
        PER_DOMAIN_DEFAULT = {
            "doisongphapluat.com.vn": "p",
            "giadinh.suckhoedoisong.vn": "trang",
            # thêm site khác nếu cần...
        }

        def _normalize_tuple(url: str):
            p = urlparse(url)
            # chuẩn hoá thứ tự query để so sánh đúng “nghĩa”
            qs = parse_qs(p.query, keep_blank_values=True)
            items = []
            for k in sorted(qs.keys()):
                for v in sorted(qs[k]):
                    items.append((k, v))
            return (
                p.scheme,
                p.netloc,
                p.path.rstrip("/"),
                p.params,
                tuple(items),
                p.fragment,
            )

        def _add(urls: list[str], seen: set, p, qdict):
            new_q = urlencode(qdict, doseq=True)
            u2 = urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))
            norm = _normalize_tuple(u2)
            if norm not in seen:
                seen.add(norm)
                urls.append(u2)

        def _add_path(urls: list[str], seen: set, p, path2):
            u2 = urlunparse((p.scheme, p.netloc, path2, p.params, p.query, p.fragment))
            norm = _normalize_tuple(u2)
            if norm not in seen:
                seen.add(norm)
                urls.append(u2)

        def _make_url_with_page(u: str, page_no: int) -> list[str]:
            """Đoán các biến thể URL phân trang phổ biến cho trang kế tiếp (ưu tiên theo domain)."""
            out: list[str] = []
            seen: set = set()
            p = urlparse(u)
            q_orig = parse_qs(p.query or "", keep_blank_values=True)
            orig_norm = _normalize_tuple(u)

            def add_if_changed(url_candidate: str):
                norm = _normalize_tuple(url_candidate)
                if norm != orig_norm and norm not in seen:
                    seen.add(norm)
                    out.append(url_candidate)

            # ===== 0) Các rule đặc thù theo domain =====

            # DanTri: ?pi=
            if p.netloc.endswith("dantri.com.vn") and p.path.startswith("/tim-kiem/"):
                q = dict(q_orig)
                q["pi"] = [str(page_no)]
                new_q = urlencode(q, doseq=True)
                next_url = urlunparse(
                    (p.scheme, p.netloc, p.path, p.params, new_q, p.fragment)
                )
                add_if_changed(next_url)
                return out

            # QDND: /tim-kiem/.../p/<page_no>
            if p.netloc.endswith("qdnd.vn") and p.path.startswith("/tim-kiem/"):
                path = p.path
                if re.search(r"/p/\d+/?$", path):
                    new_path = re.sub(r"/p/\d+/?$", f"/p/{page_no}", path)
                else:
                    new_path = path.rstrip("/") + f"/p/{page_no}"
                next_url = urlunparse(
                    (p.scheme, p.netloc, new_path, p.params, p.query, p.fragment)
                )
                add_if_changed(next_url)
                return out

            # Vietnamnet: -pN nhúng trong path (p2 = trang 3)
            if p.netloc.endswith("vietnamnet.vn") and p.path.startswith("/tim-kiem"):
                embed_no = max(1, page_no - 1)
                path = p.path
                if re.search(r"-p\d+(?:\.html)?$", path, flags=re.IGNORECASE):
                    new_path = re.sub(
                        r"-p\d+(?:\.html)?$", f"-p{embed_no}", path, flags=re.IGNORECASE
                    )
                else:
                    if path.lower().endswith(".html"):
                        new_path = re.sub(
                            r"\.html$", f"-p{embed_no}.html", path, flags=re.IGNORECASE
                        )
                    else:
                        new_path = path.rstrip("/") + f"-p{embed_no}"
                new_q = urlencode(q_orig, doseq=True)
                next_url = urlunparse(
                    (p.scheme, p.netloc, new_path, p.params, new_q, p.fragment)
                )
                add_if_changed(next_url)
                return out

            # thuehaiquan/vir: BRSR là số trang (1-based)
            if (
                "thuehaiquan.tapchikinhtetaichinh.vn" in p.netloc
                and "search_enginer.html" in p.path
            ) or ("vir.com.vn" in p.netloc and "search_enginer.html" in p.path):
                q = dict(parse_qs(p.query or "", keep_blank_values=True))
                if page_no <= 1:
                    q.pop("BRSR", None)
                else:
                    q["BRSR"] = [str(page_no)]

                # giữ p, q; ưu tiên thứ tự: BRSR → p → q → các param khác
                ordered = []
                if "BRSR" in q:
                    ordered.append(("BRSR", q["BRSR"][0]))
                for k in ("p", "q"):
                    if k in q:
                        for v in q[k]:
                            ordered.append((k, v))
                for k, vs in q.items():
                    if k in ("BRSR", "p", "q"):
                        continue
                    for v in vs or []:
                        ordered.append((k, v))

                new_q = urlencode(ordered, doseq=True)
                next_url = urlunparse(p._replace(query=new_q))
                add_if_changed(next_url)
                return out

            # ===== 1) Query params (giữ key hiện có; nếu không có, dùng default theo domain) =====

            q = dict(q_orig)

            # 1a) Nếu đã có sẵn 1 trong các key phân trang → tôn trọng key đó
            existing = [k for k in PREFERRED_KEYS if k in q]
            if existing:
                key = existing[0]
                q1 = dict(q)
                q1[key] = [str(page_no)]
                _add(out, seen, p, q1)
            else:
                # 1b) Chọn default theo domain; ví dụ doisongphapluat → 'p'
                host = p.netloc.lower()
                key = PER_DOMAIN_DEFAULT.get(host, "page")
                q1 = dict(q)
                q1[key] = [str(page_no)]
                _add(out, seen, p, q1)

                # 1c) Thêm vài biến thể khác (fallback), nhưng ưu tiên đặt 'p' trước 'page'
                for alt in PREFERRED_KEYS:
                    if alt == key:
                        continue
                    qx = dict(q)
                    qx[alt] = [str(page_no)]
                    _add(out, seen, p, qx)

            # ===== 2) Path patterns (fallback thêm) =====
            path = p.path
            candidates = []

            # /page/<n>/
            if re.search(r"/page/\d+(/|$)", path):
                candidates.append(re.sub(r"/page/\d+(/|$)", f"/page/{page_no}/", path))
            else:
                candidates.append(path.rstrip("/") + f"/page/{page_no}/")

            # /trang/<n>/, /trang-<n>/, /p<n>/
            if re.search(r"/trang/\d+(/|$)", path):
                candidates.append(
                    re.sub(r"/trang/\d+(/|$)", f"/trang/{page_no}/", path)
                )
            else:
                candidates.append(path.rstrip("/") + f"/trang/{page_no}/")

            if re.search(r"/trang-\d+(/|$)", path):
                candidates.append(
                    re.sub(r"/trang-\d+(/|$)", f"/trang-{page_no}/", path)
                )
            else:
                candidates.append(path.rstrip("/") + f"/trang-{page_no}/")

            if re.search(r"/p\d+(/|$)", path):
                candidates.append(re.sub(r"/p\d+(/|$)", f"/p{page_no}/", path))
            else:
                candidates.append(path.rstrip("/") + f"/p{page_no}/")

            for path2 in candidates:
                _add_path(out, seen, p, path2)

            # lọc bỏ bản gốc (đề phòng trường hợp không đổi)
            out = [u2 for u2 in out if _normalize_tuple(u2) != orig_norm]
            return out

        def _find_next_link(
            soup: BeautifulSoup, current_url: str, domain: str
        ) -> str | None:
            def _normalize_query(u: str) -> str:
                p = urlparse(u)
                q = parse_qs(p.query or "", keep_blank_values=True)
                # Re-encode lại query để biến ' ' → '+' và encode dấu/UTF-8 an toàn
                new_q = urlencode(q, doseq=True, quote_via=quote_plus, safe="")
                return urlunparse(
                    (p.scheme, p.netloc, p.path, p.params, new_q, p.fragment)
                )

            # 1) rel="next", aria-label, class có 'next'
            for sel in [
                'a[rel="next"]',
                'a[aria-label*="Next"]',
                'a[aria-label*="Sau"]',
                'a[aria-label*="Trang sau"]',
                'a[class*="next"]',
            ]:
                a = soup.select_one(sel)
                if a and a.get("href"):
                    u = urljoin(current_url, a["href"])
                    if domain in urlparse(u).netloc:
                        return _normalize_query(u)
                        return u

            # 2) Theo text
            next_texts = {
                "next",
                "older",
                "sau",
                "trang sau",
                "trang kế",
                "tiếp",
                "›",
                "»",
            }
            for a in soup.find_all("a"):
                t = (a.get_text() or "").strip().lower()
                if (
                    t in next_texts
                    or t.startswith("trang ")
                    and any(ch.isdigit() for ch in t)
                ):
                    href = a.get("href")
                    if href:
                        u = urljoin(current_url, href)
                        if domain in urlparse(u).netloc:
                            return _normalize_query(u)
            return None

        def strip_pagination(u: str) -> str:
            """
            Xoá mọi dấu vết phân trang khỏi URL:
            - Query: ?page=..., ?p=..., ?trang=...
            - Path: /page/2, /trang-3, /trang/4, /p5 ...
            Trả về base hub (tương đương trang 1).
            """
            p = urlparse(u)
            q = parse_qs(p.query or "", keep_blank_values=True)

            # Bỏ các key phân trang phổ biến
            for key in ("page", "p", "trang", "pi", "BRSR"):
                q.pop(key, None)

            new_path = p.path or ""

            # /page-2, /page/2, /trang-2, /trang/2 (+ .htm/.html) ở bất kỳ đâu trong path
            new_path = re.sub(
                r"/(?:page|trang)(?:-|/)?\d+(?:\.html?)?(?=[/?#]|$)",
                "",
                new_path,
                flags=re.IGNORECASE,
            )

            # /p/2, /p-2, /pi/2, /pi-2 (+ .htm/.html)
            new_path = re.sub(
                r"/p(?:i)?(?:-|/)?\d+(?:\.html?)?(?=[/?#]|$)",
                "",
                new_path,
                flags=re.IGNORECASE,
            )

            # hậu tố -p2(.htm/.html) ngay trước ?, #, hoặc hết chuỗi
            new_path = re.sub(
                r"-p\d+(?:\.html?)?(?=(?:[?#]|$))",
                "",
                new_path,
                flags=re.IGNORECASE,
            )

            # case đặc thù cũ (nếu còn cần)
            new_path = re.sub(
                r"/tim-kiem/trang-\d+\.chn(?=[/?#]|$)",
                "/tim-kiem",
                new_path,
                flags=re.IGNORECASE,
            )

            # chuẩn hoá dấu gạch chéo + bỏ đuôi '/'
            new_path = re.sub(r"//+", "/", new_path) or "/"
            if len(new_path) > 1 and new_path.endswith("/"):
                new_path = new_path[:-1]

            new_q = urlencode(q, doseq=True)

            return urlunparse(
                (p.scheme, p.netloc, new_path, p.params, new_q, p.fragment)
            )

        def _context_has_kw(
            a, p, keywords_norm: list[str], ind_norm: list[str]
        ) -> bool:
            path_l = (p.path or "").lower()
            a_txt = (a.get_text(" ", strip=True) or "").lower()
            head = a.find(["h2", "h3", "h4"]) or a.find_parent(["h2", "h3", "h4"])
            h_txt = head.get_text(" ", strip=True).lower() if head else ""
            attr_txt = " ".join(
                [
                    (a.get("title") or ""),
                    (a.get("aria-label") or ""),
                    (a.get("data-title") or ""),
                ]
            ).lower()

            should_expand = any(ch.isdigit() for ch in path_l) or any(
                (a.get(attr) or "").strip()
                for attr in (
                    "data-id",
                    "data-newsid",
                    "data-article-id",
                    "data-linkid",
                    "data-linktype",
                )
            )

            ctx_parts: list[str] = [path_l, a_txt, h_txt, attr_txt]

            def _append_text(tag) -> None:
                if not tag or not hasattr(tag, "get_text"):
                    return
                try:
                    text = tag.get_text(" ", strip=True)
                except Exception:
                    return
                if text:
                    ctx_parts.append(text.lower()[:600])

            if should_expand:
                parent = a if hasattr(a, "parent") else None
                seen_container = False
                steps = 0
                container_hints = (
                    "item",
                    "result",
                    "entry",
                    "story",
                    "article",
                    "post",
                    "card",
                    "search",
                    "box-category",
                    "listing",
                    "content",
                )
                while parent is not None and steps < 4:
                    parent = getattr(parent, "parent", None)
                    if not hasattr(parent, "get"):
                        break
                    classes = " ".join(parent.get("class") or ()).lower()
                    if parent.name in {"article", "li", "section", "div"} and any(
                        hint in classes for hint in container_hints
                    ):
                        _append_text(parent)
                        seen_container = True
                        break
                    steps += 1

                if (
                    not seen_container
                    and hasattr(a, "parent")
                    and hasattr(a.parent, "find_all")
                ):
                    for tag in a.parent.find_all(
                        ["p", "div", "span"],
                        limit=2,
                        recursive=False,
                    ):
                        classes = " ".join(tag.get("class") or ()).lower()
                        if any(
                            key in classes
                            for key in (
                                "sapo",
                                "summary",
                                "synopsis",
                                "desc",
                                "description",
                                "lead",
                            )
                        ):
                            _append_text(tag)
                            break
            ctx = " ".join(filter(None, ctx_parts))

            has_kw = any(k in ctx for k in (keywords_norm or []))
            has_ind = any(v in ctx for v in (ind_norm or []))
            return has_kw or has_ind

        def _extract_article_links_from_soup(
            soup: BeautifulSoup, current_page_url: str, ind_norm: set[str]
        ) -> tuple[list[str], bool]:
            links: list[str] = []
            hit_older_cutoff = False
            start_d = start_date.date()
            end_d = end_date.date()

            # --- Lấy domain hiện tại
            cur = urlparse(current_page_url)

            # =========================
            # 2) MẶC ĐỊNH
            # =========================
            for a in soup.select("a"):
                href = a.get("href", "")
                if not href:
                    continue
                if href.startswith(
                    ("#", "javascript:", "vbscript:", "mailto:", "tel:")
                ):
                    continue

                full_url = urljoin(current_page_url, href)
                p = urlparse(full_url)

                if p.scheme not in ("http", "https") or not p.netloc:
                    continue
                if p.path in ("", "/") or p.path.startswith(("/tags/", "/video/")):
                    continue

                kw_norm_list = [k.lower() for k in (keywords or [])]
                ind_norm_list = [v.lower() for v in (ind_norm or [])]
                relevant = _context_has_kw(a, p, kw_norm_list, ind_norm_list)

                art_date = date_from_url_path(p.path) or date_from_anchor_context(a)
                if art_date is not None:
                    if art_date < start_d:
                        hit_older_cutoff = True
                        break
                    if art_date > end_d:
                        continue

                if relevant:
                    links.append(full_url)

            return links, hit_older_cutoff

        def _as_str_for_quote(terms: set[str] | str, joiner: str = " ") -> str:
            # Cho phép truyền sẵn str (an toàn nếu nơi khác đổi type)
            if isinstance(terms, str):
                return " ".join(terms.split())
            # terms là set[str]: sort để URL ổn định, lọc rỗng
            items = [t.strip() for t in terms if str(t).strip()]
            return joiner.join(sorted(items))

        def _as_text(x) -> str | None:
            if x is None:
                return None
            if isinstance(x, str):
                return x
            if isinstance(x, (list, tuple, set)):
                # ghép bằng khoảng trắng, bỏ phần tử rỗng
                parts = [p for p in x if isinstance(p, str) and p.strip()]
                return " ".join(parts) if parts else None
            return str(x)

        def _hub_override(
            domain: str, kw: set[str], kw_raw: str | Sequence[str] | None = None
        ) -> list[str] | None:
            """
            - kw: bộ từ khoá (thường là đã normalize)
            - kw_raw: CHUỖI gốc do user nhập (giữ dấu). Nếu có, ta sẽ ưu tiên cho các site cần giữ dấu.
            """
            # 1) Chuỗi đã normalize (cũ)
            kw_norm = _as_str_for_quote(kw)  # ví dụ: "vinamilk" hoặc "loc troi"

            # 2) Chuỗi giữ dấu (nếu có), fallback về bản normalize
            raw_text = _as_text(kw_raw)
            kw_pref = (raw_text or kw_norm or "").strip()

            # Các rule mặc định dạng "đuôi domain" -> template URL (dùng {kw})
            TEMPLATES = {
                "vnexpress.net": "https://timkiem.vnexpress.net/?q={kw}",
                "dantri.com.vn": "https://dantri.com.vn/tim-kiem/{kw}.htm",
                "tuoitre.vn": "https://tuoitre.vn/tim-kiem.htm?keywords={kw}",
                "vietnamnet.vn": "https://vietnamnet.vn/tim-kiem?q={kw}",
                "thanhnien.vn": "https://thanhnien.vn/tim-kiem.htm?keywords={kw}",
                "tienphong.vn": "https://tienphong.vn/tim-kiem/?q={kw}",
                "cafef.vn": "https://cafef.vn/tim-kiem.chn?keywords={kw}",
                "cafebiz.vn": "https://cafebiz.vn/search.chn?keywords={kw}",
                "congthuong.vn": "https://congthuong.vn/search_enginer.html?q={kw}",
                "suckhoedoisong.vn": "https://suckhoedoisong.vn/tim-kiem.htm?keywords={kw}",
                "kenh14.vn": "https://kenh14.vn/tim-kiem.chn?keywords={kw}",
                "baoxaydung.vn": "https://baoxaydung.vn/tim-kiem.htm?keywords={kw}",
                "baotintuc.vn": "https://baotintuc.vn/Search.aspx?KeySearch={kw}&ar=1&op=1&dateF=&dateT=",
                "qdnd.vn": "https://www.qdnd.vn/tim-kiem/q/{kw}",
                "congan.com.vn": "https://congan.com.vn/tim-kiem?q={kw}&type=0&cid=&fromtime=",
                "congly.vn": "https://congly.vn/search?q={kw}",
                "reatimes.vn": "https://reatimes.vn/tim-kiem.htm?keyword={kw}",
                "bnews.vn": "https://bnews.vn/tim-kiem/{kw}/trang-1.html",
                "baochinhphu.vn": "https://baochinhphu.vn/tim-kiem.htm?keywords={kw}",
                "nhandan.vn": "https://nhandan.vn/tim-kiem/?q={kw}",
                "vtv.vn": "https://vtv.vn/tim-kiem.htm?keywords={kw}",
                "hanoimoi.vn": "https://hanoimoi.vn/search?q={kw}",
                "thuehaiquan.tapchikinhtetaichinh.vn": "https://thuehaiquan.tapchikinhtetaichinh.vn/search_enginer.html?p=tim-kiem&q={kw}",
                "nld.com.vn": "https://nld.com.vn/search.chn?keywords={kw}",
                "vneconomy.vn": "https://vneconomy.vn/tim-kiem.html?Text={kw}",
                "diendandoanhnghiep.vn": "https://diendandoanhnghiep.vn/search?q={kw}",
                "hanoionline.vn": "https://hanoionline.vn/tim-kiem?search={kw}",
                "baodaklak.vn": "https://baodaklak.vn/tim-kiem/?key={kw}&searchmore=0&fromdate=4&cate=",
                "sggp.org.vn": "https://www.sggp.org.vn/tim-kiem/?q={kw}",
                "theleader.vn": "https://theleader.vn/{kw}-search/",
                "vietnamplus.vn": "https://www.vietnamplus.vn/tim-kiem/?q={kw}",
                "nongnghiepmoitruong.vn": "https://nongnghiepmoitruong.vn/{kw}-search/from-to-sign-/",
                "vir.com.vn": "https://vir.com.vn/search_enginer.html?p=search&q={kw}",
                "giadinh.suckhoedoisong.vn": "https://giadinh.suckhoedoisong.vn/tim-kiem.htm?keywords={kw}",
                "doisongphapluat.com.vn": "https://doisongphapluat.com.vn/tim-kiem?q={kw}",
                "toquoc.vn": "https://toquoc.vn/tim-kiem.htm?keywords={kw}",
                "giaoducthoidai.vn": "https://giaoducthoidai.vn/tim-kiem/?q={kw}",
                "sgtt.thesaigontimes.vn": "https://sgtt.thesaigontimes.vn/tim-kiem-sgtt/#gsc.tab=0&gsc.q={kw}&gsc.sort=",
            }

            # 1) Rule đặc biệt cho bnews.vn (phải xử lý trước khi tra TEMPLATES)
            if domain.endswith("bnews.vn"):
                # Sử dụng bản normalize để so “biến thể”
                norm = kw_norm.lower()
                if norm in {"a an", "a-an", "aan"}:
                    return [
                        f"https://bnews.vn/tim-kiem/{quote_plus('gạo', safe='')}/trang-1.html"
                    ]
                # có thể bổ sung mặc định khác ở đây nếu cần

            # 2) Rule mặc định theo template
            for suffix, tpl in sorted(
                TEMPLATES.items(), key=lambda kv: len(kv[0]), reverse=True
            ):
                if domain.endswith(suffix):
                    # Với một số domain (như baotintuc, tuoitre, vneconomy, v.v.) nên GIỮ DẤU để search chính xác
                    if suffix in {
                        "baotintuc.vn",
                        "tuoitre.vn",
                        "vneconomy.vn",
                        "suckhoedoisong.vn",
                        "hanoionline.vn",
                        "congthuong.vn",
                        "qdnd.vn",
                        "congan.com.vn",
                        "congly.vn",
                        "reatimes.vn",
                        "baochinhphu.vn",
                        "nhandan.vn",
                        "vtv.vn",
                        "hanoimoi.vn",
                        "thuehaiquan.tapchikinhtetaichinh.vn",
                        "baodaklak.vn",
                        "sggp.org.vn",
                        "baoxaydung.vn",
                        "theleader.vn",
                        "vietnamplus.vn",
                        "nongnghiep.vn",
                        "vir.com.vn",
                        "giadinh.suckhoedoisong.vn",
                        "doisongphapluat.com.vn",
                        "toquoc.vn",
                        "giaoducthoidai.vn",
                        "sgtiepthi",
                    }:
                        q = quote_plus(
                            kw_pref, safe=""
                        )  # "lộc trời" -> "l%E1%BB%99c+tr%E1%BB%9Di"
                    else:
                        # các site còn lại chấp nhận không dấu hoặc dấu → tuỳ bạn chọn:
                        # (a) ưu tiên giữ dấu:
                        # q = quote_plus(kw_pref, safe="")
                        # (b) ưu tiên normalize (giống cũ):
                        q = quote_plus(kw_norm, safe="")
                    return [tpl.format(kw=q)]

            return None

        try:
            kw_norm_set = {_norm(k) for k in (keywords or [])}
            hubs_override = _hub_override(media_source.domain, kw_norm_set, keywords)
            # Lấy danh sách hub, giới hạn theo config nếu có
            hubs = hubs_override or get_hub_links_for_domain(
                media_source.domain, keywords, industry_name, max_results=30, limit=None
            )
            if not hubs:
                logger.warning(
                    f"[{media_source.name}] ❌ Không tìm thấy hub phù hợp cho từ khóa: {keywords}"
                )
                return CrawlResult(
                    source_name=media_source.name,
                    source_type=media_source.type,
                    url="",
                    articles_found=[],
                    crawl_status="failed",
                    error_message="Hub URL not found",
                    crawl_duration=0.0,
                )

            # Giới hạn số hub và số trang/hub nếu muốn
            # MAX_HUBS = getattr(self.config, "max_hubs", 5)          # ví dụ: tối đa 5 hub
            MAX_PAGES_PER_HUB = 3  # ví dụ: tối đa 3 trang mỗi hub
            pages_per_hub = 3 if hubs_override else MAX_PAGES_PER_HUB

            logger.info(f"[{media_source.name}] 🔗 {len(hubs)} hub: {hubs}")

            await _maybe_await(self.check_pause_or_cancel)
            ind_norm = _build_industry_variants(industry_name)
            domain = media_source.domain

            visited_pages_global: set[str] = set()
            page_urls: list[str] = []  # toàn cục để log
            all_links: list[str] = []  # toàn cục tất cả link bài
            all_link_hubs: list[int] = (
                []
            )  # mapping link -> hub_idx (song song với all_links)

            # --- CRAWL THEO TỪNG HUB ---
            for hub_idx, raw_hub_url in enumerate(hubs):
                try:
                    if not raw_hub_url.startswith(("http://", "https://")):
                        logger.error(
                            f"[{media_source.name}] ❌ Hub URL không hợp lệ: {raw_hub_url}"
                        )
                        continue

                    current_url = raw_hub_url

                    # Nếu là tag hub hoặc đang ở page >1 → reset về trang 1
                    if is_tag_hub_url(current_url, keywords):
                        cur_no = _get_current_page_num(current_url)
                        if cur_no and cur_no > 1:
                            picked = strip_pagination(current_url)
                            logger.info(
                                f"[{media_source.name}][hub {hub_idx}] 🔄 Reset hub về trang 1: {picked} (was {current_url})"
                            )
                            current_url = picked

                    current_page_num = _get_current_page_num(current_url)
                    visited_pages_hub: set[str] = set()

                    for page_idx in range(1, pages_per_hub + 1):
                        if not current_url:
                            break
                        if (
                            current_url in visited_pages_hub
                            or current_url in visited_pages_global
                        ):
                            logger.info(
                                f"[{media_source.name}][hub {hub_idx}] ⛔ Trang đã visit: {current_url}"
                            )
                            break

                        await _maybe_await(self.check_pause_or_cancel)
                        logger.info(
                            f"[{media_source.name}] 🌐 Crawling hub {hub_idx} page {page_idx}: {current_url}"
                        )
                        if domain in (
                            "tienphong.vn",
                            "nld.com.vn",
                            "cafebiz.vn",
                            "suckhoedoisong.vn",
                            "kenh14.vn",
                            "baoxaydung.vn",
                            "congan.com.vn",
                            "congly.vn",
                            "reatimes.vn",
                            "baochinhphu.vn",
                            "nhandan.vn",
                            "tuoitre.vn",
                            "vtv.vn",
                            "hanoimoi.vn",
                            "sggp.org.vn",
                            "theleader.vn",
                            "vietnamplus.vn",
                            "nongnghiep.vn",
                            "toquoc.vn",
                            "giaoducthoidai.vn",
                        ):
                            html = await crawl_infinite_listing(
                                current_url, max_rounds=7, idle_ms=700
                            )
                        else:
                            html = await crawl_with_playwright(current_url)
                        soup = BeautifulSoup(html or "", "html.parser")

                        # Thu link từ trang hiện tại
                        links, hit_old = _extract_article_links_from_soup(
                            soup, current_url, ind_norm
                        )
                        all_links.extend(links)
                        all_link_hubs.extend([hub_idx] * len(links))

                        visited_pages_hub.add(current_url)
                        visited_pages_global.add(current_url)
                        page_urls.append(current_url)

                        if hit_old:
                            logger.info(
                                f"[{media_source.name}][hub {hub_idx}] ⛔ reached older-than-window; stop this hub."
                            )
                            break

                        if page_idx >= pages_per_hub:
                            break

                        # Tìm trang kế
                        next_url = _find_next_link(soup, current_url, domain)
                        if not next_url:
                            if (
                                domain == "diendandoanhnghiep.vn"
                                and "search?q=" in current_url
                            ):
                                # Nếu là trang tìm kiếm, không cần tăng trang
                                next_url = None
                            elif domain in (
                                "tuoitre.vn",
                                "thanhnien.vn",
                                "tienphong.vn",
                                "nld.com.vn",
                                "cafebiz.vn",
                                "suckhoedoisong.vn",
                                "kenh14.vn",
                                "baoxaydung.vn",
                                "congan.com.vn",
                                "congly.vn",
                                "reatimes.vn",
                                "baochinhphu.vn",
                                "nhandan.vn",
                                "vtv.vn",
                                "hanoimoi.vn",
                                "baodaklak.vn",
                                "sggp.org.vn",
                                "theleader.vn",
                                "vietnamplus.vn",
                                "nongnghiep.vn",
                                "toquoc.vn",
                                "giaoducthoidai.vn",
                                "sgtiepthi",
                            ):
                                next_url = None
                            else:
                                # Đoán URL theo mẫu phổ biến
                                next_no = current_page_num + 1

                                # Ưu tiên mẫu /trang-N.html nếu khớp
                                next_url = _next_page_numbered_html(
                                    current_url, next_no
                                )

                                if not next_url:
                                    candidates = _make_url_with_page(
                                        current_url, next_no
                                    )
                                    next_url = None
                                    for cand in candidates:
                                        if (
                                            (domain in urlparse(cand).netloc)
                                            and (cand not in visited_pages_hub)
                                            and (cand not in visited_pages_global)
                                        ):
                                            next_url = cand
                                            break
                        if not next_url:
                            logger.info(
                                f"[{media_source.name}][hub {hub_idx}] ⛔ Không tìm thấy trang kế tiếp từ {current_url}"
                            )
                            break

                        current_page_num += 1
                        current_url = next_url

                except Exception as e:
                    logger.warning(
                        f"[{media_source.name}] ⚠️ Lỗi khi crawl hub {hub_idx}: {e}"
                    )
                    continue

            sem = Semaphore(4)  # tuỳ quota
            start_t = time.monotonic()
            all_articles = []

            async def process_link(link):
                await _maybe_await(self.check_pause_or_cancel)

                # 1) HTTP trước
                try:
                    dom = urlparse(link).netloc
                    await DOMAIN_LIMITER.enter(link)
                    r = await http_client.get(
                        link,
                        timeout=15,
                        follow_redirects=True,
                        headers={
                            **_random_ua(),
                            **_rand_headers(),
                            "Referer": f"https://{dom}/",
                        },
                    )
                finally:
                    DOMAIN_LIMITER.release(dom)

                content_type = r.headers.get("content-type", "")
                art_html = (
                    r.text
                    if (r.status_code == 200 and "text/html" in content_type)
                    else ""
                )

                # 2) Nếu nghi bị block/HTML quá ngắn → Playwright
                if (
                    (not art_html)
                    or (len(art_html) < MIN_HTML_LEN_HEUR)
                    or _looks_blocked(art_html)
                ):
                    art_html = await crawl_with_playwright(link)

                # 2. Extract ngày + tiêu đề
                meta = extract_dates_rule_based(art_html, link)
                title_rb = extract_title_rule_based(art_html)
                # Quick window check before any heavy parsing
                iso = (meta or {}).get("published_iso")
                if iso:
                    d = _parse_date_soft(str(iso))
                    if d is not None:
                        if d < start_date.date():
                            raise _HubCutoff(
                                url=link,
                                found_date=d,
                                threshold=start_date.date(),
                                reason="older-than-window",
                            )
                        if d > end_date.date():
                            logger.debug(
                                f"[HubCrawlTool] Skip too-new {link} | {d} > {end_date.date()}"
                            )
                            return []
                # Debug: rule-based signals before deciding fast-path
                try:
                    logger.info(
                        "[HubCrawlTool][RB] signals | link=%s | date_iso=%s | title_found=%s | html_len=%d",
                        link,
                        (meta or {}).get("published_iso"),
                        bool(title_rb),
                        len(art_html or ""),
                    )
                    missing = []
                    if not (meta or {}).get("published_iso"):
                        missing.append("date")
                    if not title_rb:
                        missing.append("title")
                    if missing:
                        logger.info(
                            "[HubCrawlTool][RB] quick-path prerequisites missing: %s",
                            ", ".join(missing),
                        )
                except Exception:
                    pass

                parsed = None

                def _is_valid_article(obj) -> bool:
                    """Đảm bảo article có đủ field tối thiểu."""
                    if not obj:
                        return False
                    # parser.parse có thể trả list[Article], hoặc một Article đơn
                    if isinstance(obj, list):
                        if not obj:
                            return False
                        obj = obj[0]
                    try:
                        t = (
                            getattr(obj, "tieu_de", None)
                            or getattr(obj, "Tiêu đề", None)
                            or getattr(obj, "cum_noi_dung_chi_tiet", None)
                        )
                        d = getattr(obj, "ngay_phat_hanh", None) or getattr(
                            obj, "Ngày phát hành", None
                        )
                        l = getattr(obj, "link_bai_bao", None) or getattr(
                            obj, "Link", None
                        )
                        s = getattr(obj, "tom_tat_noi_dung", None) or getattr(
                            obj, "Tóm tắt", None
                        )
                        return bool(t and d and l and s)
                    except Exception:
                        return False

                # FAST-PATH: nếu có ngày ISO + có tiêu đề + HTML đủ dài → bỏ qua LLM
                if (
                    meta.get("published_iso")
                    and title_rb
                    and len(art_html) >= MIN_HTML_LEN_HEUR
                ):
                    quick_json = json.dumps(
                        {
                            "Tiêu đề": title_rb,
                            "Ngày phát hành": meta["published_iso"],
                            "Nguồn trích ngày": meta.get("source_published_text") or "",
                            "Tóm tắt": _quick_summary_from_html(art_html),
                            "Link": link,
                            "nhan_hang": keywords,
                        },
                        ensure_ascii=False,
                    )
                    try:
                        # Debug preview of quick_json (trim to 300 chars)
                        try:
                            _prev = (quick_json or "")[:300].replace("\n", " ")
                        except Exception:
                            _prev = "<unavailable>"
                        logger.info(
                            "[HubCrawlTool][RB] quick_json preview: %s...", _prev
                        )
                        parsed = self.parser.parse(
                            quick_json, media_source, industry_name
                        )
                        # Debug: if invalid after quick parse, log missing fields
                        if not _is_valid_article(parsed):
                            try:
                                o = (
                                    parsed[0]
                                    if isinstance(parsed, list) and parsed
                                    else parsed
                                )
                                has_title = bool(
                                    getattr(o, "tieu_de", None)
                                    or getattr(o, "Tiêu đề", None)
                                    or getattr(o, "cum_noi_dung_chi_tiet")
                                )
                                has_date = bool(
                                    getattr(o, "ngay_phat_hanh", None)
                                    or getattr(o, "Ngày phát hành", None)
                                )
                                has_link = bool(
                                    getattr(o, "link_bai_bao", None)
                                    or getattr(o, "Link", None)
                                )
                                has_summary = bool(
                                    getattr(o, "tom_tat", None)
                                    or getattr(o, "Tóm tắt", None)
                                    or getattr(o, "tom_tat_noi_dung", None)
                                )
                                _miss = [
                                    k
                                    for k, v in {
                                        "title": has_title,
                                        "date": has_date,
                                        "link": has_link,
                                        "summary": has_summary,
                                    }.items()
                                    if not v
                                ]
                                logger.info(
                                    "[HubCrawlTool][RB] quick parse invalid -> missing: %s",
                                    ", ".join(_miss) or "none",
                                )
                            except Exception:
                                pass
                        if _is_valid_article(parsed):
                            logger.info(
                                "[HubCrawlTool] ✅ fast-path (no LLM) for %s", link
                            )
                            return parsed  # ← TRẢ VỀ SỚM, KHÔNG GỌI LLM
                    except Exception as e:
                        logger.warning(
                            "[HubCrawlTool] fast-path parse lỗi, sẽ fallback LLM: %s", e
                        )
                        parsed = None

                # Nếu fast-path không hợp lệ → fallback LLM
                if not _is_valid_article(parsed):
                    if (
                        (meta or {}).get("published_iso")
                        and title_rb
                        and len(art_html) >= MIN_HTML_LEN_HEUR
                    ):
                        prompt = self.build_prompt_with_known_date(
                            link=link,
                            known_date_iso=meta["published_iso"],
                            known_date_source=meta.get("source_published_text") or "",
                            known_title=title_rb,
                        )
                    else:
                        # fallback: chưa chắc ngày → dùng prompt đầy đủ
                        prompt = self.build_prompt(link, start_date, end_date)

                    # 3) Gọi agent
                    resp = await self._arun_with_user_fallback(
                        prompt,
                        session_id=self.session_id,
                        preferred_provider=getattr(self.config, "provider", None),
                        preferred_model=getattr(self.config, "model", None),
                    )
                    text = await _get_response_text(resp)
                    logger.info(f"... HubAgent response for {link}:\n{text[:2000]}")
                    parsed = self.parser.parse(text, media_source, industry_name)

                return parsed

            def normalize_url(s: str) -> str:
                # 1. Chuyển mã URL encoding (ví dụ: %20 thành dấu cách)
                s = urllib.parse.unquote(s)

                # 2. Loại bỏ dấu (chuyển chữ có dấu thành không dấu)
                s = "".join(
                    c
                    for c in unicodedata.normalize("NFD", s)
                    if unicodedata.category(c) != "Mn"
                )

                # 3. Thay thế mọi ký tự không phải chữ cái hoặc số (dấu cách, gạch ngang, gạch dưới, v.v.) thành dấu cách
                s = re.sub(r"[^a-zA-Z0-9]+", " ", s)

                # 4. Chuyển thành chữ thường và loại bỏ khoảng trắng dư thừa
                return s.lower().strip()

            async def _retry_process_link(
                process_link, link: str, max_retries: int = 2
            ) -> list:
                """
                Gọi process_link(link) với retry + exponential backoff.
                - Thành công khi trả về list KHÔNG RỖNG.
                - Nếu parse trả [] hoặc None hoặc ném exception → retry (tối đa max_retries).
                - Hết retry → trả [] để pipeline tiếp tục.
                """
                for attempt in range(max_retries + 1):  # 0..max_retries
                    try:
                        parsed = await process_link(link)
                        # Treat [] as a valid outcome (e.g., filtered by date). Retry only on None/exception.
                        if parsed is None:
                            raise RuntimeError("Parser returned None")
                        if isinstance(parsed, list):
                            return parsed
                        if parsed:  # list có phần tử
                            return parsed
                        # treat empty as failure để thử lại
                        raise RuntimeError("Parser returned empty list")
                    except _HubCutoff as sig:
                        logger.info(f"[HubCrawlTool] {sig}")
                        return CUT_HUB
                    except Exception as e:
                        # Lần cuối → dừng
                        if attempt >= max_retries:
                            # log ngắn gọn; có thể thêm traceback nếu cần
                            logger.warning(
                                f"Parse fail {link}: {e} (exhausted retries)"
                            )
                            return []
                        # Backoff + jitter nhẹ
                        delay = min(2**attempt, 8) + random.random() * 0.5
                        logger.info(
                            f"[retry] parse {link} attempt {attempt + 1}/{max_retries} in {delay:.1f}s: {e}"
                        )
                        await asyncio.sleep(delay)

            # --- Lọc hợp lệ + dedup GIỮ mapping hub ---
            items = []
            for url, hub_idx in zip(all_links, all_link_hubs):
                netloc = urlparse(url).netloc
                if (
                    netloc.endswith(domain)
                    and "admicro" not in netloc
                    and "adn" not in netloc
                ):
                    items.append((url, hub_idx))

            # dedup theo URL nhưng giữ hub đầu tiên phát hiện
            seen = set()
            dedup_items = []
            for url, hub_idx in items:
                if url in seen:
                    continue
                seen.add(url)
                dedup_items.append((url, hub_idx))

            # gom theo hub, có thể giữ thứ tự phát hiện
            by_hub: "OrderedDict[int, list[str]]" = OrderedDict()
            for url, hub_idx in dedup_items:
                by_hub.setdefault(hub_idx, []).append(url)

            # (khuyến nghị) sắp xếp trong từng hub: có ngày trong URL trước, mới → cũ
            def _infer_date_from_url(u: str):
                try:
                    return date_from_url_path(urlparse(u).path)
                except Exception:
                    return None

            for h, lst in by_hub.items():

                def key(u: str):
                    d = _infer_date_from_url(u)
                    return (0 if d is not None else 1, -(d.toordinal()) if d else 0)

                lst.sort(key=key)

            _flattened = [u for lst in by_hub.values() for u in lst]
            _sample = list(islice(_flattened, 5))

            if _sample:
                lines = []
                for i, u in enumerate(_sample, 1):
                    d = _infer_date_from_url(u)
                    lines.append(
                        f"{i}. {u}" + (f"  (date={d.isoformat()})" if d else "")
                    )
                logger.info(
                    "[%s] 🔍 Top %d link sau sort:\n%s",
                    media_source.name,
                    len(_sample),
                    "\n".join(lines),
                )

            logger.info(
                f"[{media_source.name}] 🔗 Tìm được {sum(len(v) for v in by_hub.values())} bài viết từ {len(page_urls)} trang hub"
            )

            for hub_idx, link_list in by_hub.items():
                logger.info(
                    f"[hub {hub_idx}] ▶ start processing {len(link_list)} links"
                )
                hub_url_norm = normalize_url(by_hub[hub_idx][0])
                hub_contains_keywords = any(
                    normalize_url(keyword) in hub_url_norm for keyword in keywords
                )
                for link in link_list:
                    if not hub_contains_keywords:
                        # Lọc bài viết không chứa từ khóa trong tóm tắt
                        html = await crawl_with_playwright(link)
                        summary = _quick_summary_from_html(html)

                        # Kiểm tra nếu tóm tắt không chứa từ khóa theo dạng nguyên chuỗi
                        found_keywords = False
                        for keyword in keywords:
                            # Tạo biểu thức chính quy để tìm từ khóa nguyên vẹn (dùng \b để bao quanh từ khóa)
                            pattern = r"\b" + re.escape(keyword.lower()) + r"\b"
                            if re.search(pattern, summary.lower()):
                                found_keywords = True
                                break  # Nếu tìm thấy một từ khóa, không cần kiểm tra các từ khóa khác

                        if not found_keywords:
                            continue  # Bỏ qua bài viết không chứa từ khóa nguyên vẹn
                    try:
                        res = await _retry_process_link(
                            process_link, link, max_retries=2
                        )
                        if res is CUT_HUB:
                            logger.info(
                                f"[hub {hub_idx}] ⛔ cutoff reached at {link} — stop this hub"
                            )
                            break
                        if res:  # list[Article]
                            all_articles.extend(res)
                    except Exception:
                        pass

            duration = time.monotonic() - start_t

            def to_date(x):
                if isinstance(x, datetime):
                    return x.date()
                if isinstance(x, date):
                    return x
                if isinstance(x, str):
                    # Ưu tiên ISO
                    s = x.strip()
                    try:
                        return datetime.fromisoformat(x).date()
                    except Exception:
                        pass
                    try:
                        return datetime.strptime(s, "%d-%m-%Y").date()
                    except Exception:
                        pass
                    except Exception:
                        m = _VN_DATE_RE.search(x)
                        if m:
                            d, mo, y = (
                                int(m.group("d")),
                                int(m.group("m")),
                                int(m.group("y")),
                            )
                            return date(y, mo, d)
                return None

            def in_range(a: Article):
                pub = to_date(a.ngay_phat_hanh)
                return pub is not None and start_date.date() <= pub <= end_date.date()

            filtered = [a for a in all_articles if in_range(a)]

            return CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url="; ".join(hubs),
                articles_found=filtered,
                crawl_status="success" if filtered else "failed",
                error_message="" if filtered else "Không có bài hợp lệ trong hub",
                crawl_duration=duration,
            )

        except Exception as e:
            logger.error(f"[{media_source.name}] ❌ Lỗi crawl hub: {e}", exc_info=True)
            return CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url="; ".join(hubs) if "hubs" in locals() else "",
                articles_found=[],
                crawl_status="failed",
                error_message=str(e),
                crawl_duration=duration if "duration" in locals() else 0.0,
            )

    def build_prompt_with_known_date(
        self,
        link: str,
        known_date_iso: str,
        known_date_source: str = "",
        known_title: str | None = None,
    ) -> str:
        """
        Khi đã rút được ngày đăng (ISO) bằng rule-based, khoá ngày đó lại để LLM không đoán sai.
        """
        return f"""
        Nhiệm vụ: Truy cập URL: {link}, đọc bài và TRẢ VỀ JSON đúng schema bên dưới.
        Lưu ý: Ngày phát hành đã được xác định chắc chắn từ metadata: {known_date_iso}.
        Bạn KHÔNG được suy đoán hay thay đổi ngày này.

        {{
        "Tiêu đề": "{(known_title or '').replace('"','').strip()}" if empty -> trích từ bài,
        "Ngày phát hành": "{known_date_iso}",
        "Nguồn trích ngày": "{known_date_source.replace('"','')[:160]}",
        "Tóm tắt": "≤ 100 từ, nêu sự kiện chính, các bên liên quan, kết quả/tác động, không lặp lại tiêu đề",
        "Link": "{link}"
        }}
        """

    def build_prompt(
        self,
        link: str,
        start_date: datetime,
        end_date: datetime,
    ) -> str:
        date_filter = f"từ ngày {start_date.strftime('%Y-%m-%d')} đến ngày {end_date.strftime('%Y-%m-%d')}"
        return f"""  
        Truy cập URL: {link} và phân tích bài báo. TRẢ VỀ DUY NHẤT MỘT OBJECT JSON theo schema dưới đây (không markdown, không giải thích).

        {{
        "Tiêu đề": "Tiêu đề đầy đủ của bài viết",
        "Ngày phát hành": "YYYY-MM-DD",
        "Nguồn trích ngày": "Chuỗi ngày/giờ đúng NGUYÊN VĂN bạn thấy (ví dụ: '31/07/2025 11:00 (GMT+7)')",
        "Ngày cập nhật": "DD-MM-YYYY hoặc null nếu không có",
        "Tóm tắt": "≤ 100 từ, nêu sự kiện chính, các bên liên quan, kết quả/tác động",
        "Link": "{link}"
        }}

        QUY TẮC LẤY NGÀY:
        1) ĐƯỢC PHÉP dùng metadata: JSON-LD Article.datePublished, meta[article:published_time], <time datetime>.
        2) KHÔNG dùng ngày giao diện (top bar, sidebar, footer).
        3) Nếu có nhiều mốc (đăng/cập nhật), ưu tiên NGÀY ĐĂNG GỐC (published). Chỉ điền "Ngày cập nhật" nếu tìm thấy mốc cập nhật.
        4) Chỉ nhận bài trong khoảng {date_filter}. Nếu ngày phát hành ngoài khoảng, trả JSON nhưng ngày phát hành phải là ngày đúng bạn tìm thấy (đừng tự đổi).
        5) Định dạng ngày bắt buộc: YYYY-MM-DD.
        """


async def run_in_domain_batches(
    jobs: list,  # list các “job” (bạn tự định nghĩa)
    get_domain,  # fn: job -> domain string
    run_job,  # fn async: job -> result
    max_domains_parallel: int = 2,  # tối đa 2 domain chạy cùng lúc
    max_per_domain: int = 2,  # tối đa 2 job/kw song song / domain
    domain_cooldown_s: float = 15.0,  # nghỉ giữa 2 domain
):
    """
    Ví dụ:
      jobs = [{'url': hub_url, 'kw': kw, ...}, ...]
      get_domain = lambda j: urlparse(j['url']).netloc
      run_job = crawl_one_keyword_on_hub
    """
    # group theo domain
    buckets = defaultdict(list)
    for j in jobs:
        buckets[get_domain(j)].append(j)

    domains = list(buckets.keys())

    # Semaphore để giới hạn số domain chạy đồng thời
    dom_sem = asyncio.Semaphore(max_domains_parallel)
    results = {}

    async def process_one_domain(dom):
        async with dom_sem:
            # giới hạn song song trong domain
            sem = asyncio.Semaphore(max_per_domain)

            async def _worker(j):
                async with sem:
                    return await run_job(j)

            # chạy tuần tự theo “đợt nhỏ” để không nổ tải ngay
            dom_jobs = buckets[dom]
            # nếu muốn thực sự tuần tự 1-1, set max_per_domain=1
            try:
                results[dom] = await asyncio.gather(
                    *[_worker(j) for j in dom_jobs], return_exceptions=True
                )
            finally:
                # cooldown nhẹ giữa các domain
                await asyncio.sleep(domain_cooldown_s)

    # chạy các domain (tối đa max_domains_parallel domain song song)
    await asyncio.gather(*[process_one_domain(dom) for dom in domains])
    return results


# <--- Agent Class -->
class CrawlerAgent(LLMUserFallbackMixin):
    """Agent chuyên crawl web, sử dụng prompt tiếng Việt."""

    def __init__(
        self,
        model: Any,
        config: CrawlConfig,
        parser: ArticleParser,
        session_id: Optional[str] = None,
        check_cancelled: Optional[callable] = None,
        check_paused: Optional[callable] = None,
        check_pause_or_cancel: Optional[callable] = None,
        user_email: Optional[str] = None,
        status: Optional[BotStatus] = None,
        on_progress_update: Optional[callable] = None,
    ):
        self.parser = parser
        self.config = config
        self.session_id = session_id
        self.search_tools = [
            ddgs_search_text,
            # ArxivTools(),
            # BaiduSearchTools(),
            # HackerNewsTools(),
            # PubmedTools(),
            # WikipediaTools(),
            # GoogleSearchWithDelay(
            #     fixed_language="vi", timeout=60, fixed_max_results=50
            # ),
        ]
        self.search_tool_index = 0
        self.model = model
        self.agent = None
        self.cache_manager = SafeCacheManager(
            cache_dir=None,  # Will use environment variable
            ttl_hours=self.config.cache_duration_hours,
            version="1.1",
        )
        self.check_cancelled = check_cancelled or (lambda: False)
        self.check_paused = check_paused or (lambda: False)
        self.check_pause_or_cancel = check_pause_or_cancel
        self.user_email = user_email
        self.rotate_index = 0
        self.status = status
        self.on_progress_update = on_progress_update
        self.hub_tool = HubCrawlTool(
            model=self.model,
            config=self.config,
            session_id=self.session_id,
            parser=self.parser,
            check_pause_or_cancel=self.check_pause_or_cancel,
        )
        self.cse_agent = CSEArticleAgent(
            model=self.model,
            config=self.config,
            parser=self.parser,
            session_id=self.session_id,
            check_pause_or_cancel=self.check_pause_or_cancel,
        )

    def return_partial_result(self, media_source: MediaSource) -> CrawlResult:
        unique_articles = {
            article.link_bai_bao: article
            for article in getattr(self, "current_articles", [])
        }
        articles = list(unique_articles.values())

        return CrawlResult(
            source_name=media_source.name,
            source_type=media_source.type,
            url=media_source.domain,
            articles_found=articles,
            crawl_status="partial" if articles else "timeout",
            error_message="Trả về các bài đã crawl được trước khi timeout.",
            crawl_duration=self.config.crawl_timeout,
        )

    def _create_agent(self):
        if self.agent:
            del self.agent
        current_tool = self.search_tools[self.search_tool_index]

        self.agent = Agent(
            name="MediaCrawler",
            role="Web Crawler và Content Extractor",
            model=self.model,
            tools=[Crawl4aiTools(max_length=2000), current_tool],
            instructions=[
                "Bạn là một chuyên gia crawl web để theo dõi truyền thông tại Việt Nam.",
                "Nhiệm vụ: Crawl các website báo chí để tìm bài viết về các đối thủ cạnh tranh dựa trên keywords, nhãn hàng và ngành hàng.",
                "Ưu tiên tin tức mới nhất trong khoảng thời gian được chỉ định",
                "Chỉ lấy các bài viết được đăng trong khoảng thời gian được yêu cầu.",
                "Nếu không tìm thấy bất kỳ bài viết nào, KHÔNG tự tạo nội dung, KHÔNG trả về kết quả giả, và để phản hồi trống.",
                "Không lấy các bài viết đăng trước hoặc sau khoảng thời gian chỉ định.",
                "Trả về kết quả dạng JSON hợp lệ chứa danh sách bài báo với các trường: tiêu đề, ngày phát hành (DD-MM-YYYY), tóm tắt nội dung, link bài báo.",
            ],
            show_tool_calls=True,
            markdown=True,
            add_datetime_to_instructions=True,
        )

    # def _rotate_tool(self):
    #     self.search_tool_index = (self.search_tool_index + 1) % len(self.search_tools)
    #     self._create_agent()

    @retry(
        stop=stop_after_attempt(2),  # thay vì 3-5
        wait=wait_exponential(multiplier=0.5, min=0.5, max=2),
        reraise=True,
    )
    async def crawl_media_source(
        self,
        media_source: MediaSource,
        industry_name: str,
        keywords: List[str],
        start_date: datetime,
        end_date: datetime,
    ) -> CrawlResult:
        start_time = datetime.now()
        date_filter = f"từ ngày {start_date.strftime('%Y-%m-%d')} đến ngày {end_date.strftime('%Y-%m-%d')}"
        domain_url = media_source.domain
        if domain_url and not domain_url.startswith("http"):
            domain_url = f"https://{domain_url}"

        def to_date(dt):
            return dt.date() if isinstance(dt, datetime) else dt

        keyword_groups = [[kw] for kw in keywords]
        articles: List[Article] = []
        self.current_articles = []

        # Khôi phục checkpoint nếu có
        task = next(
            (
                t
                for t in task_manager.get_tasks(self.user_email)
                if t["session_id"] == self.session_id
            ),
            None,
        )
        checkpoint = (
            task.get("crawl_checkpoint", {}).get(media_source.name, {}) if task else {}
        )
        tools_to_try = checkpoint.get("tool_order")
        if tools_to_try is None:
            tools_to_try = list(range(len(self.search_tools)))

        start_group_index = checkpoint.get("group_index", 0)
        start_tool_index = checkpoint.get("tool_index", 0)

        try:
            # LẶP TỪNG KEYWORD
            for g_idx in range(start_group_index, len(keyword_groups)):
                await _maybe_await(self.check_pause_or_cancel)
                group = keyword_groups[g_idx]  # ví dụ ["Tường An"]
                tool_index0 = start_tool_index if g_idx == start_group_index else 0
                kw_str = ", ".join(group)

                # progress text rõ keyword
                if self.on_progress_update:
                    self.on_progress_update(
                        source_name=media_source.name,
                        completed=self.status.completed_sources,
                        failed=self.status.failed_sources,
                        progress=(
                            (self.status.completed_sources + self.status.failed_sources)
                            / max(1, self.status.total_sources)
                        )
                        * 100.0,
                        current_task=f"Crawling {media_source.name} ({industry_name}) – keyword: {kw_str}",
                    )
                found_for_this_keyword = False

                if self.config.use_hub_page:
                    try:
                        result = await self.hub_tool.run(
                            media_source, group, start_date, end_date, industry_name
                        )
                        if result and result.articles_found:
                            # 1) Giữ batch hiện tại để UI hiển thị ngay
                            self.current_articles = list(
                                result.articles_found
                            )  # shallow copy

                            # cộng vào kho đang tích lũy để trả về:
                            articles.extend(result.articles_found)

                            # Đánh dấu đã tìm thấy ở hub -> KHÔNG chạy fallback cho keyword này
                            found_for_this_keyword = True

                            # Cập nhật tiến độ (tuỳ UI của bạn)
                            if self.on_progress_update:
                                self.on_progress_update(
                                    source_name=media_source.name,
                                    completed=self.status.completed_sources,
                                    failed=self.status.failed_sources,
                                    progress=(
                                        (
                                            self.status.completed_sources
                                            + self.status.failed_sources
                                        )
                                        / self.status.total_sources
                                    )
                                    * 100.0,
                                    current_task=(
                                        f"Đã tìm {len(self.current_articles)} bài từ hub "
                                        f"({industry_name}) – chuyển sang từ khóa tiếp theo"
                                    ),
                                )

                            # Chuyển sang từ khóa kế tiếp (bỏ nhánh fallback cho keyword này)
                            continue

                    except Exception as e:
                        # Không để lỗi hub chặn luồng; fallback sẽ xử lý tiếp
                        logger.exception(f"[{media_source.name}] Hub crawl error: {e}")

                # Use cache if true
                # if self.config.use_cache:
                #     cache_key = self.cache_manager.make_cache_key(
                #         media_source.name,
                #         industry_name,
                #         keywords,
                #         start_date,
                #         end_date,
                #     )
                #     cached_data = self.cache_manager.load_cache(cache_key)

                #     if cached_data:
                #         logger.info(f"[{media_source.name}] ✅ Loaded from cache.")
                #         return CrawlResult(**cached_data)

                # max_keywords_per_query = 3
                # keyword_groups = [
                #     keywords[i : i + max_keywords_per_query]
                #     for i in range(0, len(keywords), max_keywords_per_query)
                # ]

                if not found_for_this_keyword:
                    # Try dedicated CSEArticleAgent first for this keyword group
                    try:
                        cse_result = await self.cse_agent.run(
                            media_source, group, start_date, end_date, industry_name
                        )
                        if cse_result and cse_result.articles_found:
                            self.current_articles = list(cse_result.articles_found)
                            articles.extend(cse_result.articles_found)
                            found_for_this_keyword = True

                            if self.on_progress_update:
                                self.on_progress_update(
                                    source_name=media_source.name,
                                    completed=self.status.completed_sources,
                                    failed=self.status.failed_sources,
                                    progress=(
                                        (
                                            self.status.completed_sources
                                            + self.status.failed_sources
                                        )
                                        / max(1, self.status.total_sources)
                                    )
                                    * 100.0,
                                    current_task=(
                                        f"Đã tìm {len(self.current_articles)} bài từ CSE → chuyển keyword tiếp theo"
                                    ),
                                )

                            # go next keyword
                            continue
                    except Exception as e:
                        logger.warning(
                            f"[{media_source.name}] CSEArticleAgent error: {e}",
                            exc_info=True,
                        )

                if not found_for_this_keyword:
                    logger.info(
                        f"[{media_source}] ⏭️ Bỏ qua fallback search_tools, chuyển keyword kế tiếp"
                    )
                    continue
                    for i in range(tool_index0, len(tools_to_try)):
                        await _maybe_await(self.check_pause_or_cancel)
                        tool_index = tools_to_try[i]
                        tool = self.search_tools[tool_index]
                        tool_name = getattr(tool, "__name__", tool.__class__.__name__)

                        self.search_tool_index = tool_index
                        self._create_agent()

                        new_articles_this_tool = 0
                        self.current_articles = []

                        group_index = g_idx
                        group = keyword_groups[group_index]
                        keywords_str = ", ".join(group)

                        # Update progress
                        if self.on_progress_update:
                            flat_keywords = keywords_str
                            self.on_progress_update(
                                source_name=media_source.name,
                                completed=self.status.completed_sources,
                                failed=self.status.failed_sources,
                                progress=(
                                    (
                                        self.status.completed_sources
                                        + self.status.failed_sources
                                    )
                                    / self.status.total_sources
                                )
                                * 100.0,
                                current_task=f"Crawling {media_source.name} ({industry_name}) – từ khóa: {flat_keywords}",
                            )

                            query_variants = [
                                # f"Công ty {industry_name} {keywords_str} site:{media_source.domain} tháng {start_date.month} {start_date.year}",
                                f"{keywords_str} tin tức mới nhất tháng {start_date.month} {start_date.year} site:{media_source.domain}",
                                f"site:{media_source.domain} {industry_name} {keywords_str} tháng {start_date.month} {start_date.year}",
                            ]
                            query_lines = "\n".join([f"- {q}" for q in query_variants])

                            crawl_query = f"""
                            Crawl website: {domain_url or media_source.name}
                            Điều kiện chấp nhận bài viết:
                                - Bài báo nằm trên miền {media_source.name}, chứa từ khóa: {keywords_str} trong tiêu đề hoặc bài viết và PHẢI liên quan đến ngành hàng: {industry_name}.
                            Thời gian: {date_filter}
                            Bạn nên thử tất cả các câu truy vấn sau khi sử dụng công cụ {ddgs_search_text}: {query_lines}
                            Yêu cầu với bài viết tìm được:
                                - Trích xuất tiêu đề, tóm tắt, ngày phát hành, link gốc.
                                - Ngày phát hành (ngay_phat_hanh) phải là ngày được ghi trong nội dung bài viết.
                                - Tuyệt đối KHÔNG được lấy các ngày nằm ở phần **header**, **menu**, **sidebar**, hay **góc trên cùng của trang** (vì đó là ngày hiện tại hiển thị giao diện, KHÔNG phải ngày đăng bài viết).
                                - Ngày phát hành phải:
                                    + Xuất hiện bên trong nội dung bài viết.
                                    + Có thể nằm dưới tiêu đề, gần tên tác giả, hoặc cuối bài viết.
                                    + Nếu có nhiều ngày trong nội dung, bạn PHẢI chọn ngày:
                                        - Có định dạng hợp lệ (ví dụ: 31/07/2025, 2025-07-31, hoặc có thêm giờ)
                                        - Khác với ngày đầu trang
                                        - Là ngày nhỏ hơn (sớm hơn)
                                - Các ví dụ:
                                    ✅ Đúng: "31/07/2025 17:25" nằm gần tác giả hoặc cuối bài viết.
                                    ❌ Sai: "Thứ Ba, ngày 05/08/2025" nằm ở đầu trang, trong header.
                                - Chỉ lấy bài viết liên quan đến ngành hàng và từ khóa.
                                - QUY TẮC NO-RESULT:
                                    + Nếu danh sách bài === rỗng → trả về JSON mảng rỗng: []
                                    + Nếu bài nào có "text" < 500 ký tự → bỏ bài đó (không tóm tắt)
                                    + Tuyệt đối KHÔNG tạo bài khi không có dữ liệu.
                                - Tạo tóm tắt chi tiết (dưới 100 từ), nêu bật các thông tin chính như: sự kiện chính, các bên liên quan, và kết quả hoặc tác động của sự kiện. Không chỉ lặp lại tiêu đề.
                                - Format: Tiêu đề | Ngày phát hành | Tóm tắt | Link
                            """
                            logger.info(
                                f"[{media_source.name}] Using {tool_name} for group {group_index + 1}/{len(keyword_groups)}"
                            )

                            try:
                                logger.info(
                                    f"[{media_source.name}] Searching with keywords: {keywords_str}"
                                )
                                response = await self._arun_with_user_fallback(
                                    crawl_query,
                                    session_id=self.session_id,
                                    preferred_provider=getattr(
                                        self.config, "provider", None
                                    ),
                                    preferred_model=getattr(self.config, "model", None),
                                )
                                logger.info(
                                    f"[DEBUG] Raw response content:\n{response.content}"
                                )
                                await asyncio.sleep(1)

                                if response and response.content:
                                    if (
                                        "enable javascript" in response.content.lower()
                                        or "captcha" in response.content.lower()
                                    ):
                                        logger.warning(
                                            f"[{media_source.name}] Blocked or captcha required for {tool_name}"
                                        )
                                        continue

                                    parsed_articles = self.parser.parse(
                                        response.content, media_source, industry_name
                                    )

                                    filtered_articles = [
                                        a
                                        for a in parsed_articles
                                        if not any(
                                            exclude in a.link_bai_bao
                                            for exclude in self.config.exclude_domains
                                        )
                                    ]

                                    valid_new_articles = []
                                    seen_links = set()
                                    for a in filtered_articles:
                                        if (
                                            not a
                                            or not a.link_bai_bao
                                            or not a.ngay_phat_hanh
                                        ):
                                            continue

                                        # Kiểm tra ngày
                                        pub_date = to_date(a.ngay_phat_hanh)
                                        if not (
                                            to_date(start_date)
                                            <= pub_date
                                            <= to_date(end_date)
                                        ):
                                            continue

                                        # Kiểm tra trùng link (sau khi chắc chắn là bài hợp lệ)
                                        if a.link_bai_bao in seen_links:
                                            continue
                                        norm = await validate_and_normalize_link(
                                            a.link_bai_bao, media_source.domain
                                        )
                                        if not norm:
                                            # log lý do loại nếu muốn
                                            continue

                                        if norm in seen_links:
                                            continue
                                        a.link_bai_bao = norm
                                        seen_links.add(a.link_bai_bao)

                                        valid_new_articles.append(a)

                                    if valid_new_articles:
                                        articles.extend(valid_new_articles)
                                        self.current_articles.extend(valid_new_articles)
                                        new_articles_this_tool += len(
                                            valid_new_articles
                                        )

                                        logger.info(
                                            f"[{media_source.name}] Found {len(valid_new_articles)} articles with {tool_name} for keywords: {keywords_str}"
                                        )
                                        logger.debug(
                                            f"[{media_source.name}] Total articles so far: {len(articles)}"
                                        )
                                    else:
                                        logger.warning(
                                            f"[{media_source.name}] No articles parsed from {tool_name} for keywords: {keywords_str}"
                                        )
                                else:
                                    logger.warning(
                                        f"[{media_source.name}] No response from {tool_name} for keywords: {keywords_str}"
                                    )

                            except (
                                httpx.HTTPStatusError,
                                httpx.RequestError,
                                APITimeoutError,
                                ValueError,
                            ) as e:
                                logger.warning(
                                    f"[{media_source.name}] Error in {tool_name} for keywords {keywords_str}: {e}"
                                )
                                await asyncio.sleep(1)
                            except Exception as e:
                                logger.error(
                                    f"[{media_source.name}] Unexpected error in {tool_name} for keywords {keywords_str}: {e}",
                                    exc_info=True,
                                )
                                await asyncio.sleep(1)
                            gc.collect()

                        if new_articles_this_tool > 0:
                            logger.info(
                                f"[{media_source.name}] Stopping search as articles were found by {tool_name}"
                            )
                            break

                    unique_articles = {
                        article.link_bai_bao: article
                        for article in articles
                        if article.link_bai_bao
                    }
                    articles = list(unique_articles.values())
                    logger.info(
                        f"[{media_source.name}] Crawled {len(articles)} unique articles"
                    )

            result = CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url=media_source.domain,
                articles_found=articles,
                crawl_status="success" if articles else "failed",
                error_message=(
                    ""
                    if articles
                    else f"Thử hết {len(self.search_tools)} search tool nhưng không tìm thấy bài báo"
                ),
                crawl_duration=(datetime.now() - start_time).total_seconds(),
            )
            return result

            # if (
            #     self.config.use_cache
            #     and result.crawl_status == "success"
            #     and len(result.articles_found) > 0
            # ):
            #     self.cache_manager.save_cache(cache_key, result)

        except Exception as e:
            logger.error(f"[{media_source.name}] Crawl failed: {e}", exc_info=True)
            return CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url=media_source.domain,
                articles_found=[],
                crawl_status="failed",
                error_message=f"Crawl failed: {str(e)}",
                crawl_duration=(datetime.now() - start_time).total_seconds(),
            )
        finally:
            gc.collect()

    def close_final(self):
        logger.info("🧹 Đóng hoàn toàn CrawlerAgent, giải phóng agent và tools.")
        self.agent = None
        for tool in self.search_tools:
            if hasattr(tool, "close"):
                try:
                    tool.close()
                except Exception as e:
                    logger.warning(f"Tool {tool} đóng không thành công: {e}")
        gc.collect()


class ProcessorAgent(LLMUserFallbackMixin):
    """Agent chuyên xử lý và phân tích nội dung, sử dụng prompt tiếng Việt chi tiết."""

    def __init__(
        self,
        model: Any,
        config: CrawlConfig,
        session_id: Optional[str] = None,
    ):
        self.agent = None
        self.session_id = session_id
        self.config = config
        self.model = model

    def _create_agent(self):
        self.agent = Agent(
            name="ContentProcessor",
            role="Chuyên gia Phân tích và Phân loại Nội dung",
            model=self.model,
            instructions=[
                "Bạn là chuyên gia phân tích nội dung truyền thông cho ngành FMCG tại Việt Nam.",
                "Nhiệm vụ của bạn: Phân tích và phân loại các bài báo theo ngành hàng và nhãn hiệu.",
                "Bạn BẮT BUỘC phải trả về kết quả dưới dạng một danh sách JSON (JSON list) hợp lệ của các đối tượng Article đã được xử lý đầy đủ.",
            ],
            markdown=True,
        )

    def extract_json(self, text: str) -> str:
        """
        Trích xuất JSON từ phản hồi LLM với thứ tự ưu tiên:
        1. Tìm đoạn trong ```json ... ```
        2. Nếu không có, bỏ dấu '...' ngoài cùng (nếu có)
        3. Tìm đoạn JSON {...} hoặc [...]
        """

        text = text.strip()

        # 1️. Ưu tiên tìm ```json ... ```
        matches = re.findall(r"```json(.*?)```", text, re.DOTALL)
        if matches:
            for match in matches:
                match = match.strip()
                try:
                    json.loads(match)
                    return match  # ✅ Trả về JSON đúng luôn
                except json.JSONDecodeError:
                    continue

        # 2. Nếu không có, xử lý dấu '...' ngoài cùng
        if text.startswith("'") and text.endswith("'"):
            text = text[1:-1].strip()

        # 3️. Tìm đoạn JSON {...} hoặc [...]
        candidates = re.findall(r"(\{.*?\}|\[.*?\])", text, re.DOTALL)
        for candidate in candidates:
            candidate = candidate.strip()
            try:
                json.loads(candidate)
                return candidate  # ✅ Trả về JSON đúng
            except json.JSONDecodeError:
                continue

        # Nếu không tìm thấy
        raise ValueError("Không tìm thấy JSON hợp lệ trong phản hồi.")

    async def process_articles(
        self, raw_articles: List[Article], keywords_config: Dict[str, List[str]]
    ) -> List[Article]:
        """
        Processes a list of raw articles to classify industries, brands, content clusters,
        and extract summaries and keywords. Optimizes memory usage by processing articles in batches.

        Args:
            raw_articles: List of raw Article objects to process.
            keywords_config: Dictionary mapping industries to lists of keywords.

        Returns:
            List of processed Article objects.
        """
        if not raw_articles:
            logger.info("No articles to process.")
            return []

        processed_articles = []
        batch_size = 10  # Process 10 articles per batch to reduce memory usage

        try:
            # Extract brand list (uppercase keywords) for competitor identification
            brand_list = list(
                set(
                    kw
                    for kws in keywords_config.values()
                    for kw in kws
                    if kw[0].isupper()
                )
            )

            # Define content clusters with associated keywords
            content_clusters = {
                ContentCluster.HOAT_DONG_DOANH_NGHIEP: [
                    "sản xuất",
                    "nhà máy",
                    "tuyển dụng",
                    "doanh nghiệp",
                    "hoạt động",
                    "đầu tư",
                ],
                ContentCluster.CHUONG_TRINH_CSR: [
                    "tài trợ",
                    "môi trường",
                    "cộng đồng",
                    "CSR",
                    "từ thiện",
                ],
                ContentCluster.MARKETING_CAMPAIGN: [
                    "truyền thông",
                    "KOL",
                    "khuyến mãi",
                    "quảng cáo",
                    "chiến dịch",
                ],
                ContentCluster.PRODUCT_LAUNCH: [
                    "sản phẩm mới",
                    "bao bì",
                    "công thức",
                    "ra mắt",
                    "phát hành",
                ],
                ContentCluster.PARTNERSHIP: [
                    "MOU",
                    "liên doanh",
                    "ký kết",
                    "hợp tác",
                    "đối tác",
                ],
                ContentCluster.FINANCIAL_REPORT: [
                    "lợi nhuận",
                    "doanh thu",
                    "tăng trưởng",
                    "báo cáo tài chính",
                    "kết quả kinh doanh",
                ],
                ContentCluster.FOOD_SAFETY: [
                    "an toàn thực phẩm",
                    "ATTP",
                    "ngộ độc",
                    "nhiễm khuẩn",
                    "thu hồi sản phẩm",
                    "chất cấm",
                    "kiểm tra ATTP",
                    "thanh tra an toàn thực phẩm",
                    "truy xuất nguồn gốc",
                    "blockchain thực phẩm",
                    "tem QR",
                    "chuỗi cung ứng sạch",
                    "cam kết chất lượng thực phẩm",
                    "quy định an toàn thực phẩm",
                    "xử phạt vi phạm ATTP",
                    "sức khỏe",
                    "thu hồi sản phẩm",
                ],
                ContentCluster.OTHER: [],
            }

            # Override with external config if available to keep clusters in sync
            try:
                with open(
                    CONFIG_DIR / "content_cluster_keywords.json",
                    "r",
                    encoding="utf-8-sig",
                ) as f:
                    _ext_clusters = json.load(f)

                def _norm_key(s: str) -> str:
                    try:
                        return unicodedata.normalize("NFKC", s).strip().lower()
                    except Exception:
                        return (s or "").strip().lower()

                _value_to_enum = {_norm_key(c.value): c for c in ContentCluster}
                _mapped = {}
                for _k, _kws in _ext_clusters.items():
                    _enum = _value_to_enum.get(_norm_key(_k))
                    if _enum is not None:
                        _mapped[_enum] = _kws
                    else:
                        logger.warning(f"Unrecognized content cluster in config: {_k}")

                if _mapped:
                    content_clusters = _mapped
                    logger.debug(f"Has load content cluster in config")
            except Exception:
                logger.warning(
                    "Failed to load content_cluster_keywords.json; using built-in cluster keywords",
                    exc_info=True,
                )

            # Process articles in batches
            for i in range(0, len(raw_articles), batch_size):
                batch = raw_articles[i : i + batch_size]
                logger.info(
                    f"Processing batch {i // batch_size + 1} with {len(batch)} articles"
                )

                # Create analysis prompt for the batch
                analysis_prompt = f"""
                Phân tích và phân loại {len(batch)} bài báo sau đây.

                Danh sách các nhãn hàng đối thủ cần xác định:
                {json.dumps(brand_list, ensure_ascii=False, indent=2)}
                
                Keywords config:
                {json.dumps(keywords_config, ensure_ascii=False, indent=2)}
                
                Raw articles:
                {json.dumps([a.model_dump(mode='json') for a in batch], ensure_ascii=False, indent=2)}
                
                Yêu cầu:
                1. Phân loại chính xác ngành hàng cho từng bài (dựa theo bối cảnh bài và danh sách nhãn hàng ngành hàng tương ứng).
                2. Trích xuất `nhan_hang`:
                    - Nếu đã `nhan_hang` đã có nội dung, giữ nguyên. Nếu là `[]` hoặc trống thì tiến hành trích xuất theo quy trình sau:
                        + Đọc nội dung bài viết và kiểm tra xem có nhãn hàng nào trong danh sách sau xuất hiện hay không: 
                        {json.dumps(brand_list, ensure_ascii=False, indent=2)}
                        + Chỉ ghi nhận những nhãn hàng thực sự xuất hiện trong bài viết (bất kể viết hoa hay viết thường).
                        + Nếu không thấy nhãn hàng nào thì để `nhan_hang` là `[]`. Không tự bịa hoặc tự suy đoán thêm.
                3. Phân loại lại cụm nội dung (`cum_noi_dung`). Nếu bài viết có nội dung tương đương, đồng nghĩa hoặc gần giống với các cụm từ khóa: {json.dumps({k.value: v for k, v in content_clusters.items()}, ensure_ascii=False, indent=2)}, hãy phân loại vào cụm đó.
                4. Nếu không tìm thấy cụm nội dung nào khớp với danh sách từ khóa cụm nội dung, BẮT BUỘC gán trường (`cum_noi_dung`) là '{ContentCluster.OTHER.value}', KHÔNG ĐƯỢC để trống hoặc trả về none hay null.
                5. `cum_noi_dung_chi_tiet` là phần mô tả ngắn gọn (~10–20 từ) và mang tính khái quát thông tin chính mà bài báo muốn truyền đạt.. Quy trình tạo `cum_noi_dung_chi_tiet`:
                    - [Loại thông tin]: [Tóm tắt nội dung nổi bật]
                    - [Loại thông tin] sẽ được gán bởi nội dung của trường (`cum_noi_dung`).
                    **Ví dụ:**
                        + `cum_noi_dung`: "Hoạt động doanh nghiệp và thông tin sản phẩm"  
                        `cum_noi_dung_chi_tiet`: "Hoạt động doanh nghiệp và thông tin sản phẩm: Tường An khẳng định vị thế dịp Tết 2025"

                        + `cum_noi_dung`: "Marketing và chiến lược"  
                        `cum_noi_dung_chi_tiet`: "Marketing và chiến lược: Chiến lược Tết 2025 của Vinamilk"
                6. Trích xuất và ghi vào `keywords_found`:
                    - Là tất cả các từ khóa ngành liên quan thực sự xuất hiện trong bài viết.
                    - Chỉ được trích xuất từ các từ khóa đã cung cấp trong `keywords_config`.
                    - Nếu không tìm thấy từ khóa nào, để `keywords_found` là []
                7. Chỉ giữ bài viết liên quan đến ngành FMCG (Dầu ăn, Gia vị, Sữa, v.v.) dựa trên từ khóa trong `keywords_config`. Loại bỏ bài không liên quan (e.g., chính trị, sức khỏe không liên quan).
                8. Định dạng ngày phát hành bắt buộc: dd/mm/yyyy (VD: 01/07/2025)"
                9. Nếu một bài báo đề cập nhiều nhãn hàng thì ghi tất cả nhãn hàng trong danh sách `nhan_hang`.
                10. Nếu bài liên quan nhiều ngành (ví dụ sản phẩm đa dụng), hãy chọn ngành chính nhất liên quan đến bối cảnh.
                11. Giữ nguyên `tom_tat_noi_dung`, không cắt bớt, sinh ra hay thay đổi nội dung.
                Định dạng đầu ra:
                Trả về một danh sách JSON hợp lệ chứa các đối tượng Article đã được xử lý. Cấu trúc JSON của mỗi đối tượng phải khớp với Pydantic model. Đây là 1 ví dụ cho bạn làm mẫu:
                [
                    {{
                        "stt": 1,
                        "ngay_phat_hanh": "01/07/2025",
                        "dau_bao": "VNEXPRESS",
                        "cum_noi_dung": "Chiến dịch Marketing",
                        "cum_noi_dung_chi_tiet": "Chiến dịch Tết 2025 của Vinamilk chinh phục người tiêu dùng trẻ",
                        "tom_tat_noi_dung": "Vinamilk tung chiến dịch Tết 2025...",
                        "link_bai_bao": "https://vnexpress.net/...",
                        "nganh_hang": "Sữa (UHT)",
                        "nhan_hang": ["Vinamilk"],
                        "keywords_found": ["Tết", "TV quảng cáo", "Vinamilk"]
                    }}
                ]
                [
                    {{
                        "stt": 2,
                        "ngay_phat_hanh": "06/07/2025",
                        "dau_bao": "Thanh Nien",
                        "cum_noi_dung": "Hoạt động doanh nghiệp và thông tin sản phẩm",
                        "cum_noi_dung_chi_tiet": "Thông tin doanh nghiệp: Tường An và Coba mở rộng thị phần dầu ăn miền Tây",
                        "tom_tat_noi_dung": "Tường An và Coba tăng cường đầu tư và phân phối sản phẩm dầu ăn tại khu vực miền Tây Nam Bộ.",
                        "link_bai_bao": "https://thanhnien.vn/tuong-an-coba-dau-an",
                        "nganh_hang": "Dầu ăn",
                        "nhan_hang": ["Tường An", "Coba"],
                        "keywords_found": ["Tường An", "Coba", "dầu ăn", "thị phần"]
                    }}
                ]
                [
                    {{
                        "stt": 3,
                        "ngay_phat_hanh": "07/07/2025",
                        "dau_bao": "Tuoi Tre",
                        "cum_noi_dung": "Chương trình CSR",
                        "cum_noi_dung_chi_tiet": "CSR: Doanh nghiệp địa phương hỗ trợ cộng đồng miền núi",
                        "tom_tat_noi_dung": "Một số doanh nghiệp địa phương phối hợp tổ chức chương trình hỗ trợ bà con miền núi mùa mưa lũ.",
                        "link_bai_bao": "https://tuoitre.vn/csr-mien-nui",
                        "nganh_hang": "Dầu ăn",
                        "nhan_hang": [],
                        "keywords_found": ["hỗ trợ cộng đồng", "CSR", "miền núi"]
                    }}
                ]
                """

                try:
                    response = await self._arun_with_user_fallback(
                        analysis_prompt,
                        session_id=self.session_id,
                        preferred_provider=getattr(self.config, "provider", None),
                        preferred_model=getattr(self.config, "model", None),
                    )
                    logger.debug(f"LLM raw output:\n{response.content}")

                    if response and response.content:
                        try:
                            raw_json = self.extract_json(response.content)
                            articles_data = json.loads(raw_json)

                            km = KeywordManager(
                                CONFIG_DIR / "content_cluster_keywords.json"
                            )
                            valid_clusters = [c.value for c in ContentCluster]

                            for item in articles_data:
                                text_to_check = (
                                    item.get("tom_tat_noi_dung", "")
                                    + " "
                                    + (item.get("cum_noi_dung_chi_tiet", "") or "")
                                ).lower()

                                # Fallback cụm nội dung nếu cần
                                if (
                                    item.get("cum_noi_dung")
                                    in [None, "null", "", "Khác"]
                                    or item.get("cum_noi_dung") not in valid_clusters
                                ):
                                    text_to_check = (
                                        item.get("tom_tat_noi_dung", "")
                                        + " "
                                        + (item.get("cum_noi_dung_chi_tiet", "") or "")
                                    ).strip()
                                    fallback_cluster = km.map_to_cluster(text_to_check)
                                    item["cum_noi_dung"] = fallback_cluster

                                    # Cập nhật cum_noi_dung_chi_tiet để đồng bộ với cum_noi_dung mới
                                    # Tìm phần trước dấu ":" và thay nó bằng fallback_cluster
                                    if item.get("cum_noi_dung_chi_tiet"):
                                        cum_noi_dung_chi_tiet = item[
                                            "cum_noi_dung_chi_tiet"
                                        ]
                                        # Kiểm tra nếu có dấu ":" trong cum_noi_dung_chi_tiet
                                        if ":" in cum_noi_dung_chi_tiet:
                                            # Cập nhật phần đầu trước dấu ":"
                                            item["cum_noi_dung_chi_tiet"] = (
                                                f"{fallback_cluster}: {cum_noi_dung_chi_tiet.split(':', 1)[-1]}"
                                            )
                                        else:
                                            # Nếu không có dấu ":", chỉ cần thêm vào phần sau
                                            item["cum_noi_dung_chi_tiet"] = (
                                                f"{fallback_cluster}: {cum_noi_dung_chi_tiet}"
                                            )

                                # Fallback keywords_found
                                if (
                                    "keywords_found" not in item
                                    or not item["keywords_found"]
                                ):
                                    item["keywords_found"] = []
                                    for industry, keywords in keywords_config.items():
                                        for kw in keywords:
                                            if kw.lower() in text_to_check:
                                                item["keywords_found"].append(kw)
                                    # Loại trùng
                                    item["keywords_found"] = list(
                                        set(item["keywords_found"])
                                    )

                                # Fallback nhãn hàng
                                if "nhan_hang" not in item or not item["nhan_hang"]:
                                    item["nhan_hang"] = []
                                    for brand in brand_list:
                                        if brand.lower() in text_to_check:
                                            item["nhan_hang"].append(brand)
                                    # Loại trùng
                                    item["nhan_hang"] = list(set(item["nhan_hang"]))

                            # Build quick lookups from original batch to restore required fields
                            try:
                                _orig_by_stt = {a.stt: a for a in batch}
                            except Exception:
                                _orig_by_stt = {}
                            _orig_by_link = {}
                            try:
                                for a in batch:
                                    try:
                                        _k = (a.link_bai_bao or "").strip().lower()
                                        if _k:
                                            _orig_by_link[_k] = a
                                    except Exception:
                                        continue
                            except Exception:
                                pass

                            # Ensure required field 'tom_tat_noi_dung' exists; recover from original batch when missing
                            for item in articles_data:
                                need_restore = (
                                    "tom_tat_noi_dung" not in item
                                    or not isinstance(item.get("tom_tat_noi_dung"), str)
                                    or not item.get("tom_tat_noi_dung", "").strip()
                                )
                                if need_restore:
                                    orig = None
                                    stt_val = item.get("stt")
                                    if stt_val in _orig_by_stt:
                                        orig = _orig_by_stt.get(stt_val)
                                    else:
                                        link_val = item.get("link_bai_bao")
                                        if isinstance(link_val, str):
                                            orig = _orig_by_link.get(
                                                link_val.strip().lower()
                                            )
                                    if orig and getattr(orig, "tom_tat_noi_dung", None):
                                        item["tom_tat_noi_dung"] = orig.tom_tat_noi_dung
                                    else:
                                        # As a last resort, set to empty string to satisfy schema
                                        item["tom_tat_noi_dung"] = ""

                            # Safely instantiate Article models; skip invalid items with a warning
                            _safe_models = []
                            for item in articles_data:
                                try:
                                    _safe_models.append(Article(**item))
                                except Exception as _e:
                                    logger.warning(
                                        f"Skipping invalid article (stt={item.get('stt')} link={item.get('link_bai_bao')}): {_e}"
                                    )

                            processed_articles.extend(_safe_models)
                            logger.info(
                                f"Batch {i // batch_size + 1} processed: {len(articles_data)} articles"
                            )

                        except (json.JSONDecodeError, TypeError) as e:
                            logger.error(
                                f"Failed to parse JSON response for batch {i // batch_size + 1}: {e}"
                            )
                            # Fallback: gán cụm nội dung bằng KeywordManager khi lỗi
                            km = KeywordManager(
                                CONFIG_DIR / "content_cluster_keywords.json"
                            )
                            for article in batch:
                                text = (
                                    article.tom_tat_noi_dung
                                    + " "
                                    + (article.cum_noi_dung_chi_tiet or "")
                                )
                                article.cum_noi_dung = km.map_to_cluster(text)
                            processed_articles.extend(batch)

                    else:
                        logger.warning(
                            f"No valid response for batch {i // batch_size + 1}"
                        )
                        # Fallback tương tự khi không có response
                        km = KeywordManager(
                            CONFIG_DIR / "content_cluster_keywords.json"
                        )
                        for article in batch:
                            text = (
                                article.tom_tat_noi_dung
                                + " "
                                + (article.cum_noi_dung_chi_tiet or "")
                            )
                            article.cum_noi_dung = km.map_to_cluster(text)
                        processed_articles.extend(batch)

                except Exception as e:
                    logger.error(
                        f"Error processing batch {i // batch_size + 1}: {e}",
                        exc_info=True,
                    )
                    processed_articles.extend(
                        batch
                    )  # Keep raw articles if processing fails

                # Free memory after each batch
                gc.collect()

            logger.info(
                f"Processing completed. Total processed articles: {len(processed_articles)}"
            )
            return processed_articles

        except Exception as e:
            logger.error(f"Article processing failed: {e}", exc_info=True)
            return raw_articles  # Return raw articles if the entire process fails
        finally:
            gc.collect()  # Final memory cleanup

    def close(self):
        del self.agent
        gc.collect()


class ReportAgent(LLMUserFallbackMixin):
    """Agent chuyên tạo báo cáo, sử dụng prompt tiếng Việt."""

    def __init__(
        self, model: Any, config: CrawlConfig, session_id: Optional[str] = None
    ):
        self.agent = None
        self.model = model
        self.session_id = session_id
        self.config = config

    def _create_agent(self):
        schema = CompetitorReport.model_json_schema()
        schema_text = json.dumps(schema, ensure_ascii=False)
        self.agent = Agent(
            name="ReportGenerator",
            role="Chuyên gia Tạo Báo cáo và Phân tích Dữ liệu",
            model=self.model,
            instructions=[
                "Bạn là chuyên gia tạo báo cáo phân tích truyền thông cho ngành FMCG.",
                "Nhiệm vụ: Tạo một báo cáo phân tích đối thủ cạnh tranh từ dữ liệu các bài báo đã được cung cấp.",
                "Bạn BẮT BUỘC phải trả về kết quả dưới dạng một đối tượng JSON (JSON object) duy nhất, hợp lệ và tuân thủ nghiêm ngặt theo cấu trúc của JSON Schema bên dưới",
                "Nếu không có dữ liệu hoặc có lỗi, trả về CompetitorReport rỗng hợp lệ với các trường là [] hoặc 0.",
                f"JSON_SCHEMA:\n{schema_text}\n",
            ],
            markdown=False,
        )

    def extract_json(self, text: str) -> str:
        text = text.strip()

        # Ưu tiên tìm đoạn ```json ... ```
        matches = re.findall(r"```json(.*?)```", text, re.DOTALL)
        if matches:
            for match in matches:
                match = match.strip()
                try:
                    obj = json.loads(match)
                    if isinstance(obj, dict):
                        return match
                except:
                    continue

        # Nếu không có, tìm JSON object ngoài cùng
        try:
            # Thử parse toàn bộ text luôn nếu là dict
            obj = json.loads(text)
            if isinstance(obj, dict):
                return text
        except:
            pass

        # Fallback tìm các cặp { } lớn nhất
        dict_candidates = re.findall(r"(\{.*\})", text, re.DOTALL)
        for candidate in dict_candidates:
            try:
                obj = json.loads(candidate.strip())
                if isinstance(obj, dict):
                    return candidate.strip()
            except:
                continue

        raise ValueError("Không tìm thấy JSON dict hợp lệ trong phản hồi.")

    def _sanitize_report_payload(self, d: dict, articles, date_range: str) -> dict:
        d = dict(d or {})

        def _normalize_industry_name(value: Any) -> str:
            if value is None:
                return ""
            if hasattr(value, "value"):
                value = value.value
            return str(value).strip().lower()

        def _extract_article_field(article, attr, default=None):
            if isinstance(article, dict):
                return article.get(attr, default)
            return getattr(article, attr, default)

        def _article_industry(article):
            for key in ("nganh_hang", "nganh", "industry"):
                value = _extract_article_field(article, key)
                if value:
                    return value
            return None

        def _article_brands(article) -> List[str]:
            brands = _extract_article_field(article, "nhan_hang")
            if brands is None:
                brands = _extract_article_field(article, "brands")
            if brands is None:
                return []
            if isinstance(brands, list):
                return [b for b in brands if b]
            if isinstance(brands, str) and brands.strip():
                return [brands.strip()]
            return []

        industries_with_brandless = {
            _normalize_industry_name(_article_industry(article))
            for article in (articles or [])
            if article is not None and not _article_brands(article)
        }

        def _ensure_brand_placeholder(target: dict):
            key = _normalize_industry_name(target.get("nganh_hang"))
            if key and key in industries_with_brandless:
                brands = target.setdefault("nhan_hang", [])
                if not isinstance(brands, list):
                    brands = [brands]
                if "Không có" not in brands:
                    brands.append("Không có")
                target["nhan_hang"] = brands

        # ---- overall_summary ----
        osum = dict(d.get("overall_summary") or {})
        industries = osum.get("industries")

        # 1) industries phải là list
        if isinstance(industries, dict):
            industries = [industries]
        elif not isinstance(industries, list):
            industries = []

        fixed_industries = []
        for it in industries:
            if not isinstance(it, dict):
                continue
            it = dict(it)

            # 2) Chuẩn hóa/đổi tên khóa về đúng schema
            if "nganh_hang" not in it:
                it["nganh_hang"] = it.pop(
                    "industry", it.pop("nganh", it.pop("sector", "Khác"))
                )
            if "nhan_hang" not in it:
                it["nhan_hang"] = it.pop("brands", it.pop("brand", []))
            if "cum_noi_dung" not in it:
                it["cum_noi_dung"] = it.pop("clusters", it.pop("topics", []))
            if "so_luong_bai" not in it:
                it["so_luong_bai"] = it.pop("count", it.pop("total", 0))
            if "cac_dau_bao" not in it:
                it["cac_dau_bao"] = it.pop("sources", [])

            # 3) Đảm bảo kiểu dữ liệu tối thiểu
            if not isinstance(it["nhan_hang"], list):
                it["nhan_hang"] = [it["nhan_hang"]]
            if not isinstance(it["cum_noi_dung"], list):
                it["cum_noi_dung"] = [it["cum_noi_dung"]]
            if not isinstance(it["cac_dau_bao"], list):
                it["cac_dau_bao"] = [it["cac_dau_bao"]]
            if not isinstance(it["so_luong_bai"], int):
                try:
                    it["so_luong_bai"] = int(it["so_luong_bai"])
                except Exception:
                    it["so_luong_bai"] = 0

            _ensure_brand_placeholder(it)
            fixed_industries.append(it)

        osum["industries"] = fixed_industries
        osum.setdefault("thoi_gian_trich_xuat", date_range)
        osum.setdefault("tong_so_bai", len(articles))
        d["overall_summary"] = osum

        # ---- industry_summaries ----
        iss = d.get("industry_summaries")
        if not isinstance(iss, list) or not all(isinstance(x, dict) for x in iss):
            # nếu LLM không trả, dùng lại danh sách đã fix từ overall_summary
            iss = fixed_industries
        else:
            fixed_iss = []
            for it in iss:
                it = dict(it)
                it.setdefault("nganh_hang", osum.get("nganh_hang", "Khác"))
                it.setdefault("nhan_hang", [])
                it.setdefault("cum_noi_dung", [])
                it.setdefault("so_luong_bai", 0)
                it.setdefault("cac_dau_bao", [])
                if not isinstance(it["nhan_hang"], list):
                    it["nhan_hang"] = [it["nhan_hang"]]
                _ensure_brand_placeholder(it)
                fixed_iss.append(it)
            iss = fixed_iss
        d["industry_summaries"] = iss

        # ---- các trường gốc khác ----
        d.setdefault("total_articles", len(articles))
        d.setdefault("date_range", date_range)

        return d

    async def generate_report(
        self, articles: List[Article], date_range: str
    ) -> CompetitorReport:
        """
        Generates a competitor analysis report from a list of articles for a given date range.
        """

        if not articles:
            logger.info(
                "No articles provided for report generation. Returning basic report."
            )
            return self._create_basic_report([], date_range)

        try:
            logger.info(f"Generating full report for {len(articles)} articles...")

            report_prompt = f"""
            Tạo một báo cáo phân tích đối thủ cạnh tranh từ {len(articles)} bài báo sau đây cho khoảng thời gian: {date_range}.
            
            Dữ liệu đầu vào:
            - Input: Một danh sách các bài báo đã được xử lý đầy đủ, không cần sửa đổi gì thêm: {json.dumps([a.model_dump(mode='json') for a in articles], ensure_ascii=False, indent=2)}

            Yêu cầu nhiệm vụ:
            1. Dùng danh sách articles trên để tạo `overall_summary` và `industry_summaries`.
            2. Tạo 'overall_summary' (tóm tắt tổng quan), bao gồm: thoi_gian_trich_xuat, industries (nganh_hang, nhan_hang, cum_noi_dung, so_luong_bai, cac_dau_bao), cac_dau_bao và tong_so_bai.
            3. Tạo danh sách 'industry_summaries' (tóm tắt theo ngành), mỗi ngành là 1 mục (nganh_hang), bao gồm: nhan_hang, cum_noi_dung (trường cum_noi_dung sẽ là bao gồm hết tất cả các cụm nội dung của tất cả các bài trong cùng 1 ngành), cac_dau_bao, so_luong_bai.
            4. Quy tắc khi tạo trường `cum_noi_dung` trong `industry_summaries`:
                - `cum_noi_dung` chỉ được chọn trong danh sách sau (không thêm mô tả chi tiết):
                - "Hoạt động doanh nghiệp và thông tin sản phẩm"
                - "Chương trình CSR"
                - "Chiến dịch Marketing"
                - "Ra mắt sản phẩm"
                - "Hợp tác đối tác"
                - "Báo cáo tài chính"
                - "An toàn thực phẩm"
                - "Khác"
            5. Nếu cần mô tả chi tiết, hãy ghi vào `cum_noi_dung_chi_tiet`, không được ghi vào `cum_noi_dung`.
            6. Đảm bảo trả về đúng 1 đối tượng JSON duy nhất, không có markdown, không có giải thích ngoài lề.

            Trả về đúng một đối tượng JSON duy nhất với cấu trúc sau:
            {{
                overall_summary: { ... },
                industry_summaries: [ ... ],
                total_articles={len(articles)},
                date_range={date_range}
            }}
            
            Quy tắc bắt buộc:
            - Bắt đầu output bằng '{' và kết thúc bằng '}' duy nhất.
            """
            response = await self._arun_with_user_fallback(
                report_prompt,
                session_id=self.session_id,
                preferred_provider=getattr(self.config, "provider", None),
                preferred_model=getattr(self.config, "model", None),
            )
            logger.debug(f"Raw LLM response: {response.content}")
            if response and response.content:
                try:
                    try:
                        # Ưu tiên parse bằng json.loads
                        summary_data = json.loads(response.content)
                    except json.JSONDecodeError:
                        try:
                            # Nếu LLM trả về dạng {'key': 'value'}, dùng ast.literal_eval
                            summary_data = ast.literal_eval(response.content)
                        except Exception:
                            # Fallback dùng extract_json để tìm đúng đoạn JSON
                            raw_json = self.extract_json(response.content)
                            summary_data = json.loads(raw_json)

                    # Check có phải dict không
                    if not isinstance(summary_data, dict):
                        logger.error("LLM trả về list hoặc sai schema. Fallback.")
                        return self._create_basic_report(articles, date_range)

                    # Gộp lại articles từ input, không cho LLM sinh
                    summary_data["articles"] = [
                        a.model_dump(mode="json") for a in articles
                    ]
                    summary_data = self._sanitize_report_payload(
                        summary_data, articles, date_range
                    )

                    # Truyền vào CompetitorReport
                    return CompetitorReport(**summary_data)

                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(
                        f"Không thể phân tích phản hồi từ ReportAgent dưới dạng JSON: {e}. Đang tạo báo cáo cơ bản."
                    )
                    return self._create_basic_report(articles, date_range)
            return self._create_basic_report(articles, date_range)
        except Exception as e:
            logger.error(f"Tạo báo cáo thất bại: {e}", exc_info=True)
            return self._create_basic_report(articles, date_range)
        finally:
            gc.collect()

    def _create_basic_report(
        self, articles: List[Article], date_range: str
    ) -> CompetitorReport:
        industry_groups = {}
        for article in articles:
            industry = article.nganh_hang
            if industry not in industry_groups:
                industry_groups[industry] = []
            industry_groups[industry].append(article)

        industry_summaries = []
        for industry, industry_articles in industry_groups.items():
            brand_list: List[str] = []
            for article in industry_articles:
                article_brands = [b for b in (article.nhan_hang or []) if b]
                if article_brands:
                    for brand in article_brands:
                        if brand not in brand_list:
                            brand_list.append(brand)
                else:
                    if "Không có" not in brand_list:
                        brand_list.append("Không có")

            clusters = []
            for article in industry_articles:
                cluster = article.cum_noi_dung
                if cluster is not None and cluster not in clusters:
                    clusters.append(cluster)
            if not clusters:
                clusters = [ContentCluster.OTHER.value]

            dau_bao_list = list(
                dict.fromkeys(article.dau_bao for article in industry_articles)
            )

            industry_summaries.append(
                IndustrySummary(
                    nganh_hang=industry,
                    nhan_hang=brand_list,
                    cum_noi_dung=clusters,
                    so_luong_bai=len(industry_articles),
                    cac_dau_bao=dau_bao_list,
                )
            )

        overall_summary = OverallSummary(
            thoi_gian_trich_xuat=date_range,
            industries=industry_summaries,
            tong_so_bai=len(articles),
        )

        return CompetitorReport(
            overall_summary=overall_summary,
            industry_summaries=industry_summaries,
            articles=articles,
            total_articles=len(articles),
            date_range=date_range,
        )

    def close(self):
        del self.agent
        gc.collect()


class MediaTrackerTeam:
    """Đội điều phối chính cho toàn bộ quy trình."""

    def __init__(
        self,
        config: CrawlConfig,
        start_date: datetime,
        end_date: datetime,
        user_email: Optional[str] = None,
        session_id: Optional[str] = None,
        on_progress_update: Optional[callable] = None,
        check_cancelled: Optional[callable] = None,
        check_paused: Optional[callable] = None,
        articles_so_far=None,
        source_status_list=None,
        provider: Optional[str] = None,
        model: Optional[str] = None,
    ):
        model_runtime = get_llm_model(provider, model)
        self.config = config

        def _source_key_of(s):
            return getattr(s, "reference_name", None) or f"{s.type}|{s.domain}"

        sel = set(getattr(self.config, "selected_sources", []) or [])
        if sel:
            self.config.media_sources = [
                s for s in self.config.media_sources if _source_key_of(s) in sel
            ]

        self.session_id = session_id
        self.user_email = user_email
        self.status = BotStatus()
        self.check_cancelled = check_cancelled or (lambda: False)
        self.check_paused = check_paused or (lambda: False)
        self.articles_so_far = articles_so_far or []
        self.on_progress_update = on_progress_update
        parser = ArticleParser()
        self.crawler = CrawlerAgent(
            model_runtime,
            config,
            parser,
            session_id,
            check_cancelled,
            check_paused,
            check_pause_or_cancel=self.check_pause_or_cancel,
            user_email=user_email,
            status=self.status,
            on_progress_update=self.on_progress_update,
        )
        # self.processor = ProcessorAgent(get_llm_model("openai", "gpt-4o"))
        # self.reporter = ReportAgent(get_llm_model("openai", "gpt-4o"))
        self.processor = ProcessorAgent(model_runtime, config, session_id)
        self.reporter = ReportAgent(model_runtime, config, session_id)
        self.start_date = start_date
        self.end_date = end_date
        self.source_status_list = source_status_list or []
        self.config.use_hub_page = True

    @staticmethod
    def _norm_url(u: str) -> str:
        try:
            p = urlparse(u or "")
            path = (p.path or "").rstrip("/")
            qs = [
                (k, v)
                for k, v in parse_qsl(p.query or "", keep_blank_values=True)
                if not k.lower().startswith(("utm_", "fbclid", "gclid"))
            ]
            return urlunparse(
                (
                    p.scheme or "https",
                    (p.netloc or "").lower().lstrip("www."),
                    path,
                    "",
                    urlencode(qs, doseq=True),
                    "",
                )
            )
        except Exception:
            return u or ""

    def _merge_articles(self, new_articles):
        acc = (self.articles_so_far or []) + (new_articles or [])
        seen, uniq = set(), []
        for a in acc:
            url = (
                getattr(a, "link_bai_bao", None)
                or getattr(a, "url", None)
                or getattr(a, "link", None)
                or ""
            )
            key = self._norm_url(url)
            if key and key not in seen:
                uniq.append(a)
                seen.add(key)
        self.articles_so_far = uniq

    async def check_pause_or_cancel(self):
        if self.check_cancelled():
            logger.info("⛔ Cancelled. Stop pipeline.")
            raise asyncio.CancelledError("Pipeline is cancelled.")

        while self.check_paused():
            logger.info("⏸️  Pipeline paused. Waiting to resume...")
            await asyncio.sleep(2)

            if self.check_cancelled():
                logger.info(f"[{self.session_id}] Task was cancelled during pause.")
                raise asyncio.CancelledError("Cancelled during pause")

    async def run_full_pipeline(self) -> Optional[CompetitorReport]:
        """
        Executes the full media tracking pipeline: crawling, processing, and report generation.
        Optimizes memory usage by limiting concurrent tasks and processing articles in batches.
        """
        self.status.is_running = True
        self.status.current_task = "Initializing pipeline"
        self.status.total_sources = len(self.config.media_sources)
        self.status.progress = 0.0
        # all_articles = self.articles_so_far.copy()

        logger.info(
            f"📅 Khoảng thời gian crawl dữ liệu: từ ngày {self.start_date.strftime('%d/%m/%Y')} đến ngày {self.end_date.strftime('%d/%m/%Y')}"
        )

        try:
            # Step 1: Crawl data from media sources
            self.status.current_task = "Crawling data from media sources"
            logger.info(f"Starting crawl for {self.status.total_sources} sources.")

            # Limit concurrent crawl tasks using Semaphore
            self.semaphore = asyncio.Semaphore(self.config.max_concurrent_sources)

            async def wrapped_crawl(media_source, industry_name, keywords):
                # Nếu đã completed rồi thì skip
                if any(
                    s["source_name"] == media_source.name and s["status"] == "completed"
                    for s in self.source_status_list
                ):
                    logger.info(
                        f"[{media_source.name}] ✅ Đã hoàn thành từ trước, bỏ qua."
                    )
                    return media_source, None

                async with self.semaphore:
                    try:
                        await _maybe_await(self.check_pause_or_cancel)
                        result = await self.crawler.crawl_media_source(
                            media_source=media_source,
                            industry_name=industry_name,
                            keywords=keywords,
                            start_date=self.start_date,
                            end_date=self.end_date,
                        )

                        if result and result.articles_found:
                            self._merge_articles(result.articles_found)

                        task_manager.update_task(
                            self.user_email,
                            self.session_id,
                            {
                                "articles_so_far": [
                                    a.model_dump(mode="json")
                                    for a in self.articles_so_far
                                ],
                                "source_status_list": self.source_status_list,
                            },
                        )

                        return media_source, result

                    except asyncio.TimeoutError:
                        logger.warning(
                            f"[{media_source.name}] ⏰ Timeout sau {self.config.crawl_timeout} giây."
                        )
                        partial_result = self.crawler.return_partial_result(
                            media_source
                        )

                        if partial_result.articles_found:
                            self._merge_articles(partial_result.articles_found)

                        self.source_status_list.append(
                            {"source_name": media_source.name, "status": "failed"}
                        )

                        task_manager.update_task(
                            self.user_email,
                            self.session_id,
                            {
                                "articles_so_far": [
                                    a.model_dump(mode="json")
                                    for a in self.articles_so_far
                                ],
                                "source_status_list": self.source_status_list,
                            },
                        )

                        return media_source, partial_result

            # Prepare all keywords
            # all_keywords = list(
            #     set(kw for kws in self.config.keywords.values() for kw in kws)
            # )
            jobs = []
            tasks: list[tuple[MediaSource, asyncio.Task]] = []

            def _domain_key(source: MediaSource) -> str:
                raw = (source.domain or "").strip()
                if not raw:
                    return source.name.lower()
                normalized = raw.lower()
                if "://" not in normalized:
                    normalized = f"https://{normalized}"
                parsed = urlparse(normalized)
                candidate = parsed.netloc or parsed.path or source.name
                return (candidate or source.name).lower()

            for industry_name, keywords in self.config.keywords.items():
                for media_source in self.config.media_sources:
                    already_done = next(
                        (
                            s
                            for s in self.source_status_list
                            if s["source_name"] == media_source.name
                        ),
                        None,
                    )
                    if already_done and already_done["status"] == "completed":
                        continue

                    domain_value = _domain_key(media_source)
                    jobs.append(
                        {
                            "media_source": media_source,
                            "industry_name": industry_name,
                            "keywords": keywords,
                            "domain": domain_value,
                            "task": None,
                        },
                    )

            job_result_pairs = []
            if jobs:
                total_cap = max(1, self.config.max_concurrent_sources)
                desired_domains = max(
                    1, getattr(self.config, "max_parallel_domains", total_cap)
                )
                desired_per_domain = max(
                    1, getattr(self.config, "max_jobs_per_domain", 1)
                )
                max_domains_parallel = min(desired_domains, total_cap)
                max_per_domain = min(desired_per_domain, total_cap)
                while max_domains_parallel * max_per_domain > total_cap:
                    if max_per_domain > 1:
                        max_per_domain -= 1
                    elif max_domains_parallel > 1:
                        max_domains_parallel -= 1
                    else:
                        break

                domain_cooldown = max(
                    0.0, float(getattr(self.config, "domain_cooldown_seconds", 10.0))
                )

                async def _run_job(job):
                    media_source = job["media_source"]
                    task = asyncio.create_task(
                        wrapped_crawl(
                            media_source,
                            job["industry_name"],
                            job["keywords"],
                        )
                    )
                    job["task"] = task
                    tasks.append((media_source, task))
                    try:
                        AgentManager.get_instance().register_provider_task(
                            self.session_id,
                            f"crawl:{media_source.name}",
                            task,
                        )
                    except Exception:
                        pass
                    return await task

                domain_results = await run_in_domain_batches(
                    jobs=jobs,
                    get_domain=lambda job: job["domain"],
                    run_job=_run_job,
                    max_domains_parallel=max_domains_parallel,
                    max_per_domain=max_per_domain,
                    domain_cooldown_s=domain_cooldown,
                )

                domain_iters = {
                    dom: iter(results_list)
                    for dom, results_list in domain_results.items()
                }
                for job in jobs:
                    iterator = domain_iters.get(job["domain"])
                    if iterator is None:
                        logger.warning(
                            f"No crawl results collected for domain {job['domain']}"
                        )
                        job_result_pairs.append(
                            (
                                job,
                                RuntimeError(
                                    f"No crawl results collected for domain {job['domain']}"
                                ),
                            )
                        )
                        continue
                    try:
                        job_result_pairs.append((job, next(iterator)))
                    except StopIteration:
                        logger.warning(
                            f"Result iterator exhausted early for domain {job['domain']}"
                        )
                        job_result_pairs.append(
                            (
                                job,
                                RuntimeError(
                                    f"Result iterator exhausted early for domain {job['domain']}"
                                ),
                            )
                        )
                        continue
            else:
                logger.info("No crawl jobs pending; skipping crawling phase.")

            for job, result in job_result_pairs:
                media_source = job["media_source"]
                await _maybe_await(self.check_pause_or_cancel)

                try:
                    if isinstance(result, Exception):
                        logger.error(
                            f"[{media_source.name}] ❌ Lỗi: {result}", exc_info=True
                        )
                        self.source_status_list.append(
                            {"source_name": media_source.name, "status": "failed"}
                        )
                        continue

                    media_source_result, crawl_result = result
                    media_source = media_source_result or media_source
                    self.status.completed_sources += 1

                    crawl_status = (
                        getattr(crawl_result, "crawl_status", "failed")
                        if crawl_result
                        else "failed"
                    )
                    status = "completed" if crawl_status == "success" else "failed"

                    self.source_status_list.append(
                        {"source_name": media_source.name, "status": status}
                    )

                    completed = len(
                        [
                            s
                            for s in self.source_status_list
                            if s["status"] == "completed"
                        ]
                    )
                    failed = len(
                        [s for s in self.source_status_list if s["status"] == "failed"]
                    )

                    if self.on_progress_update:
                        self.on_progress_update(
                            source_name=media_source.name,
                            completed=completed,
                            failed=failed,
                            progress=((completed + failed) / self.status.total_sources)
                            * 100.0,
                            current_task={
                                "message_key": "task.crawling_source",
                                "params": {"source": media_source.name},
                            },
                        )

                    gc.collect()

                except asyncio.CancelledError:
                    logger.warning(f"Crawl task cancelled.")
                    for job_entry in jobs:
                        task = job_entry.get("task")
                        if task and not task.done():
                            task.cancel()
                    self.status.failed_sources += 1
                    raise
                except Exception as e:
                    self.status.failed_sources += 1
                    logger.error(
                        f"Unexpected error crawling source {media_source.name}: {e}",
                        exc_info=True,
                    )
            if not self.articles_so_far:
                logger.warning("⚠️ No articles found from crawling or cache.")
                return None

            all_articles = self.articles_so_far.copy()
            logger.info(f"Crawling completed. Found {len(all_articles)} raw articles.")

            # Step 2: Process articles in batches
            self.status.current_task = "Processing and analyzing articles"
            self.status.progress = 60.0
            batch_size = 10  # Process 20 articles per batch
            processed_articles = []
            for i in range(0, len(all_articles), batch_size):
                await _maybe_await(self.check_pause_or_cancel)

                batch = all_articles[i : i + batch_size]
                batch_processed = await self.processor.process_articles(
                    batch, self.config.keywords
                )
                processed_articles.extend(batch_processed)
                logger.info(
                    f"Processed batch {i // batch_size + 1}: {len(batch_processed)} articles"
                )
                gc.collect()  # Free memory after each batch

            logger.info(
                f"Processing completed. Retained {len(processed_articles)} articles."
            )
            self.status.progress = 80.0

            # Step 3: Generate report in batches
            self.status.current_task = "Generating final report"
            date_range_str = f"Từ ngày {self.start_date.strftime('%d/%m/%Y')} đến ngày {self.end_date.strftime('%d/%m/%Y')}"

            report = None
            if not processed_articles:
                logger.warning("No valid articles to generate report.")
                return None

            await _maybe_await(self.check_pause_or_cancel)

            report = await self.reporter.generate_report(
                processed_articles, date_range_str
            )

            if report is None:
                logger.warning("No valid articles to generate report.")
                return None

            # Update overall summary
            report.overall_summary.tong_so_bai = len(report.articles)
            report.total_articles = len(report.articles)
            report.date_range = date_range_str

            self.status.progress = 100.0
            self.status.current_task = "Pipeline completed"
            logger.info("Pipeline completed successfully.")
            await asyncio.sleep(0.5)
            return report

        except (InterruptedError, asyncio.CancelledError) as e:
            self.status.current_task = f"Stopped: {str(e)}"
            for _, task in tasks:
                task.cancel()
            results = await asyncio.gather(
                *[t[1] for t in tasks], return_exceptions=True
            )
            for (media_source, _), result in zip(tasks, results):
                if isinstance(result, Exception):
                    logger.error(f"[{media_source.name}] ❌ Lỗi: {result}")
                    self.source_status_list.append(
                        {"source_name": media_source.name, "status": "failed"}
                    )
                else:
                    logger.info(f"[{media_source.name}] ✅ Crawl thành công")
                    self.source_status_list.append(
                        {"source_name": media_source.name, "status": "completed"}
                    )
            self.status.current_task = "Đã hủy"
            raise
        except Exception as e:
            self.status.current_task = f"Failed: {str(e)}"
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return None
        finally:
            self.status.is_running = False
            self.status.last_run = datetime.now()
            if self.status.progress < 100.0:
                self.status.progress = 100.0
            if "completed" not in self.status.current_task.lower():
                self.status.current_task = "Ready"
            self.cleanup()
            gc.collect()  # Final memory cleanup

    def get_status(self) -> BotStatus:
        return self.status

    def cleanup(self):
        logger.info("🔧 Đang giải phóng tài nguyên pipeline...")
        # Đóng pool Playwright (không chặn thread gọi cleanup)
        try:
            asyncio.create_task(PlaywrightPool.instance().close())
        except RuntimeError:
            # nếu không có loop đang chạy, đóng đồng bộ “best effort”
            try:
                loop = asyncio.get_event_loop()
                loop.run_until_complete(PlaywrightPool.instance().close())
            except Exception:
                pass
        self.crawler.close_final()
        self.processor.close()
        self.reporter.close()
        gc.collect()
