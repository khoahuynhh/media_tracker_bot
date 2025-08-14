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

from datetime import datetime, date
from typing import List, Dict, Optional, Any, Tuple
from playwright.sync_api import sync_playwright
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from bs4 import BeautifulSoup
from asyncio import Semaphore

# Import Agno
from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.models.groq import Groq
from agno.models.google import Gemini
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
    HubPageNotFound,
)
from .configs import CONFIG_DIR, settings
from .cache_manager import SafeCacheManager
from .task_state import task_manager

logger = logging.getLogger(__name__)

PROVIDER_MODEL_MAP = {
    "openai": {"default": "gpt-4o-mini", "report": "gpt-4o-mini"},
    "groq": {"default": "llama-3.1-70b-versatile", "report": "llama-3.1-70b-versatile"},
    "gemini": {"default": "gemini-2.0-flash", "report": "gemini-2.0-flash"},
}

# Async http_client với retry tối đa 3 lần
http_client = httpx.AsyncClient(
    timeout=60.0,
    limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
    transport=httpx.AsyncHTTPTransport(retries=3),  # <-- retry tối đa 3 lần
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
        return Gemini(id=mid)
    else:
        return OpenAIChat(id=mid, http_client=http_client)


# API Errors
def _configured_providers_in_order(settings, preferred: str | None = None) -> list[str]:
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


# Search tools
def ddgs_search_text(query: str):
    time.sleep(1.5)
    # headers = {
    #     "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/{random.randint(100,120)}.0.{random.randint(1000,9999)}.100 Safari/537.36"
    # }
    with DDGS(timeout=60) as ddg:
        return list(ddg.text(query, max_results=30, backend="auto"))


class GoogleSearchWithDelay(GoogleSearchTools):
    def run(self, query: str):
        time.sleep(random.uniform(2.5, 5.0))  # Delay tự nhiên
        return super().run(query)


def google_cse_search(domain, keywords):
    API_KEY = os.getenv("GOOGLE_API_KEY")
    query = f"site:{domain} {' '.join(keywords)} tag"
    url = "https://www.googleapis.com/customsearch/v1"
    params = {"key": API_KEY, "cx": "16c863775f52f42cd", "q": query, "num": 10}
    resp = requests.get(url, params=params)
    data = resp.json()

    if "items" not in data:
        print("❌ Không tìm thấy kết quả:", data)
        return None

    for item in data["items"]:
        print(item["link"])
    return data["items"][0]["link"]


def _norm_host(h):
    h = h.lower()
    return h[4:] if h.startswith("www.") else h


def get_first_search_link(domain: str, keywords: list[str]) -> str | None:
    wanted_host = _norm_host(domain)

    def search_ddg():
        headers = {
            "User-Agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/{random.randint(100,120)}.0.{random.randint(1000,9999)}.100 Safari/537.36"
        }
        query = f"site:{domain} {' '.join(keywords)} tin tức"
        with DDGS(headers=headers, timeout=60) as ddg:
            results = list(ddg.text(query, max_results=20, backend="auto"))
            logger.info(
                f"🔍 DuckDuckGo results ({domain}): {[r.get('href') for r in results]}"
            )

            same_domain = []
            for r in results:
                href = r.get("href") or ""
                host = _norm_host(urlparse(href).netloc)
                if host == wanted_host:
                    same_domain.append(href)

            def is_tag_hub(u: str) -> bool:
                lu = u.lower()
                return (
                    "/tag" in lu
                    or "/tags/" in lu
                    or "/chu-de/" in lu
                    or "/the/" in lu
                    or "/topic/" in lu
                )

            def contains_kw(u: str) -> bool:
                lu = u.lower()
                return any(kw.lower() in lu for kw in keywords)

            # a) tag-hub + chứa keyword trong URL
            for href in same_domain:
                if is_tag_hub(href) and contains_kw(href):
                    return href
            # b) chỉ cần là tag-hub đúng domain
            for href in same_domain:
                if is_tag_hub(href):
                    return href
            # c) fallback: bất kỳ link nào đúng domain
            if same_domain:
                return same_domain[0]

            # d) fallback toàn bộ results
            for r in results:
                href = r.get("href") or ""
                if _norm_host(urlparse(href).netloc) == wanted_host:
                    return href

            return None

    # --- 1. DDG lần 1 ---
    link = search_ddg()
    if link:
        return link

    # --- 2. DDG lần 2 ---
    logger.warning(f"⚠️ Không tìm thấy hub link cho {domain}, thử lại sau 10s...")
    time.sleep(10)
    link = search_ddg()
    if link:
        return link

    # --- 3. Fallback sang Google CSE ---
    logger.warning(f"⚠️ DDG thất bại, fallback sang Google CSE cho {domain}")
    return google_cse_search(domain, keywords)


_playwright_sem = Semaphore(2)


def crawl_with_playwright_sync(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                # hai flag dưới chỉ dùng nếu máy/antivirus khó tính:
                # "--single-process", "--no-zygote",
            ],
        )
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0 Safari/537.36"
            )
        )
        page = context.new_page()
        try:
            page.goto(url, timeout=60_000, wait_until="domcontentloaded")
            # nếu còn request ngầm, cho đợi thêm chút
            try:
                page.wait_for_load_state("networkidle", timeout=5_000)
            except:
                pass
            html = page.content()
            return html
        finally:
            context.close()
            browser.close()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=4),
    reraise=True,
)
async def crawl_with_playwright(url: str) -> str:
    # wrapper async gọi bản sync trong thread pool, có semaphore hạn chế song song
    async with _playwright_sem:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, crawl_with_playwright_sync, url)


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


def _norm_iso(s: str | None) -> str | None:
    if not s:
        return None
    s = s.strip()
    # ISO trong meta/json-ld
    try:
        from datetime import datetime

        s2 = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s2).date().isoformat()
    except Exception:
        pass
    # dd/mm/yyyy (VN)
    m = _VN_DATE_RE.search(s)
    if m:
        d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
        if 1 <= d <= 31 and 1 <= mo <= 12:
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def extract_dates_rule_based(html: str, url: str):
    """
    Trả về dict:
    {
      'published_iso': 'YYYY-MM-DD' | None,
      'modified_iso': 'YYYY-MM-DD' | None,
      'source_published_text': 'chuỗi gốc' | None
    }
    Ưu tiên: JSON-LD -> meta article:published_time -> <time datetime> -> text khối meta gần tiêu đề.
    """
    soup = BeautifulSoup(html, "html.parser")
    head = soup.find("head") or soup

    # 1) JSON-LD (NewsArticle/Article)
    for s in head.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string or "")
            items = data if isinstance(data, list) else [data]
            for it in items:
                if isinstance(it, dict) and it.get("@type") in (
                    "NewsArticle",
                    "Article",
                    "Report",
                ):
                    dp = it.get("datePublished") or it.get("dateCreated")
                    dm = it.get("dateModified")
                    pub_iso = _norm_iso(dp)
                    mod_iso = _norm_iso(dm)
                    if pub_iso:
                        return {
                            "published_iso": pub_iso,
                            "modified_iso": mod_iso,
                            "source_published_text": dp,
                        }
        except Exception:
            pass

    # 2) OpenGraph/Article meta
    meta_pub = head.find("meta", {"property": "article:published_time"}) or head.find(
        "meta", {"name": "pubdate"}
    )
    if meta_pub and meta_pub.get("content"):
        pub_iso = _norm_iso(meta_pub["content"])
        meta_mod = head.find("meta", {"property": "article:modified_time"})
        mod_iso = (
            _norm_iso(meta_mod["content"])
            if meta_mod and meta_mod.get("content")
            else None
        )
        if pub_iso:
            return {
                "published_iso": pub_iso,
                "modified_iso": mod_iso,
                "source_published_text": meta_pub.get("content"),
            }

    # 3) <time datetime="...">
    t = soup.find("time", datetime=True)
    if t:
        pub_iso = _norm_iso(t.get("datetime"))
        if pub_iso:
            return {
                "published_iso": pub_iso,
                "modified_iso": None,
                "source_published_text": t.get_text(" ", strip=True)
                or t.get("datetime"),
            }

    # 4) Text gần tiêu đề (phù hợp Lao Động / Thanh Niên)
    # Gom các khối meta phổ biến quanh tiêu đề
    meta_blocks = []
    for sel in [
        ".article__meta",
        ".meta",
        ".date",
        ".ldo-meta",
        "span.time",
        ".details__meta",
        ".details__time",
    ]:
        meta_blocks += [el.get_text(" ", strip=True) for el in soup.select(sel)]
    joined = " | ".join([b for b in meta_blocks if b])

    m = _VN_DATE_RE.search(joined)
    if m:
        d, mo, y = int(m.group("d")), int(m.group("m")), int(m.group("y"))
        pub_iso = f"{y:04d}-{mo:02d}-{d:02d}"
        return {
            "published_iso": pub_iso,
            "modified_iso": None,
            "source_published_text": joined[m.start() : m.end()],
        }

    return {"published_iso": None, "modified_iso": None, "source_published_text": None}


def extract_title_rule_based(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    # Ưu tiên thẻ <h1>, rồi meta og:title
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    og = soup.find("meta", {"property": "og:title"})
    if og and og.get("content"):
        return og["content"].strip()
    if soup.title and soup.title.get_text(strip=True):
        return soup.title.get_text(strip=True)
    return None


async def validate_and_normalize_link(
    raw_url: str, allowed_domain: str, timeout=10
) -> str | None:
    """Trả về URL đã chuẩn hoá nếu hợp lệ, ngược lại None."""
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/123 Safari/537.36"
            },
        ) as client:
            r = await client.get(raw_url)
        if r.status_code != 200:
            return None
        ctype = r.headers.get("Content-Type", "")
        if "text/html" not in ctype:
            return None

        final_url = str(r.url)

        # Bỏ tracking params
        u = urlparse(final_url)
        q = [
            (k, v)
            for k, v in parse_qsl(u.query, keep_blank_values=True)
            if not k.lower().startswith(("utm_", "fbclid"))
        ]
        final_url = urlunparse(u._replace(query=urlencode(q, doseq=True)))

        # Canonical nếu có
        soup = BeautifulSoup(r.text, "html.parser")
        can = soup.find("link", rel=lambda x: x and "canonical" in x.lower())
        if can and can.get("href"):
            final_url = can["href"].strip()

        # Check domain cuối cùng
        host = urlparse(final_url).netloc.lower().lstrip("www.")
        if host != allowed_domain.lower().lstrip("www."):
            return None

        # (tuỳ chọn) basic sanity: phải có <h1> hoặc og:title
        if not (soup.find("h1") or soup.find("meta", {"property": "og:title"})):
            return None

        return final_url
    except Exception:
        return None


# --- END: date/title/url extract helpers ---
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
        last_err = None
        providers = _configured_providers_in_order(settings, preferred_provider)
        asked = set()  # tránh hỏi trùng 1 provider/model trong cùng lượt
        seq = 0
        if not providers:
            raise RuntimeError("No LLM provider configured")

        for i, prov in enumerate(providers):
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
                task = asyncio.create_task(
                    self.agent.arun(prompt, session_id=session_id)
                )
                AgentManager.get_instance().register_provider_task(
                    session_id, prov, task
                )
                return await asyncio.wait_for(task, timeout=35)

            except Exception as e:
                code, msg = _map_llm_error(e)
                last_err = e
                remaining = providers[i + 1 :]

                async with fallback_lock_by_session[session_id]:
                    # nếu đã hỏi provider/model này rồi thì bỏ qua lần hỏi nữa
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
                        # đưa provider user chọn lên tiếp theo
                        providers = (
                            providers[: i + 1]
                            + [picked]
                            + [x for x in remaining if x != picked]
                        )
                    continue

        raise last_err or RuntimeError("No provider available")


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
        self.agent = Agent(
            name="HubCrawler",
            role="Web Crawler và Content Extractor",
            tools=[Crawl4aiTools(max_length=10000)],
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
    ) -> CrawlResult:
        try:
            hub_url = get_first_search_link(media_source.domain, keywords)
            if not hub_url:
                logger.warning(
                    f"[{media_source.name}] ❌ Không tìm thấy hub phù hợp cho từ khóa: {keywords}"
                )
                raise HubPageNotFound("System error. Please try again.")

            logger.info(f"[{media_source.name}] 🔗 Hub URL: {hub_url}")

            await _maybe_await(self.check_pause_or_cancel)
            html = await crawl_with_playwright(hub_url)
            soup = BeautifulSoup(html, "html.parser")

            article_links = []
            for a in soup.select("a"):
                href = a.get("href", "")
                if not href or "/video/" in href or "/tags/" in href:
                    continue
                if any(kw.lower() in href.lower() for kw in keywords):
                    full_url = (
                        href
                        if href.startswith("http")
                        else f"https://{media_source.domain}{href}"
                    )
                    article_links.append(full_url)

            valid_links = list(
                dict.fromkeys(
                    link
                    for link in article_links
                    if media_source.domain in link
                    and "admicro" not in link
                    and "adn" not in link
                )
            )

            logger.info(
                f"[{media_source.name}] 🔗 Tìm được {len(valid_links)} bài viết từ hub"
            )

            sem = Semaphore(4)  # tuỳ quota
            start_t = time.monotonic()
            all_articles = []

            async def process_link(link):
                await _maybe_await(self.check_pause_or_cancel)

                # 1) Tự crawl HTML bài để lấy ngày/title rule-based (ổn cho Lao Động)
                art_html = await crawl_with_playwright(link)
                meta = extract_dates_rule_based(art_html, link)
                title_rb = extract_title_rule_based(art_html)

                # 2) Build prompt tuỳ trường hợp
                if meta.get("published_iso"):
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
                return self.parser.parse(text, media_source)

            tasks = []
            for link in valid_links[:8]:

                async def worker(l=link):
                    async with sem:
                        try:
                            return await process_link(l)
                        except Exception as e:
                            logger.warning(f"Parse fail {l}: {e}")
                            return []

                tasks.append(asyncio.create_task(worker()))

            parsed_lists = await asyncio.gather(*tasks)
            all_articles = [a for lst in parsed_lists for a in lst]

            duration = time.monotonic() - start_t

            def to_date(x):
                if isinstance(x, datetime):
                    return x.date()
                if isinstance(x, date):
                    return x
                if isinstance(x, str):
                    # Ưu tiên ISO
                    try:
                        from datetime import datetime as _dt

                        return _dt.fromisoformat(x).date()
                    except Exception:
                        m = _VN_DATE_RE.search(x)
                        if m:
                            d, mo, y = (
                                int(m.group("d")),
                                int(m.group("m")),
                                int(m.group("y")),
                            )
                            from datetime import date as _date

                            return _date(y, mo, d)
                return None

            def in_range(a: Article):
                pub = to_date(a.ngay_phat_hanh)
                return pub is not None and start_date.date() <= pub <= end_date.date()

            filtered = [a for a in all_articles if in_range(a)]

            return CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url=hub_url,
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
                url=hub_url if "hub_url" in locals() else "",
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
        "Ngày phát hành": "DD-MM-YYYY",
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
            cache_dir="cache/crawl_results",
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
            self.model, self.parser, self.session_id, self.config, self.check_pause_or_cancel
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
                "Ưu tiên tin tức mới nhất trong khoảng thời gian được chỉ định.",
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
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type(
            (APITimeoutError, httpx.ConnectTimeout, httpx.ReadTimeout)
        ),
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
            base_order = list(range(len(self.search_tools)))
            start_tool_index = self.rotate_index
            self.rotate_index = (self.rotate_index + 1) % len(self.search_tools)
            tools_to_try = base_order[start_tool_index:] + base_order[:start_tool_index]

        start_group_index = checkpoint.get("group_index", 0)
        start_tool_index = checkpoint.get("tool_index", 0)

        try:
            # LẶP TỪNG KEYWORD
            for g_idx in range(start_group_index, len(keyword_groups)):
                await _maybe_await(self.check_pause_or_cancel)
                group = keyword_groups[g_idx]  # ví dụ ["Tường An"]
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
                        * 50.0,
                        current_task=f"Crawling {media_source.name} ({industry_name}) – keyword: {kw_str}",
                    )
                found_for_this_keyword = False

                if self.config.use_hub_page:
                    try:
                        result = await self.hub_tool.run(
                            media_source, group, start_date, end_date
                        )
                        if result and result.articles_found:
                            self.current_articles = result.articles_found.copy()
                            self.articles_so_far = result.articles_found.copy()
                            found_for_this_keyword = True
                            return result
                    except HubPageNotFound as e:
                        logger.warning(f"[{media_source.name}] ⚠️ {str(e)}")
                        return CrawlResult(
                            source_name=media_source.name,
                            source_type=media_source.type,
                            url=media_source.domain,
                            articles_found=[],
                            crawl_status="failed",
                            error_message=str(e),
                            crawl_duration=0.0,
                        )
                    except Exception as e:
                        logger.warning(
                            f"[{media_source.name}] ⚠️ Lỗi hub cho keyword '{keywords_str}': {e}"
                        )
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
                    for i in range(start_tool_index, len(tools_to_try)):
                        await _maybe_await(self.check_pause_or_cancel)
                        tool_index = tools_to_try[i]
                        tool_name = type(self.search_tools[tool_index]).__name__

                        self.search_tool_index = tool_index
                        self._create_agent()

                        new_articles_this_tool = 0
                        self.current_articles = []

                        for group_index in range(
                            start_group_index, len(keyword_groups)
                        ):
                            await _maybe_await(self.check_pause_or_cancel)
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
                                    * 50.0,
                                    current_task=f"Crawling {media_source.name} ({industry_name}) – từ khóa: {flat_keywords}",
                                )

                            query_variants = [
                                # f"Công ty {industry_name} {keywords_str} site:{media_source.domain} tháng {start_date.month} {start_date.year}",
                                f"{keywords_str} {industry_name} tin tức mới nhất tháng {start_date.month} {start_date.year} site:{media_source.domain}",
                                f"site:{media_source.domain} {keywords_str} tháng {start_date.month} {start_date.year}",
                            ]
                            query_lines = "\n".join([f"- {q}" for q in query_variants])

                            crawl_query = f"""
                            Crawl website: {domain_url or media_source.name}
                            Tìm các bài báo trên {domain_url or media_source.name} có chứa các từ khóa: {keywords_str} trong tiêu đề hoặc trong bài viết và PHẢI liên quan đến ngành hàng: {industry_name}
                            Thời gian: {date_filter}
                            Bạn nên thử tất cả các câu truy vấn sau:
                            {query_lines}
                            Yêu cầu:
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
                                        response.content, media_source
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

                            task_manager.update_task(
                                self.user_email,
                                self.session_id,
                                {
                                    "articles_so_far": [
                                        a.model_dump(mode="json") for a in articles
                                    ],
                                    "crawl_checkpoint": {
                                        media_source.name: {
                                            "tool_order": tools_to_try,
                                            "tool_index": i,
                                            "group_index": group_index + 1,
                                        }
                                    },
                                },
                            )

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

                    # if (
                    #     self.config.use_cache
                    #     and result.crawl_status == "success"
                    #     and len(result.articles_found) > 0
                    # ):
                    #     self.cache_manager.save_cache(cache_key, result)

                    # return result

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

    def __init__(self, model: Any):
        self.agent = Agent(
            name="ContentProcessor",
            role="Chuyên gia Phân tích và Phân loại Nội dung",
            model=model,
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
        batch_size = 10  # Process 20 articles per batch to reduce memory usage

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
                    - Đọc nội dung bài viết và kiểm tra xem có nhãn hàng nào trong danh sách sau xuất hiện hay không: 
                    {json.dumps(brand_list, ensure_ascii=False, indent=2)}
                    - Chỉ ghi nhận những nhãn hàng thực sự xuất hiện trong bài viết (bất kể viết hoa hay viết thường).
                    - Nếu không thấy nhãn hàng nào thì để `nhan_hang` là `[]`. Không tự bịa hoặc tự suy đoán thêm.
                3. Phân loại lại cụm nội dung (`cum_noi_dung`). Nếu bài viết có nội dung tương đương, đồng nghĩa hoặc gần giống với các cụm từ khóa: {json.dumps({k.value: v for k, v in content_clusters.items()}, ensure_ascii=False, indent=2)}, hãy phân loại vào cụm đó.
                4. Nếu không tìm thấy cụm nội dung nào khớp với danh sách từ khóa cụm nội dung, BẮT BUỘC gán trường (`cum_noi_dung`) là '{ContentCluster.OTHER.value}', KHÔNG ĐƯỢC để trống hoặc trả về none hay null.
                5. Viết lại nội dung chi tiết, ngắn gọn và mang tính mô tả khái quát cho trường `cum_noi_dung_chi_tiet` dựa trên nội dung đã có sẵn:
                    - Là 1 dòng mô tả ngắn (~10–20 từ) cho bài báo, có cấu trúc:  
                    `[Loại thông tin]: [Tóm tắt nội dung nổi bật]`
                    - Ví dụ: "Thông tin doanh nghiệp: Tường An khẳng định vị thế dịp Tết 2025"
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

                            processed_articles.extend(
                                [Article(**item) for item in articles_data]
                            )
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

    def __init__(self, model: Any):
        self.agent = Agent(
            name="ReportGenerator",
            role="Chuyên gia Tạo Báo cáo và Phân tích Dữ liệu",
            model=model,
            instructions=[
                "Bạn là chuyên gia tạo báo cáo phân tích truyền thông cho ngành FMCG.",
                "Nhiệm vụ: Tạo một báo cáo phân tích đối thủ cạnh tranh từ dữ liệu các bài báo đã được cung cấp.",
                "Bạn BẮT BUỘC phải trả về kết quả dưới dạng một đối tượng JSON (JSON object) duy nhất, hợp lệ và tuân thủ nghiêm ngặt theo cấu trúc của Pydantic model 'CompetitorReport'.",
                "Nếu không có dữ liệu hoặc có lỗi, trả về CompetitorReport rỗng hợp lệ với các trường là [] hoặc 0.",
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
            logger.error(f"Raw LLM response: {response.content}")
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

        industry_summaries = [
            IndustrySummary(
                nganh_hang=industry,
                nhan_hang=list(
                    set(
                        brand
                        for article in industry_articles
                        for brand in article.nhan_hang
                    )
                ),
                cum_noi_dung=list(
                    set(
                        c
                        for article in industry_articles
                        if (c := article.cum_noi_dung) is not None
                    )
                )
                or [ContentCluster.OTHER.value],
                so_luong_bai=len(industry_articles),
                cac_dau_bao=list(set(article.dau_bao for article in industry_articles)),
            )
            for industry, industry_articles in industry_groups.items()
        ]

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
        self.processor = ProcessorAgent(model_runtime)
        self.reporter = ReportAgent(model_runtime)
        self.start_date = start_date
        self.end_date = end_date
        self.source_status_list = source_status_list or []
        self.config.use_hub_page = True

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
                            self.articles_so_far.extend(result.articles_found)

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
                            self.articles_so_far.extend(partial_result.articles_found)

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
            tasks = []
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

                    task = asyncio.create_task(
                        wrapped_crawl(media_source, industry_name, keywords)
                    )
                    tasks.append((media_source, task))

            # Execute crawl tasks
            tasks_only = [t[1] for t in tasks]
            results = await asyncio.gather(*tasks_only, return_exceptions=True)

            for (media_source, _), result in zip(tasks, results):
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

                    media_source, crawl_result = result
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
                            * 50.0,
                            current_task=f"Crawling {media_source.name}",
                        )

                    gc.collect()

                except asyncio.CancelledError:
                    logger.warning(f"Crawl task cancelled.")
                    for _, t in tasks:
                        t.cancel()
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
        self.crawler.close_final()
        self.processor.close()
        self.reporter.close()
        gc.collect()
