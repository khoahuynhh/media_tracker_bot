# src/parsing.py
"""
Module dedicated to parsing raw text/JSON content into structured Pydantic models.
This version includes a robust URL resolver and an intelligent text parser
that uses regular expressions to reliably extract data.
"""

import json
import logging
import re
import gc
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
import aiohttp
import asyncio
import chrono

from .models import Article, MediaSource, ContentCluster, IndustryType
from datetime import datetime, date

logger = logging.getLogger(__name__)


class ArticleParser:
    """
    A robust parser to extract Article objects from various response formats.
    """

    async def _validate_url(self, url: str, timeout: int = 5) -> bool:
        """
        Validates if a URL is accessible by sending an HTTP GET request.
        Returns True if the URL returns status code 200, False otherwise.
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=timeout) as response:
                    return response.status == 200
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"URL validation failed for {url}: {e}")
            return False

    def _resolve_url(self, base_domain: Optional[str], relative_url: str) -> str:
        """
        Converts a relative URL to an absolute URL using the base domain.
        Ensures the URL is valid and points to the specific article.
        """
        if not relative_url or not isinstance(relative_url, str):
            return "https://example.com/invalid-link"

        # Remove any surrounding brackets or parentheses
        relative_url = relative_url.strip("[]()")

        parsed_url = urlparse(relative_url)
        if parsed_url.scheme and parsed_url.netloc:
            return relative_url

        if not base_domain:
            return relative_url

        base_url_with_scheme = f"https://{base_domain}"
        return urljoin(base_url_with_scheme, relative_url)

    async def parse(
        self, content: str, media_source: MediaSource, start_index: int = 1
    ) -> List[Article]:
        """
        Main parsing method. Tries to parse content as JSON first,
        then falls back to an intelligent text parsing method.
        """
        if not content or not content.strip():
            return []

        content = content.strip()
        articles = []

        try:
            if content.startswith("[") or content.startswith("{"):
                data = json.loads(content)
                if isinstance(data, list):
                    articles = await self._parse_from_json_list(
                        data, media_source, start_index
                    )
                elif isinstance(data, dict):
                    for key in data:
                        if isinstance(data[key], list):
                            articles = await self._parse_from_json_list(
                                data[key], media_source, start_index
                            )
        except json.JSONDecodeError:
            logger.warning("Content is not valid JSON. Falling back to text parsing.")
            articles = await self._parse_text_articles_robust(
                content, media_source, start_index
            )

        # Filter articles with valid URLs only
        valid_articles = []
        for article in articles:
            if await self._validate_url(article.link_bai_bao):
                valid_articles.append(article)
            else:
                logger.info(
                    f"Skipping article with invalid URL: {article.link_bai_bao}"
                )

        return valid_articles

    def _parse_from_json_list(
        self, data: List[Dict], media_source: MediaSource, start_index: int
    ) -> List[Article]:
        """Creates a list of Article objects from a list of dictionaries."""
        articles = []
        for i, item in enumerate(data):
            try:
                raw_link = item.get(
                    "link", item.get("url", item.get("link_bai_bao", ""))
                )
                resolved_link = self._resolve_url(media_source.domain, raw_link)

                date_str = item.get(
                    "date", item.get("ngay_phat_hanh", item.get("ngày", ""))
                )
                pub_date = self._parse_date(date_str)

                # Ensure summary is concise
                summary = item.get(
                    "summary", item.get("tom_tat_noi_dung", item.get("title", ""))
                )
                summary = self._truncate_summary(summary)

                article = Article(
                    stt=start_index + i,
                    ngay_phat_hanh=pub_date,
                    dau_bao=media_source.name,
                    cum_noi_dung=item.get("cum_noi_dung", ContentCluster.OTHER),
                    tom_tat_noi_dung=summary,
                    link_bai_bao=resolved_link,
                    nganh_hang=item.get("nganh_hang", IndustryType.DAU_AN),
                    nhan_hang=item.get("nhan_hang", []),
                    keywords_found=item.get("keywords_found", []),
                )
                articles.append(article)
                if i % 50 == 0:  # Thu gom rác sau mỗi 50 item
                    gc.collect()
            except Exception as e:
                logger.warning(
                    f"Skipping an item in JSON list due to parsing error: {e} | Item: {item}"
                )
        return articles

    def _parse_text_articles_robust(
        self, content: str, media_source: MediaSource, start_index: int
    ) -> List[Article]:
        """
        Intelligent text parsing: extracts full Article fields from AI-generated text.
        Uses start_index for sequential stt.
        """
        from .models import ContentCluster, IndustryType

        articles = []
        article_blocks = re.split(r"\n---\n|\n\n+", content)

        batch_size = 50
        url_pattern = re.compile(r"https?://[^\s\)\<]+")

        for i in range(0, len(article_blocks), batch_size):
            batch = article_blocks[i : i + batch_size]
            for block in batch:
                block = block.strip()
                if not block:
                    continue

                try:
                    # 1. URL
                    url_match = url_pattern.search(block)
                    if not url_match:
                        continue
                    raw_link = url_match.group(0)
                    resolved_link = self._resolve_url(media_source.domain, raw_link)

                    # Regex này tìm và loại bỏ các cấu trúc như `(text)(url)` hoặc `(url)`
                    cleaned_block = re.sub(
                        r"\s*\([^)]*\)?\s*\(" + re.escape(raw_link) + r"\)", "", block
                    )
                    cleaned_block = cleaned_block.replace(
                        f"({raw_link})", ""
                    )  # Xóa nốt trường hợp chỉ có (url)

                    # 2. Regex patterns
                    title_match = re.search(
                        r"(?:Tiêu đề|Title)\s*:\s*(.*)", block, re.IGNORECASE
                    )
                    date_match = re.search(
                        r"(?:Ngày|Date)\s*:\s*(.*)", block, re.IGNORECASE
                    )
                    cluster_match = re.search(
                        r"(?:Cụm nội dung|Cluster)\s*:\s*(.*)", block, re.IGNORECASE
                    )
                    industry_match = re.search(
                        r"(?:Ngành|Industry)\s*:\s*(.*)", block, re.IGNORECASE
                    )
                    brands_match = re.search(
                        r"(?:Nhãn hàng|Brands)\s*:\s*(.*)", block, re.IGNORECASE
                    )
                    keywords_match = re.search(
                        r"(?:Keywords)\s*:\s*(.*)", block, re.IGNORECASE
                    )

                    # 3. Parse fields
                    title = title_match.group(1).strip() if title_match else ""
                    date_str = date_match.group(1).strip() if date_match else ""
                    pub_date = self._parse_date(date_str)

                    cluster = ContentCluster.OTHER
                    if cluster_match:
                        try:
                            cluster = ContentCluster(cluster_match.group(1).strip())
                        except:
                            pass

                    industry = IndustryType.DAU_AN
                    if industry_match:
                        try:
                            industry = IndustryType(industry_match.group(1).strip())
                        except:
                            pass

                    nhan_hang = []
                    if brands_match:
                        nhan_hang = [
                            b.strip()
                            for b in brands_match.group(1).split(",")
                            if b.strip()
                        ]

                    keywords = []
                    if keywords_match:
                        keywords = [
                            k.strip()
                            for k in keywords_match.group(1).split(",")
                            if k.strip()
                        ]

                    # 4. Summary: extract remaining content after removing meta lines
                    summary = block
                    for m in [
                        title_match,
                        date_match,
                        cluster_match,
                        industry_match,
                        brands_match,
                        keywords_match,
                    ]:
                        if m:
                            summary = summary.replace(m.group(0), "")
                    summary = summary.replace(raw_link, "")
                    summary = self._truncate_summary(summary)

                    article = Article(
                        stt=len(articles) + start_index,
                        ngay_phat_hanh=pub_date,
                        dau_bao=media_source.name,
                        cum_noi_dung=cluster,
                        tom_tat_noi_dung=summary if summary else title,
                        link_bai_bao=resolved_link,
                        nganh_hang=industry,
                        nhan_hang=nhan_hang,
                        keywords_found=keywords,
                    )
                    articles.append(article)

                except Exception as e:
                    logger.warning(
                        f"[Parser] Skipping block due to error: {e} | Content: {block[:100]}..."
                    )
            gc.collect()
        return articles

    def _parse_date(self, date_str: str) -> date:
        """
        Parse date string using chrono for flexible date parsing.
        Falls back to current date if parsing fails.
        """
        if not date_str:
            return datetime.now().date()

        try:
            parsed_date = chrono.parse_date(date_str)
            if parsed_date:
                return parsed_date.date()
        except Exception:
            pass

        # Try additional formats
        for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m"]:
            try:
                parsed = datetime.strptime(date_str, fmt)
                if fmt == "%d/%m":  # Add current year if only day/month
                    parsed = parsed.replace(year=datetime.now().year)
                return parsed.date()
            except ValueError:
                continue

        logger.warning(f"Could not parse date: {date_str}. Using current date.")
        return datetime.now().date()

    def _truncate_summary(self, summary: str, max_words: int = 100) -> str:
        """
        Truncate summary to max_words, ensuring it's clean and concise.
        """
        if not summary:
            return ""

        words = summary.split()
        if len(words) > max_words:
            summary = " ".join(words[:max_words]) + "..."
        return " ".join(summary.split()).strip()
