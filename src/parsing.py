# src/parsing.py
"""
Module dedicated to parsing raw text/JSON content into structured Pydantic models.
This version includes a robust URL resolver and an intelligent text parser
that uses regular expressions to reliably extract data.
"""

import json
import logging
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse

from .models import Article, MediaSource, ContentCluster, IndustryType
from datetime import datetime

logger = logging.getLogger(__name__)

class ArticleParser:
    """
    A robust parser to extract Article objects from various response formats.
    """

    def _resolve_url(self, base_domain: Optional[str], relative_url: str) -> str:
        """
        Converts a relative URL to an absolute URL using the base domain.
        """
        if not relative_url or not isinstance(relative_url, str):
            return "https://example.com/invalid-link"
        
        parsed_url = urlparse(relative_url)
        if parsed_url.scheme and parsed_url.netloc:
            return relative_url
            
        if not base_domain:
            return relative_url 

        base_url_with_scheme = f"https://{base_domain}"
        return urljoin(base_url_with_scheme, relative_url)

    def parse(self, content: str, media_source: MediaSource) -> List[Article]:
        """
        Main parsing method. Tries to parse content as JSON first,
        then falls back to an intelligent text parsing method.
        """
        if not content or not content.strip():
            return []

        content = content.strip()

        try:
            if content.startswith('[') or content.startswith('{'):
                data = json.loads(content)
                if isinstance(data, list):
                    return self._parse_from_json_list(data, media_source)
                elif isinstance(data, dict):
                    for key in data:
                        if isinstance(data[key], list):
                            return self._parse_from_json_list(data[key], media_source)
        except json.JSONDecodeError:
            logger.warning("Content is not valid JSON. Falling back to text parsing.")

        return self._parse_text_articles_robust(content, media_source)

    def _parse_from_json_list(self, data: List[Dict], media_source: MediaSource) -> List[Article]:
        """Creates a list of Article objects from a list of dictionaries."""
        articles = []
        for i, item in enumerate(data):
            try:
                raw_link = item.get('link', item.get('url', item.get('link_bai_bao', '')))
                resolved_link = self._resolve_url(media_source.domain, raw_link)

                article = Article(
                    stt=item.get('stt', i + 1),
                    ngay_phat_hanh=item.get('date', item.get('ngay_phat_hanh', datetime.now().date())),
                    dau_bao=media_source.name,
                    cum_noi_dung=item.get('cum_noi_dung', ContentCluster.OTHER),
                    tom_tat_noi_dung=item.get('summary', item.get('tom_tat_noi_dung', item.get('title', ''))),
                    link_bai_bao=resolved_link,
                    nganh_hang=item.get('nganh_hang', IndustryType.DAU_AN),
                    nhan_hang=item.get('nhan_hang', []),
                    keywords_found=item.get('keywords_found', []),
                )
                articles.append(article)
            except Exception as e:
                logger.warning(f"Skipping an item in JSON list due to parsing error: {e} | Item: {item}")
        return articles

    def _parse_text_articles_robust(self, content: str, media_source: MediaSource) -> List[Article]:
        """
        NÂNG CẤP: Phân tích text một cách thông minh hơn bằng regex.
        """
        articles = []
        # Tách content thành các khối, giả định mỗi khối là một bài báo
        article_blocks = re.split(r'\n---\n|\n\n+', content)
        
        # Pattern để tìm URL
        url_pattern = re.compile(r'https?://[^\s\)\<]+')

        for block in article_blocks:
            block = block.strip()
            if not block:
                continue

            try:
                # 1. Tìm URL trước tiên - đây là phần độc nhất
                url_match = url_pattern.search(block)
                if not url_match:
                    continue  # Bỏ qua các khối không có URL

                raw_link = url_match.group(0)
                resolved_link = self._resolve_url(media_source.domain, raw_link)

                # 2. Làm sạch khối văn bản bằng cách loại bỏ URL và các phần markdown thừa
                # Regex này tìm và loại bỏ các cấu trúc như `(text)(url)` hoặc `(url)`
                cleaned_block = re.sub(r'\s*\([^)]*\)?\s*\(' + re.escape(raw_link) + r'\)', '', block)
                cleaned_block = cleaned_block.replace(f"({raw_link})", "") # Xóa nốt trường hợp chỉ có (url)

                # 3. Trích xuất các thông tin khác từ khối văn bản đã được làm sạch
                title_match = re.search(r'(?:Tiêu đề|Title)\s*:\s*(.*)', cleaned_block, re.IGNORECASE)
                date_match = re.search(r'(?:Ngày|Date)\s*:\s*(.*)', cleaned_block, re.IGNORECASE)
                
                title = title_match.group(1).strip() if title_match else ''
                date_str = date_match.group(1).strip() if date_match else datetime.now().date()
                
                # Phần còn lại của khối văn bản đã được làm sạch chính là tóm tắt
                summary_text = cleaned_block
                if title_match:
                    summary_text = summary_text.replace(title_match.group(0), '')
                if date_match:
                    summary_text = summary_text.replace(date_match.group(0), '')
                
                # Loại bỏ các dấu | thừa và làm sạch
                summary = " ".join(summary_text.split('|')).strip()
                
                final_summary = summary if summary else title
                if not final_summary:
                    continue # Bỏ qua nếu không có nội dung

                article = Article(
                    stt=len(articles) + 1,
                    ngay_phat_hanh=date_str,
                    dau_bao=media_source.name,
                    cum_noi_dung=ContentCluster.OTHER,
                    tom_tat_noi_dung=final_summary,
                    link_bai_bao=resolved_link,
                    nganh_hang=IndustryType.DAU_AN,
                )
                articles.append(article)
            
            except Exception as e:
                logger.warning(f"Could not create Article from text block '{block[:100]}...': {e}")

        return articles
