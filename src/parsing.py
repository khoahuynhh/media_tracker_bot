# src/parsing.py
"""
Module dedicated to parsing raw text/JSON content into structured Pydantic models.
This isolates brittle parsing logic from the agents.
"""

import json
import logging
from typing import List, Dict, Optional

from .models import Article, MediaSource, ContentCluster, IndustryType
from datetime import datetime

logger = logging.getLogger(__name__)

class ArticleParser:
    """
    A robust parser to extract Article objects from various response formats.
    """

    def parse(self, content: str, media_source: MediaSource) -> List[Article]:
        """
        Main parsing method. Tries to parse content as JSON first,
        then falls back to structured text parsing.
        """
        if not content or not content.strip():
            return []

        content = content.strip()

        # Attempt to parse as JSON array
        if content.startswith('['):
            try:
                data = json.loads(content)
                if isinstance(data, list):
                    return self._parse_from_json_list(data, media_source)
            except json.JSONDecodeError:
                logger.warning("Content looks like a list but failed to parse as JSON. Falling back to text parsing.")

        # Attempt to parse as a single JSON object (if it's a list wrapped in an object)
        if content.startswith('{'):
            try:
                data = json.loads(content)
                # Check for common patterns where the list is a value of a key
                for key in data:
                    if isinstance(data[key], list):
                        return self._parse_from_json_list(data[key], media_source)
            except json.JSONDecodeError:
                logger.warning("Content looks like an object but failed to parse as JSON. Falling back to text parsing.")

        # Fallback to text-based parsing
        logger.info("Response is not a direct JSON list, attempting text-based parsing.")
        return self._parse_from_text(content, media_source)

    def _parse_from_json_list(self, data: List[Dict], media_source: MediaSource) -> List[Article]:
        """Creates a list of Article objects from a list of dictionaries."""
        articles = []
        for i, item in enumerate(data):
            try:
                # Map dictionary keys to Article fields
                # This is more robust than assuming exact key names
                article = Article(
                    stt=item.get('stt', i + 1),
                    ngay_phat_hanh=item.get('ngay_phat_hanh', item.get('date', datetime.now().date())),
                    dau_bao=media_source.name,
                    cum_noi_dung=item.get('cum_noi_dung', ContentCluster.OTHER),
                    tom_tat_noi_dung=item.get('tom_tat_noi_dung', item.get('summary', item.get('title', ''))),
                    link_bai_bao=item.get('link_bai_bao', item.get('link', item.get('url', ''))),
                    nganh_hang=item.get('nganh_hang', IndustryType.DAU_AN), # Default, will be classified later
                    nhan_hang=item.get('nhan_hang', []),
                    keywords_found=item.get('keywords_found', []),
                )
                articles.append(article)
            except Exception as e:
                logger.warning(f"Skipping an item in JSON list due to parsing error: {e} | Item: {item}")
        return articles

    def _parse_from_text(self, content: str, media_source: MediaSource) -> List[Article]:
        """
        Parses articles from a structured text format.
        This implementation is more resilient to variations.
        """
        articles = []
        # Split by a common delimiter for articles, like '---' or a double newline
        # followed by a number, or 'Article:'.
        # This regex looks for '---', or two newlines.
        import re
        article_blocks = re.split(r'\n---\n|\n\n', content)

        for i, block in enumerate(article_blocks):
            block = block.strip()
            if not block:
                continue

            try:
                # Use regex to find key-value pairs, ignoring case and whitespace
                title_match = re.search(r'(?:tiêu đề|title)\s*:\s*(.*)', block, re.IGNORECASE)
                link_match = re.search(r'(?:link|url)\s*:\s*(.*)', block, re.IGNORECASE)
                summary_match = re.search(r'(?:tóm tắt|summary)\s*:\s*(.*)', block, re.IGNORECASE)
                date_match = re.search(r'(?:ngày|date)\s*:\s*(.*)', block, re.IGNORECASE)

                # Require at least a link and a title/summary
                if not link_match or not (title_match or summary_match):
                    continue

                link = link_match.group(1).strip()
                title = title_match.group(1).strip() if title_match else ''
                summary = summary_match.group(1).strip() if summary_match else title
                date_str = date_match.group(1).strip() if date_match else datetime.now().date()

                article = Article(
                    stt=len(articles) + 1,
                    ngay_phat_hanh=date_str,
                    dau_bao=media_source.name,
                    cum_noi_dung=ContentCluster.OTHER,
                    tom_tat_noi_dung=summary,
                    link_bai_bao=link,
                    nganh_hang=IndustryType.DAU_AN, # Default
                    nhan_hang=[],
                    keywords_found=[],
                )
                articles.append(article)
            except Exception as e:
                logger.warning(f"Could not parse text block into an article: {e}\nBlock: '{block[:100]}...'")
        
        return articles
