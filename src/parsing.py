# src/parsing.py
"""
Module dedicated to parsing raw text/JSON content into structured Pydantic models.
This version includes a fallback to text parsing if JSON parsing fails,
making the system more robust and forgiving.
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
        then falls back to structured text parsing from the original implementation.
        """
        if not content or not content.strip():
            return []

        content = content.strip()

        # 1. Thử phân tích JSON trước
        try:
            if content.startswith('[') or content.startswith('{'):
                data = json.loads(content)
                # Xử lý trường hợp JSON là một list hoặc một object chứa list
                if isinstance(data, list):
                    return self._parse_from_json_list(data, media_source)
                elif isinstance(data, dict):
                    for key in data:
                        if isinstance(data[key], list):
                            return self._parse_from_json_list(data[key], media_source)
                logger.warning("JSON received but not in expected list format. Falling back to text parsing.")
        except json.JSONDecodeError:
            logger.warning("Content is not valid JSON. Falling back to text parsing.")

        # 2. Nếu JSON thất bại, quay lại dùng logic phân tích text gốc của bạn
        return self._parse_text_articles(content, media_source)

    def _parse_from_json_list(self, data: List[Dict], media_source: MediaSource) -> List[Article]:
        """Creates a list of Article objects from a list of dictionaries."""
        articles = []
        for i, item in enumerate(data):
            try:
                article = Article(
                    stt=item.get('stt', i + 1),
                    ngay_phat_hanh=item.get('date', item.get('ngay_phat_hanh', datetime.now().date())),
                    dau_bao=media_source.name,
                    cum_noi_dung=item.get('cum_noi_dung', ContentCluster.OTHER),
                    tom_tat_noi_dung=item.get('summary', item.get('tom_tat_noi_dung', item.get('title', ''))),
                    link_bai_bao=item.get('link', item.get('url', item.get('link_bai_bao', ''))),
                    nganh_hang=item.get('nganh_hang', IndustryType.DAU_AN),
                    nhan_hang=item.get('nhan_hang', []),
                    keywords_found=item.get('keywords_found', []),
                )
                articles.append(article)
            except Exception as e:
                logger.warning(f"Skipping an item in JSON list due to parsing error: {e} | Item: {item}")
        return articles

    def _parse_text_articles(self, content: str, media_source: MediaSource) -> List[Article]:
        """
        KHÔI PHỤC LOGIC GỐC: Phân tích articles từ định dạng text "Tiêu đề | Ngày | Tóm tắt | Link".
        This is the original, brittle but effective parsing logic.
        """
        articles = []
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 4:
                title, date_str, summary, link = parts[0], parts[1], parts[2], parts[3]
                try:
                    article = Article(
                        stt=len(articles) + 1,
                        ngay_phat_hanh=date_str,
                        dau_bao=media_source.name,
                        cum_noi_dung=ContentCluster.OTHER, # Sẽ được Processor phân loại lại
                        tom_tat_noi_dung=summary if summary else title,
                        link_bai_bao=link,
                        nganh_hang=IndustryType.DAU_AN, # Sẽ được Processor phân loại lại
                    )
                    articles.append(article)
                except Exception as e:
                    logger.warning(f"Could not create Article from text line '{line}': {e}")
        
        if not articles:
             logger.warning("Text parsing did not find any articles in the expected 'Title | Date | Summary | Link' format.")

        return articles
