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

    def parse(
        self,
        content: str,
        media_source: MediaSource,
        industry_name: Optional[str] = None,
    ) -> List[Article]:
        """
        Main parsing method. Tries to parse content as JSON first,
        then falls back to an intelligent text parsing method.
        """
        if not content or not content.strip():
            return []

        content = content.strip()

        try:
            if "```json" in content:
                content = content.split("```json", 1)[1]

            if "```" in content:
                content = content.split("```", 1)[0]

            if content.strip().startswith("[") or content.strip().startswith("{"):
                data = json.loads(content)
                if isinstance(data, list):
                    data = [it for it in data if isinstance(it, dict)]
                    if data:
                        return self._parse_from_json_list(
                            data, media_source, industry_name
                        )
                elif isinstance(data, dict):
                    # Ưu tiên key "articles" nếu có
                    if "articles" in data and isinstance(data["articles"], list):
                        return self._parse_from_json_list(
                            data["articles"], media_source, industry_name
                        )
                    for key in data:
                        if isinstance(data[key], list):
                            return self._parse_from_json_list(
                                data[key], media_source, industry_name
                            )
                    return self._parse_from_json_list([data], media_source)
        except json.JSONDecodeError:
            logger.warning("Content is not valid JSON. Falling back to text parsing.")

        return self._parse_text_articles_robust(content, media_source, industry_name)

    def _parse_from_json_list(
        self,
        data: List[Dict],
        media_source: MediaSource,
        industry_name: Optional[str] = None,
    ) -> List[Article]:
        """Creates a list of Article objects from a list of dictionaries."""
        articles = []
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                logger.warning(f"[Parser] Bỏ qua item không phải object: {item!r}")
                continue
            try:
                raw_link = (
                    item.get("link")
                    or item.get("Link")
                    or item.get("url")
                    or item.get("link bài báo")
                    or item.get("link_bài_báo")
                    or item.get("link_bai_bao")
                    or item.get("link bai bao")
                    or ""
                )
                resolved_link = self._resolve_url(media_source.domain, raw_link)

                raw_summary = (
                    item.get("summary")
                    or item.get("tóm_tắt_nội_dung")
                    or item.get("tóm tắt nội dung")
                    or item.get("tóm tắt")
                    or item.get("tóm_tắt")
                    or item.get("Tóm tắt")
                    or ""
                )
                if (
                    "http" in raw_summary
                    or "Tiêu đề" in raw_summary
                    or "Ngày" in raw_summary
                ):
                    raw_summary = ""  # loại bỏ nếu có dấu hiệu sai định dạng

                # Chuẩn hóa ngày phát hành thành chuỗi DD-MM-YYYY
                raw_date = (
                    item.get("date")
                    or item.get("ngày_phát_hành")
                    or item.get("ngày phát hành")
                    or item.get("Ngày phát hành")
                )
                if isinstance(raw_date, str):
                    try:
                        dt_obj = datetime.strptime(raw_date, "%d-%m-%Y")
                    except ValueError:
                        try:
                            dt_obj = datetime.strptime(raw_date, "%Y-%m-%d")
                        except ValueError:
                            dt_obj = datetime.now()
                elif isinstance(raw_date, datetime):
                    dt_obj = raw_date
                else:
                    dt_obj = datetime.now()
                ngay_phat_hanh = dt_obj.date()

                final_nganh_hang = (
                    IndustryType(industry_name)
                    if industry_name
                    else IndustryType.DAU_AN
                )
                article = Article(
                    stt=item.get("stt", i + 1),
                    ngay_phat_hanh=ngay_phat_hanh,
                    dau_bao=media_source.name,
                    cum_noi_dung=None,
                    cum_noi_dung_chi_tiet=item.get("tiêu đề")
                    or item.get("title")
                    or item.get("Tiêu đề")
                    or "Chưa rõ",
                    tom_tat_noi_dung=raw_summary.strip(),
                    link_bai_bao=resolved_link,
                    nganh_hang=final_nganh_hang,
                    nhan_hang=item.get("nhan_hang", []),
                    keywords_found=item.get("keywords_found", []),
                )
                articles.append(article)
            except Exception as e:
                logger.warning(
                    f"Skipping an item in JSON list due to parsing error: {e} | Item: {item}"
                )
        return articles

    def _infer_cluster(self, text: str) -> ContentCluster:
        """Tự suy đoán cụm nội dung dựa vào từ khóa."""
        lower = text.lower()
        if any(
            k in lower
            for k in ["sản xuất", "tuyển dụng", "nhà máy", "đầu tư", "hoạt động"]
        ):
            return ContentCluster.HOAT_DONG_DOANH_NGHIEP
        elif any(
            k in lower
            for k in ["tài trợ", "csr", "môi trường", "cộng đồng", "từ thiện"]
        ):
            return ContentCluster.CHUONG_TRINH_CSR
        elif any(
            k in lower
            for k in [
                "quảng cáo",
                "truyền thông",
                "KOL",
                "PR",
                "khuyến mãi",
                "chiến dịch",
            ]
        ):
            return ContentCluster.MARKETING_CAMPAIGN
        elif any(
            k in lower
            for k in ["ra mắt", "phát hành", "giới thiệu", "sản phẩm mới", "bao bì"]
        ):
            return ContentCluster.PRODUCT_LAUNCH
        elif any(k in lower for k in ["hợp tác", "liên doanh", "ký kết", "MOU"]):
            return ContentCluster.PARTNERSHIP
        elif any(
            k in lower
            for k in [
                "doanh thu",
                "lợi nhuận",
                "báo cáo tài chính",
                "kết quả kinh doanh",
            ]
        ):
            return ContentCluster.FINANCIAL_REPORT
        return ContentCluster.OTHER

    def _parse_text_articles_robust(
        self,
        content: str,
        media_source: MediaSource,
        industry_name: Optional[str] = None,
    ) -> List[Article]:
        """
        NÂNG CẤP: Phân tích text một cách thông minh hơn bằng regex.
        """

        if (
            "không tìm thấy bài viết nào" in content.lower()
            or "chưa có bài viết nào thỏa mãn điều kiện tìm kiếm" in content.lower()
        ):
            logger.warning("[Parser] Bỏ qua vì nội dung xác nhận không có bài viết.")
            return []

        articles = []
        # Tách content thành các khối, giả định mỗi khối là một bài báo
        article_blocks = re.split(r"\n---\n|\n\n+", content)

        # Pattern để tìm URL
        url_pattern = re.compile(r"https?://[^\s\)\<]+")

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
                cleaned_block = re.sub(
                    r"\s*\([^)]*\)?\s*\(" + re.escape(raw_link) + r"\)", "", block
                )
                cleaned_block = cleaned_block.replace(
                    f"({raw_link})", ""
                )  # Xóa nốt trường hợp chỉ có (url)

                # 3. Trích xuất các thông tin khác từ khối văn bản đã được làm sạch
                title_match = re.search(
                    r"(?:Tiêu đề|Title)\s*:\s*(.*)", cleaned_block, re.IGNORECASE
                )
                title = title_match.group(1).strip() if title_match else ""

                date_match = re.search(
                    r"(?:Ngày|Date)\s*:\s*(.*)", cleaned_block, re.IGNORECASE
                )

                if date_match:
                    date_str = date_match.group(1).strip()
                else:
                    continue

                # Phần còn lại của khối văn bản đã được làm sạch chính là tóm tắt
                summary_text = cleaned_block
                if title_match:
                    summary_text = summary_text.replace(title_match.group(0), "")
                if date_match:
                    summary_text = summary_text.replace(date_match.group(0), "")

                # Loại bỏ các dấu | thừa và làm sạch
                summary = re.sub(r"[|]+", " ", summary_text).strip()

                # Nếu vẫn quá ngắn hoặc rác → bỏ
                if (
                    len(summary.split()) < 5
                    or "Tiêu đề" in summary
                    or "Ngày" in summary
                ):
                    continue

                # Ngành hàng
                try:
                    final_nganh_hang = (
                        IndustryType(industry_name)
                        if industry_name
                        else IndustryType.DAU_AN
                    )
                except ValueError:
                    final_nganh_hang = IndustryType.DAU_AN

                article = Article(
                    stt=len(articles) + 1,
                    ngay_phat_hanh=date_str,
                    dau_bao=media_source.name,
                    cum_noi_dung=self._infer_cluster(summary),
                    tom_tat_noi_dung=summary,
                    link_bai_bao=resolved_link,
                    nganh_hang=final_nganh_hang,
                )
                articles.append(article)

            except Exception as e:
                logger.warning(
                    f"Could not create Article from text block '{block[:100]}...': {e}"
                )

        return articles
