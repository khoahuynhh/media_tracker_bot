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
import threading
import httpx
import re

from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from playwright.sync_api import sync_playwright
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

# Import Agno
from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.models.groq import Groq
from agno.tools.crawl4ai import Crawl4aiTools
from agno.tools.googlesearch import GoogleSearchTools
from ddgs import DDGS
from agno.tools.arxiv import ArxivTools
from agno.tools.baidusearch import BaiduSearchTools
from agno.tools.hackernews import HackerNewsTools
from agno.tools.pubmed import PubmedTools
from agno.tools.searxng import SearxngTools
from agno.tools.wikipedia import WikipediaTools

# Import modules
from openai import APITimeoutError
from .parsing import ArticleParser
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
)

logger = logging.getLogger(__name__)


def get_llm_model(provider: str, model_id: str) -> Any:
    """Factory function to get an LLM model instance."""
    if provider == "groq":
        return Groq(id=model_id)
    return OpenAIChat(id=model_id, timeout=90)


class CrawlerAgent:
    """Agent chuyên crawl web, sử dụng prompt tiếng Việt."""

    def __init__(self, model: Any, parser: ArticleParser, timeout: int = 10):
        self.parser = parser
        self.search_tools = [
            GoogleSearchTools(timeout),
            DDGS(),
            # ArxivTools(),
            # BaiduSearchTools(),
            # HackerNewsTools(),
            PubmedTools(),
            WikipediaTools(),
        ]
        self.search_tool_index = 0
        self.model = model
        self.agent = None
        self._create_agent()

    async def _check_pause(self):
        if hasattr(self, "pause_event"):
            while self.pause_event.is_set():
                logging.info("⏸ CrawlerAgent paused... waiting to resume.")
                await asyncio.sleep(1)

    async def _check_stop(self):
        if hasattr(self, "stop_event"):
            while self.stop_event.is_set():
                raise InterruptedError("Pipeline stopped by user.")

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
                "Nhiệm vụ: Crawl các website báo chí để tìm bài viết về các đối thủ cạnh tranh dựa trên keywords và ngành hàng.",
                "Ưu tiên tin tức mới nhất trong khoảng thời gian được chỉ định.",
                "Chỉ lấy các bài viết được đăng trong khoảng thời gian được yêu cầu.",
                "Nếu không tìm thấy bài viết thì để trống, đừng tự tạo nội dung hay format.",
                "Không lấy các bài viết đăng trước hoặc sau khoảng thời gian chỉ định.",
                "Trả về kết quả dạng JSON hợp lệ chứa danh sách bài báo với các trường: tiêu đề, ngày phát hành (DD-MM-YYYY), tóm tắt nội dung, link bài báo.",
            ],
            show_tool_calls=True,
            markdown=True,
            add_datetime_to_instructions=True,
        )

    def _rotate_tool(self):
        self.search_tool_index = (self.search_tool_index + 1) % len(self.search_tools)
        self._create_agent()

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

        max_keywords_per_query = 3
        keyword_groups = [
            keywords[i : i + max_keywords_per_query]
            for i in range(0, len(keywords), max_keywords_per_query)
        ]
        articles = []
        session_id = (
            f"crawler-{media_source.name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )

        def to_date(dt):
            return dt.date() if isinstance(dt, datetime) else dt

        try:
            for tool in self.search_tools:
                await self._check_stop()
                await self._check_pause()
                self._rotate_tool()
                tool_name = type(self.agent.tools[1]).__name__
                logger.info(f"[{media_source.name}] Đang dùng search tool: {tool_name}")

                new_articles_this_tool = 0
                for group in keyword_groups:
                    await self._check_stop()
                    await self._check_pause()
                    keywords_str = ", ".join(group)

                    crawl_query = f"""
                    Crawl website: {domain_url or media_source.name}
                    Tìm các bài báo có chứa các từ khóa: {keywords_str} và PHẢI liên quan đến ngành hàng: {industry_name}
                    Thời gian: {date_filter}
                    Yêu cầu:
                    - Trích xuất tiêu đề, tóm tắt, ngày đăng, link gốc.
                    - Chỉ lấy bài viết liên quan đến ngành hàng và từ khóa.
                    - Tạo tóm tắt chi tiết (dưới 100 từ), nêu bật các thông tin chính như: sự kiện chính, các bên liên quan, và kết quả hoặc tác động của sự kiện. Không chỉ lặp lại tiêu đề.
                    - Format: Tiêu đề | Ngày | Tóm tắt | Link
                    """

                    try:
                        logger.info(
                            f"[{media_source.name}] Searching with keywords: {keywords_str}"
                        )
                        response = await self.agent.arun(
                            crawl_query, session_id=session_id
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
                            valid_new_articles = [
                                a
                                for a in parsed_articles
                                if a
                                and a.link_bai_bao
                                and a.ngay_phat_hanh
                                and to_date(start_date)
                                <= to_date(a.ngay_phat_hanh)
                                <= to_date(end_date)
                            ]

                            if valid_new_articles:
                                articles.extend(valid_new_articles)
                                new_articles_this_tool += len(valid_new_articles)

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

            return CrawlResult(
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


class ProcessorAgent:
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
                2. Đọc kỹ nội dung bài viết, chỉ liệt kê các nhãn hàng trong `nhan_hang` nếu thực sự xuất hiện trong bài. Nếu không thấy nhãn hàng nào thì để `nhan_hang` là `[]`. Không tự bịa hoặc tự suy đoán thêm.
                3. Phân loại lại cụm nội dung (`cum_noi_dung`): Dựa trên tóm tắt nội dung và từ khóa tương ứng:
                {json.dumps({k.value: v for k, v in content_clusters.items()}, ensure_ascii=False, indent=2)}
                - Nếu không tìm thấy cụm nội dung nào khớp với danh sách từ khóa cụm nội dung, BẮT BUỘC gán trường (`cum_noi_dung`) là '{ContentCluster.OTHER}', KHÔNG ĐƯỢC để trống hoặc trả về none hay null.
                4. Viết lại nội dung chi tiết, ngắn gọn và mang tính mô tả khái quát cho trường `cum_noi_dung_chi_tiet` dựa trên nội dung đã có sẵn:
                    - Là 1 dòng mô tả ngắn (~10–20 từ) cho bài báo, có cấu trúc:  
                    `[Loại thông tin]: [Tóm tắt nội dung nổi bật]`
                    - Ví dụ: "Thông tin doanh nghiệp: Tường An khẳng định vị thế dịp Tết 2025"
                5. Trích xuất keywords tìm thấy trong bài.
                6. Chỉ giữ bài viết liên quan đến ngành FMCG (Dầu ăn, Gia vị, Sữa, v.v.) dựa trên từ khóa trong `keywords_config`. Loại bỏ bài không liên quan (e.g., chính trị, sức khỏe không liên quan).
                7. Định dạng ngày phát hành bắt buộc: dd/mm/yyyy (VD: 01/07/2025)"
                8. Nếu một bài báo đề cập nhiều nhãn hàng thì ghi tất cả nhãn hàng trong danh sách `nhan_hang`.
                9. Nếu bài liên quan nhiều ngành (ví dụ sản phẩm đa dụng), hãy chọn ngành chính nhất liên quan đến bối cảnh.

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
                        "nganh_hang": "Sữa",
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
                    response = await self.agent.arun(analysis_prompt)
                    if response and response.content:
                        try:
                            processed_data = json.loads(response.content)
                            for item in processed_data:
                                if item.get("cum_noi_dung") in [None, "null", ""]:
                                    item["cum_noi_dung"] = ContentCluster.OTHER
                            processed_articles.extend(
                                [Article(**item) for item in processed_data]
                            )
                            logger.info(
                                f"Batch {i // batch_size + 1} processed: {len(processed_data)} articles"
                            )
                        except (json.JSONDecodeError, TypeError) as e:
                            logger.error(
                                f"Failed to parse JSON response for batch {i // batch_size + 1}: {e}"
                            )
                            processed_articles.extend(
                                batch
                            )  # Keep raw articles if parsing fails
                    else:
                        logger.warning(
                            f"No valid response for batch {i // batch_size + 1}"
                        )
                        processed_articles.extend(
                            batch
                        )  # Keep raw articles if no response

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


class ReportAgent:
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
        """
        Cắt phần JSON thuần từ chuỗi LLM trả về, bỏ các markdown ```json ... ```
        """
        pattern = r"```json(.*?)```"
        match = re.search(pattern, text, re.DOTALL)
        if match:
            return match.group(1).strip()

        # Nếu không có ```json thì tìm cặp ngoặc { }
        match_brace = re.search(r"(\{.*\})", text, re.DOTALL)
        if match_brace:
            return match_brace.group(1).strip()

        raise ValueError("Không tìm thấy JSON trong phản hồi")

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
            - Danh sách bài báo: {json.dumps([a.model_dump(mode='json') for a in articles], ensure_ascii=False, indent=2)}

            Yêu cầu nhiệm vụ:
            1. Tạo 'overall_summary' (tóm tắt tổng quan), bao gồm ngành hàng, nhãn hàng, các đầu báo và số bài theo ngành.
            2. Tạo danh sách 'industry_summaries' (tóm tắt theo ngành), mỗi ngành là 1 mục, bao gồm: danh sách nhãn hàng, cụm nội dung, số lượng bài.
            3. Trả về danh sách 'articles' đã được xử lý (giữ nguyên input là được).
            4. Đảm bảo trả về đúng 1 đối tượng JSON duy nhất, không có markdown, không có giải thích ngoài lề.

            Trả về đúng 1 đối tượng JSON duy nhất với cấu trúc sau:
            CompetitorReport(
                overall_summary: { ... },
                industry_summaries: [ ... ],
                articles=[ ... ],
                total_articles={len(articles)},
                date_range="{date_range}"
            )

            Quy tắc:
                - Bắt đầu phản hồi bằng ký tự '{' và kết thúc bằng '}'.
                - Không thêm bất kỳ chú thích, markdown hay ký tự thừa nào ngoài JSON.
                - Trả về JSON thuần theo chuẩn RFC8259.
            """

            response = await self.agent.arun(report_prompt)
            logger.error(f"Raw LLM response: {response.content}")
            if response and response.content:
                try:
                    raw_json = self.extract_json(response.content)
                    return CompetitorReport(**json.loads(raw_json))
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
                or [ContentCluster.OTHER],
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


class MediaTrackerTeam:
    """Đội điều phối chính cho toàn bộ quy trình."""

    def __init__(
        self,
        config: CrawlConfig,
        start_date: datetime,
        end_date: datetime,
        stop_event: Optional[threading.Event] = None,
        pause_event=threading.Event,
    ):
        self.config = config
        self.stop_event = stop_event or threading.Event()
        self.pause_event = pause_event
        self.status = BotStatus()

        parser = ArticleParser()
        self.crawler = CrawlerAgent(get_llm_model("openai", "gpt-4o-mini"), parser)
        self.crawler.pause_event = self.pause_event
        self.crawler.stop_event = self.stop_event
        self.processor = ProcessorAgent(get_llm_model("openai", "gpt-4o"))
        self.reporter = ReportAgent(get_llm_model("openai", "gpt-4o"))
        self.start_date = start_date
        self.end_date = end_date

    async def _check_pause(self):
        while self.pause_event.is_set():
            logger.info("⏸ Pipeline paused... waiting to resume.")
            await asyncio.sleep(1)

    async def run_full_pipeline(self) -> Optional[CompetitorReport]:
        """
        Executes the full media tracking pipeline: crawling, processing, and report generation.
        Optimizes memory usage by limiting concurrent tasks and processing articles in batches.
        """
        self.status.is_running = True
        self.status.current_task = "Initializing pipeline"
        self.status.total_sources = len(self.config.media_sources)
        self.status.progress = 0.0
        all_articles = []

        try:
            # Step 1: Crawl data from media sources
            self.status.current_task = "Crawling data from media sources"
            logger.info(f"Starting crawl for {self.status.total_sources} sources.")

            # Limit concurrent crawl tasks using Semaphore
            semaphore = asyncio.Semaphore(self.config.max_concurrent_sources)

            async def bounded_crawl(media_source, industry_name, keywords):
                async with semaphore:
                    return await self.crawler.crawl_media_source(
                        media_source=media_source,
                        industry_name=industry_name,
                        keywords=keywords,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        # date_range_days=self.config.date_range_days,
                    )

            # Prepare all keywords
            # all_keywords = list(
            #     set(kw for kws in self.config.keywords.values() for kw in kws)
            # )
            tasks = []
            for industry_name, keywords in self.config.keywords.items():
                for media_source in self.config.media_sources:
                    tasks.append(bounded_crawl(media_source, industry_name, keywords))

            # Execute crawl tasks
            for i, task in enumerate(asyncio.as_completed(tasks)):
                await self._check_pause()
                if self.stop_event.is_set():
                    logger.warning("Pipeline stopped by user during crawling.")
                    raise InterruptedError("Pipeline stopped by user during crawling.")

                try:
                    result = await task
                    self.status.completed_sources += 1
                    self.status.progress = (
                        self.status.completed_sources / self.status.total_sources
                    ) * 50.0

                    if result.crawl_status == "success":
                        all_articles.extend(result.articles_found)
                        logger.info(
                            f"Crawled {len(result.articles_found)} articles from {result.source_name}"
                        )
                    else:
                        self.status.failed_sources += 1
                        logger.warning(
                            f"Crawl failed for {result.source_name}: {result.error_message}"
                        )

                    # Free memory after each source
                    gc.collect()

                except asyncio.CancelledError:
                    logger.warning(f"Crawl task {i} cancelled.")
                    self.status.failed_sources += 1
                except Exception as e:
                    self.status.failed_sources += 1
                    logger.error(
                        f"Unexpected error crawling source {i + 1}: {e}", exc_info=True
                    )

            logger.info(f"Crawling completed. Found {len(all_articles)} raw articles.")

            if self.stop_event.is_set():
                raise InterruptedError("Pipeline stopped by user after crawling.")

            # Step 2: Process articles in batches
            self.status.current_task = "Processing and analyzing articles"
            self.status.progress = 60.0
            batch_size = 10  # Process 20 articles per batch
            processed_articles = []
            for i in range(0, len(all_articles), batch_size):
                await self._check_pause()
                if self.stop_event.is_set():
                    raise InterruptedError("Pipeline stopped by user after processing.")
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
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.config.date_range_days)
            date_range_str = f"Từ ngày {start_date.strftime('%d/%m/%Y')} đến ngày {end_date.strftime('%d/%m/%Y')}"

            report = None
            for i in range(0, len(processed_articles), batch_size):
                await self._check_pause()
                if self.stop_event.is_set():
                    raise InterruptedError("Pipeline stopped by user after processing.")

                batch = processed_articles[i : i + batch_size]
                batch_report = await self.reporter.generate_report(
                    batch, date_range_str
                )
                if report is None:
                    report = batch_report
                else:
                    report.articles.extend(batch_report.articles)
                    report.industry_summaries.extend(batch_report.industry_summaries)
                    report.total_articles += batch_report.total_articles
                gc.collect()  # Free memory after each batch

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
            logger.warning(f"Pipeline stopped or cancelled: {e}")
            return None
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
            gc.collect()  # Final memory cleanup

    def get_status(self) -> BotStatus:
        return self.status
