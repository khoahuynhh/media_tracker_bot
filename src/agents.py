# src/agents.py
"""
Multi-Agent System for the Media Tracker Bot.
Phiên bản này khôi phục lại các prompt chi tiết của phiên bản gốc
để đảm bảo chất lượng phân tích, đồng thời giữ lại yêu cầu output dạng JSON
để hệ thống hoạt động bền bỉ.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import threading
import httpx

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.models.groq import Groq
from agno.tools.crawl4ai import Crawl4aiTools
from agno.tools.googlesearch import GoogleSearchTools
from agno.team import Team

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
from .parsing import ArticleParser

logger = logging.getLogger(__name__)


def get_llm_model(provider: str, model_id: str) -> Any:
    """Factory function to get an LLM model instance."""
    if provider == "groq":
        return Groq(id=model_id)
    return OpenAIChat(id=model_id)


class CrawlerAgent:
    """Agent chuyên crawl web, sử dụng prompt tiếng Việt."""

    def __init__(self, model: Any, parser: ArticleParser):
        self.agent = Agent(
            name="MediaCrawler",
            role="Web Crawler và Content Extractor",
            model=model,
            tools=[Crawl4aiTools(max_length=2000), GoogleSearchTools()],
            instructions=[
                "Bạn là một chuyên gia crawl web để theo dõi truyền thông tại Việt Nam.",
                "Nhiệm vụ: Crawl các website báo chí để tìm bài viết về các đối thủ cạnh tranh dựa trên keywords.",
                "Ưu tiên tin tức mới nhất trong khoảng thời gian được chỉ định.",
                "Trả về kết quả dưới dạng text có cấu trúc rõ ràng.",
            ],
            show_tool_calls=True,
            markdown=True,
            add_datetime_to_instructions=True,
        )
        self.parser = parser
    
    async def _validate_article_links(self, articles: List[Article]) -> List[Article]:
        """
        Kiểm tra đồng thời các URL để đảm bảo chúng có thể truy cập.
        Sử dụng HEAD request để tăng hiệu suất.
        """
        async def check_url(article: Article, client: httpx.AsyncClient):
            try:
                # Sử dụng HEAD request để chỉ lấy headers, không tải toàn bộ trang
                response = await client.head(article.link_bai_bao, follow_redirects=True, timeout=10)
                # Chấp nhận các status code 2xx (thành công) và 3xx (chuyển hướng)
                if 200 <= response.status_code < 400:
                    return article
                else:
                    logger.warning(f"Link trả về lỗi {response.status_code}: {article.link_bai_bao}")
                    return None
            except (httpx.RequestError, httpx.TimeoutException) as e:
                logger.warning(f"Không thể xác thực link {article.link_bai_bao}: {e}")
                return None

        async with httpx.AsyncClient() as client:
            tasks = [check_url(article, client) for article in articles]
            results = await asyncio.gather(*tasks)
        
        # Lọc ra các bài báo có link hợp lệ
        valid_articles = [article for article in results if article is not None]
        logger.info(f"Xác thực link: {len(valid_articles)} trên tổng số {len(articles)} link có thể truy cập.")
        return valid_articles

    async def crawl_media_source(
        self,
        media_source: MediaSource,
        keywords: List[str],
        date_range_days: int = 30,
    ) -> CrawlResult:
        """Thực hiện crawl một nguồn media với prompt tiếng Việt."""
        start_time = datetime.now()
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=date_range_days)
            date_filter = f"từ ngày {start_date.strftime('%Y-%m-%d')} đến ngày {end_date.strftime('%Y-%m-%d')}"

            domain_url = media_source.domain
            if domain_url and not domain_url.startswith("http"):
                domain_url = f"https://{domain_url}"

            keywords_str = ", ".join(keywords[:5])

            # KHÔI PHỤC PROMPT GỐC: Yêu cầu output theo format text cụ thể
            crawl_query = f"""
            Crawl website: {domain_url or media_source.name}
            Tìm các bài báo có chứa từ khóa: {keywords_str}
            Thời gian: {date_filter}
            Yêu cầu:
            - Trích xuất tiêu đề, tóm tắt, ngày đăng, link gốc.
            - Chỉ lấy bài viết liên quan đến keywords.
            - Format: Tiêu đề | Ngày | Tóm tắt | Link
            - Nếu bạn không tự tin khi trả về JSON có cấu trúc đầy đủ, thì chỉ cần trả về văn bản thuần túy với tên trường rõ ràng như:
                Tiêu đề: ...
                Ngày: ...
                Tóm tắt: ...
                Link: ...
            """

            session_id = (
                f"crawler-{media_source.name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            )
            response = await self.agent.arun(crawl_query, session_id=session_id)
            duration = (datetime.now() - start_time).total_seconds()

            if response and response.content:
                # Trình phân tích mới sẽ tự động xử lý cả JSON và text
                # Bước 1: Phân tích text thô
                articles = self.parser.parse(response.content, media_source)
                
                # Bước 2: Xác thực các link đã phân tích
                if articles:
                    validated_articles = await self._validate_article_links(articles)
                else:
                    validated_articles = []
                
                return CrawlResult(
                    source_name=media_source.name,
                    source_type=media_source.type,
                    url=media_source.domain,
                    articles_found=validated_articles,
                    crawl_status="success",
                    crawl_duration=duration,
                )
            else:
                return CrawlResult(
                    source_name=media_source.name,
                    source_type=media_source.type,
                    url=media_source.domain,
                    articles_found=[],
                    crawl_status="failed",
                    error_message="Không có phản hồi từ crawler",
                    crawl_duration=duration,
                )
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Crawl thất bại cho {media_source.name}: {e}", exc_info=True)
            return CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url=media_source.domain,
                articles_found=[],
                crawl_status="failed",
                error_message=str(e),
                crawl_duration=duration,
            )


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
        if not raw_articles:
            return []
        try:
            brand_list = list(
                set(
                    kw
                    for kws in keywords_config.values()
                    for kw in kws
                    if kw[0].isupper()
                )
            )

            # KHÔI PHỤC PROMPT GỐC: Đưa lại toàn bộ logic phân loại chi tiết của bạn
            analysis_prompt = f"""
            Phân tích và phân loại {len(raw_articles)} bài báo sau đây.

            Danh sách các nhãn hàng đối thủ cần xác định:
            {json.dumps(brand_list, ensure_ascii=False, indent=2)}
            
            Keywords config:
            {json.dumps(keywords_config, ensure_ascii=False, indent=2)}
            
            Raw articles:
            {json.dumps([a.model_dump(mode='json') for a in raw_articles], ensure_ascii=False, indent=2)}
            
            Yêu cầu:
            1. Phân loại chính xác ngành hàng cho từng bài (dựa theo bối cảnh bài và danh sách nhãn hàng ngành hàng tương ứng).
            2. Với mỗi bài viết, xác định các nhãn hàng competitors nào được đề cập trong nội dung từ danh sách các nhãn hàng.
            3. Phân loại nội dung bài viết vào 1 trong các nhóm sau:
                    - `Hoạt động doanh nghiệp`: sản xuất, nhà máy, tuyển dụng, tổ chức nội bộ...
                    - `Chương trình CSR`: tài trợ, môi trường, cộng đồng...
                    - `Chiến dịch Marketing`: truyền thông, KOLs, khuyến mãi...
                    - `Ra mắt sản phẩm`: giới thiệu sản phẩm mới, bao bì, công thức...
                    - `Hợp tác`: MOU, liên doanh, ký kết...
                    - `Báo cáo tài chính`: kết quả kinh doanh, lợi nhuận, tăng trưởng...
                    - `Khác`: không thuộc các mục trên.
            4. Trích xuất keywords tìm thấy trong bài.
            5. Cải thiện tóm tắt nội dung cho súc tích, đầy đủ.
            6. Loại bỏ các bài không liên quan (ví dụ: chỉ đề cập thương hiệu mà không nói về sản phẩm ngành liên quan).

            Định dạng đầu ra:
            Trả về một danh sách JSON hợp lệ chứa các đối tượng Article đã được xử lý. Cấu trúc JSON của mỗi đối tượng phải khớp với Pydantic model:
            Cấu trúc JSON nên khớp với schema sau:
            [
            {
                "stt": 1,
                "ngay_phat_hanh": "2025-07-01",
                "dau_bao": "VNEXPRESS",
                "cum_noi_dung": "Chiến dịch Marketing",
                "tom_tat_noi_dung": "Vinamilk tung chiến dịch Tết 2025...",
                "link_bai_bao": "https://vnexpress.net/...",
                "nganh_hang": "Sữa",
                "nhan_hang": ["Vinamilk"],
                "keywords_found": ["Tết", "TV quảng cáo", "Vinamilk"]
            }
            ]
            Nếu bạn không chắc chắn trả về đúng JSON, hãy viết từng bài báo như sau:
                Tiêu đề: ...
                Link: ...
                Tóm tắt: ...
                Ngày: ...
                Ngành hàng: ...
                Nhãn hàng: ...
                Cụm nội dung: ...
                Keywords: ...
            ---
            """
            response = await self.agent.arun(analysis_prompt)
            if response and response.content:
                try:
                    processed_data = json.loads(response.content)
                    return [Article(**item) for item in processed_data]
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(
                        f"Không thể phân tích phản hồi từ Processor dưới dạng JSON: {e}"
                    )
                    return raw_articles
            return raw_articles
        except Exception as e:
            logger.error(f"Quá trình xử lý thất bại: {e}", exc_info=True)
            return raw_articles


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
            ],
            markdown=True,
        )

    async def generate_report(
        self, articles: List[Article], date_range: str
    ) -> CompetitorReport:
        try:
            report_prompt = f"""
            Tạo một báo cáo phân tích đối thủ cạnh tranh từ {len(articles)} bài báo sau đây cho khoảng thời gian: {date_range}.
            
            Dữ liệu đầu vào:
            - Danh sách bài báo: {json.dumps([a.model_dump(mode='json') for a in articles], ensure_ascii=False, indent=2)}

            Yêu cầu nhiệm vụ:
            1. Tạo một 'overall_summary' (tóm tắt tổng quan) với các thống kê chung.
            2. Tạo một danh sách 'industry_summaries' (tóm tắt theo ngành), mỗi ngành một mục.
            3. Bao gồm các phân tích về đối thủ cạnh tranh hàng đầu, các nhận định về thị trường và xu hướng.
            
            Định dạng đầu ra:
            Trả về một đối tượng JSON duy nhất, hợp lệ và tuân thủ nghiêm ngặt cấu trúc của model 
            CompetitorReport(
            overall_summary=overall_summary,
            industry_summaries=industry_summaries,
            articles=articles,
            total_articles=len(articles),
            date_range=date_range,).
            """
            response = await self.agent.arun(report_prompt)
            if response and response.content:
                try:
                    return CompetitorReport(**json.loads(response.content))
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(
                        f"Không thể phân tích phản hồi từ ReportAgent dưới dạng JSON: {e}. Đang tạo báo cáo cơ bản."
                    )
                    return self._create_basic_report(articles, date_range)
            return self._create_basic_report(articles, date_range)
        except Exception as e:
            logger.error(f"Tạo báo cáo thất bại: {e}", exc_info=True)
            return self._create_basic_report(articles, date_range)

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
                    set(article.cum_noi_dung for article in industry_articles)
                ),
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
        self, config: CrawlConfig, stop_event: Optional[threading.Event] = None
    ):
        self.config = config
        self.stop_event = stop_event or threading.Event()
        self.status = BotStatus()

        parser = ArticleParser()
        self.crawler = CrawlerAgent(get_llm_model("openai", "gpt-4o-mini"), parser)
        self.processor = ProcessorAgent(get_llm_model("openai", "gpt-4o"))
        self.reporter = ReportAgent(get_llm_model("openai", "gpt-4o"))

    async def run_full_pipeline(self) -> Optional[CompetitorReport]:
        self.status.is_running = True
        self.status.current_task = "Khởi tạo quy trình"
        self.status.total_sources = len(self.config.media_sources)
        all_articles = []

        try:
            self.status.current_task = "Đang thu thập dữ liệu từ các nguồn"
            logger.info(f"Bắt đầu crawl {self.status.total_sources} nguồn.")

            tasks = []
            all_keywords = list(
                set(kw for kws in self.config.keywords.values() for kw in kws)
            )
            for media_source in self.config.media_sources:
                if self.stop_event.is_set():
                    raise InterruptedError(
                        "Quy trình bị dừng bởi người dùng trong lúc thiết lập."
                    )

                task = self.crawler.crawl_media_source(
                    media_source=media_source,
                    keywords=all_keywords,
                    date_range_days=self.config.date_range_days,
                )
                tasks.append(task)

            for f in asyncio.as_completed(tasks):
                if self.stop_event.is_set():
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    raise InterruptedError(
                        "Quy trình bị dừng bởi người dùng trong lúc crawl."
                    )

                result = await f
                self.status.completed_sources += 1
                self.status.progress = (
                    self.status.completed_sources / self.status.total_sources * 50
                )

                if result.crawl_status == "success":
                    all_articles.extend(result.articles_found)
                else:
                    self.status.failed_sources += 1
                    logger.warning(
                        f"Crawl thất bại {result.source_name}: {result.error_message}"
                    )

            logger.info(f"Thu thập hoàn tất. Tìm thấy {len(all_articles)} bài báo thô.")

            if self.stop_event.is_set():
                raise InterruptedError(
                    "Quy trình bị dừng bởi người dùng sau khi crawl."
                )

            self.status.current_task = "Đang xử lý và phân tích bài báo"
            self.status.progress = 60
            processed_articles = await self.processor.process_articles(
                all_articles, self.config.keywords
            )
            logger.info(f"Xử lý hoàn tất. Còn lại {len(processed_articles)} bài báo.")
            self.status.progress = 80

            if self.stop_event.is_set():
                raise InterruptedError(
                    "Quy trình bị dừng bởi người dùng sau khi xử lý."
                )

            self.status.current_task = "Đang tạo báo cáo tổng hợp"
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.config.date_range_days)
            date_range_str = f"Từ ngày {start_date.strftime('%d/%m/%Y')} đến ngày {end_date.strftime('%d/%m/%Y')}"

            report = await self.reporter.generate_report(
                processed_articles, date_range_str
            )
            self.status.progress = 100
            self.status.current_task = "Hoàn thành"
            logger.info("Quy trình đã hoàn tất thành công.")
            return report

        except (InterruptedError, asyncio.CancelledError) as e:
            self.status.current_task = f"Đã dừng: {e}"
            logger.warning(f"Quy trình đã bị dừng hoặc hủy: {e}")
            return None
        except Exception as e:
            self.status.current_task = f"Thất bại: {e}"
            logger.error(f"Quy trình thất bại nghiêm trọng: {e}", exc_info=True)
            return None
        finally:
            self.status.is_running = False
            self.status.last_run = datetime.now()

    def get_status(self) -> BotStatus:
        return self.status
