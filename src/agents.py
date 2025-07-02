# src/agents.py
"""
Multi-Agent System for the Media Tracker Bot.
Refactored to use dependency injection and a dedicated parsing module.
This version uses Vietnamese prompts for better localization and context understanding.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import threading

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.models.groq import Groq
from agno.tools.crawl4ai import Crawl4aiTools
from agno.tools.googlesearch import GoogleSearchTools
from agno.team import Team

from models import (
    MediaSource, Article, IndustrySummary, OverallSummary,
    CompetitorReport, CrawlResult, CrawlConfig, BotStatus, ContentCluster
)
from parsing import ArticleParser

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
            role="Chuyên gia thu thập và trích xuất nội dung web",
            model=model,
            tools=[Crawl4aiTools(max_length=2000), GoogleSearchTools()],
            instructions=[
                "Bạn là một chuyên gia crawl web để theo dõi truyền thông tại Việt Nam.",
                "Nhiệm vụ: Crawl các website báo chí để tìm bài viết về các đối thủ cạnh tranh dựa trên keywords.",
                "Ưu tiên các tin tức mới nhất trong khoảng thời gian được chỉ định.",
                "Tập trung vào việc crawl trực tiếp website thay vì dùng máy tìm kiếm.",
                # YÊU CẦU QUAN TRỌNG: Bắt buộc output là JSON để đảm bảo sự ổn định.
                "Bạn BẮT BUỘC phải trả về kết quả dưới dạng một danh sách JSON (JSON list) hợp lệ. Mỗi bài báo trong danh sách là một đối tượng JSON với các key: 'title', 'summary', 'date', 'link'."
            ],
            show_tool_calls=True,
            markdown=True,
            add_datetime_to_instructions=True,
        )
        self.parser = parser

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
            
            crawl_query = (
                f"Crawl trang web: {domain_url or media_source.name}\n"
                f"Tìm các bài báo chứa từ khóa: {keywords_str}\n"
                f"Khoảng thời gian: {date_filter}\n"
                "Yêu cầu: Trích xuất tiêu đề, tóm tắt, ngày đăng, và link gốc cho mỗi bài báo tìm thấy."
            )

            session_id = f"crawler-{media_source.name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            response = await self.agent.arun(crawl_query, session_id=session_id)
            duration = (datetime.now() - start_time).total_seconds()

            if response and response.content:
                articles = self.parser.parse(response.content, media_source)
                return CrawlResult(
                    source_name=media_source.name, source_type=media_source.type, url=media_source.domain,
                    articles_found=articles, crawl_status="success", crawl_duration=duration
                )
            else:
                return CrawlResult(
                    source_name=media_source.name, source_type=media_source.type, url=media_source.domain,
                    articles_found=[], crawl_status="failed", error_message="Không có phản hồi từ crawler",
                    crawl_duration=duration
                )
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Crawl thất bại cho {media_source.name}: {e}", exc_info=True)
            return CrawlResult(
                source_name=media_source.name, source_type=media_source.type, url=media_source.domain,
                articles_found=[], crawl_status="failed", error_message=str(e), crawl_duration=duration
            )

class ProcessorAgent:
    """Agent chuyên xử lý và phân tích nội dung, sử dụng prompt tiếng Việt."""
    def __init__(self, model: Any):
        self.agent = Agent(
            name="ContentProcessor",
            role="Chuyên gia Phân tích và Phân loại Nội dung",
            model=model,
            instructions=[
                "Bạn là chuyên gia phân tích nội dung truyền thông cho ngành FMCG tại Việt Nam.",
                "Nhiệm vụ của bạn: Phân tích và phân loại các bài báo theo ngành hàng và nhãn hiệu.",
                "Xác định chính xác: Ngành hàng, Nhãn hàng, và Cụm nội dung chính.",
                # YÊU CẦU QUAN TRỌNG: Bắt buộc output là JSON để đảm bảo sự ổn định.
                "Bạn BẮT BUỘC phải trả về kết quả dưới dạng một danh sách JSON (JSON list) hợp lệ của các đối tượng Article đã được xử lý đầy đủ."
            ],
            markdown=True,
        )

    async def process_articles(self, raw_articles: List[Article], keywords_config: Dict[str, List[str]]) -> List[Article]:
        if not raw_articles:
            return []
        try:
            analysis_prompt = f"""
            Phân tích và phân loại {len(raw_articles)} bài báo dưới đây dựa trên danh sách từ khóa và hướng dẫn phân loại.
            
            **Hướng dẫn phân loại 'cum_noi_dung' (Cụm nội dung):**
            - **"{ContentCluster.HOAT_DONG_DOANH_NGHIEP.value}"**: Các bài viết về hoạt động sản xuất, vận hành, mở rộng nhà máy, tuyển dụng, tổ chức nội bộ.
            - **"{ContentCluster.CHUONG_TRINH_CSR.value}"**: Các bài viết về hoạt động vì cộng đồng, tài trợ học bổng, từ thiện, bảo vệ môi trường.
            - **"{ContentCluster.MARKETING_CAMPAIGN.value}"**: Các bài viết về chiến dịch quảng bá, truyền thông, KOLs, sự kiện, khuyến mãi.
            - **"{ContentCluster.PRODUCT_LAUNCH.value}"**: Các bài viết giới thiệu sản phẩm mới, cải tiến sản phẩm, bao bì mới.
            - **"{ContentCluster.PARTNERSHIP.value}"**: Các bài viết về hợp đồng hợp tác, ký kết MOU, liên doanh.
            - **"{ContentCluster.FINANCIAL_REPORT.value}"**: Các bài viết về kết quả kinh doanh, lợi nhuận, chi phí, tăng trưởng.
            - **"{ContentCluster.OTHER.value}"**: CHỈ sử dụng mục này khi bài viết thực sự không thuộc bất kỳ loại nào ở trên.

            **Yêu cầu nhiệm vụ:**
            1. Với mỗi bài báo, xác định chính xác 'nganh_hang' (ngành hàng) dựa trên ngữ cảnh và từ khóa.
            2. Xác định tất cả các nhãn hàng đối thủ được đề cập từ danh sách từ khóa.
            3. Phân loại mỗi bài báo vào một trong các cụm nội dung đã được hướng dẫn ở trên.
            4. Tinh chỉnh lại 'tom_tat_noi_dung' (tóm tắt) để trở nên súc tích và đầy đủ thông tin.
            5. Loại bỏ bất kỳ bài báo nào không liên quan đến các ngành hàng đã chỉ định, ngay cả khi tên thương hiệu được đề cập ngoài ngữ cảnh.

            **Dữ liệu đầu vào:**
            - Cấu hình từ khóa: {json.dumps(keywords_config, ensure_ascii=False, indent=2)}
            - Danh sách bài báo thô: {json.dumps([a.model_dump(mode='json') for a in raw_articles], ensure_ascii=False, indent=2)}

            **Định dạng đầu ra:**
            Trả về một danh sách JSON hợp lệ chứa các đối tượng Article đã được xử lý. Cấu trúc JSON của mỗi đối tượng phải khớp với Pydantic model.
            """
            response = await self.agent.arun(analysis_prompt)
            if response and response.content:
                try:
                    processed_data = json.loads(response.content)
                    return [Article(**item) for item in processed_data]
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(f"Không thể phân tích phản hồi từ Processor dưới dạng JSON: {e}")
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
                # YÊU CẦU QUAN TRỌNG: Bắt buộc output là JSON để đảm bảo sự ổn định.
                "Bạn BẮT BUỘC phải trả về kết quả dưới dạng một đối tượng JSON (JSON object) duy nhất, hợp lệ và tuân thủ nghiêm ngặt theo cấu trúc của Pydantic model 'CompetitorReport'."
            ],
            markdown=True,
        )

    async def generate_report(self, articles: List[Article], date_range: str) -> CompetitorReport:
        try:
            report_prompt = f"""
            Tạo một báo cáo phân tích đối thủ cạnh tranh từ {len(articles)} bài báo sau đây cho khoảng thời gian: {date_range}.
            
            **Dữ liệu đầu vào:**
            - Danh sách bài báo: {json.dumps([a.model_dump(mode='json') for a in articles], ensure_ascii=False, indent=2)}

            **Yêu cầu nhiệm vụ:**
            1. Tạo một 'overall_summary' (tóm tắt tổng quan) với các thống kê chung.
            2. Tạo một danh sách 'industry_summaries' (tóm tắt theo ngành), mỗi ngành một mục.
            3. Bao gồm các phân tích về đối thủ cạnh tranh hàng đầu, các nhận định về thị trường và xu hướng.
            
            **Định dạng đầu ra:**
            Trả về một đối tượng JSON duy nhất, hợp lệ và tuân thủ nghiêm ngặt cấu trúc của model CompetitorReport.
            """
            response = await self.agent.arun(report_prompt)
            if response and response.content:
                try:
                    return CompetitorReport(**json.loads(response.content))
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error(f"Không thể phân tích phản hồi từ ReportAgent dưới dạng JSON: {e}. Đang tạo báo cáo cơ bản.")
                    return self._create_basic_report(articles, date_range)
            return self._create_basic_report(articles, date_range)
        except Exception as e:
            logger.error(f"Tạo báo cáo thất bại: {e}", exc_info=True)
            return self._create_basic_report(articles, date_range)

    def _create_basic_report(self, articles: List[Article], date_range: str) -> CompetitorReport:
        # Phương thức dự phòng khi LLM thất bại
        industry_groups = {}
        for article in articles:
            industry = article.nganh_hang
            if industry not in industry_groups:
                industry_groups[industry] = []
            industry_groups[industry].append(article)

        industry_summaries = [
            IndustrySummary(
                nganh_hang=industry,
                nhan_hang=list(set(brand for article in industry_articles for brand in article.nhan_hang)),
                cum_noi_dung=list(set(article.cum_noi_dung for article in industry_articles)),
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

    def __init__(self, config: CrawlConfig, stop_event: Optional[threading.Event] = None):
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
            # Giai đoạn 1: Thu thập dữ liệu (Crawling)
            self.status.current_task = "Đang thu thập dữ liệu từ các nguồn"
            logger.info(f"Bắt đầu crawl {self.status.total_sources} nguồn.")
            
            tasks = []
            all_keywords = list(set(kw for kws in self.config.keywords.values() for kw in kws))
            for media_source in self.config.media_sources:
                if self.stop_event.is_set():
                    raise InterruptedError("Quy trình bị dừng bởi người dùng trong lúc thiết lập.")
                
                task = self.crawler.crawl_media_source(
                    media_source=media_source,
                    keywords=all_keywords,
                    date_range_days=self.config.date_range_days
                )
                tasks.append(task)
            
            for f in asyncio.as_completed(tasks):
                if self.stop_event.is_set():
                    for t in tasks:
                        if not t.done():
                            t.cancel()
                    raise InterruptedError("Quy trình bị dừng bởi người dùng trong lúc crawl.")
                
                result = await f
                self.status.completed_sources += 1
                self.status.progress = self.status.completed_sources / self.status.total_sources * 50

                if result.crawl_status == "success":
                    all_articles.extend(result.articles_found)
                else:
                    self.status.failed_sources += 1
                    logger.warning(f"Crawl thất bại {result.source_name}: {result.error_message}")

            logger.info(f"Thu thập hoàn tất. Tìm thấy {len(all_articles)} bài báo thô.")

            if self.stop_event.is_set():
                raise InterruptedError("Quy trình bị dừng bởi người dùng sau khi crawl.")

            # Giai đoạn 2: Xử lý và phân tích
            self.status.current_task = "Đang xử lý và phân tích bài báo"
            self.status.progress = 60
            processed_articles = await self.processor.process_articles(all_articles, self.config.keywords)
            logger.info(f"Xử lý hoàn tất. Còn lại {len(processed_articles)} bài báo.")
            self.status.progress = 80

            if self.stop_event.is_set():
                raise InterruptedError("Quy trình bị dừng bởi người dùng sau khi xử lý.")

            # Giai đoạn 3: Tạo báo cáo
            self.status.current_task = "Đang tạo báo cáo tổng hợp"
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.config.date_range_days)
            date_range_str = f"Từ ngày {start_date.strftime('%d/%m/%Y')} đến ngày {end_date.strftime('%d/%m/%Y')}"
            
            report = await self.reporter.generate_report(processed_articles, date_range_str)
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
