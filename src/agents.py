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
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import threading
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

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
from agno.team import Team
from openai import APITimeoutError
import httpx

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
    return OpenAIChat(id=model_id, timeout=90)


class CrawlerAgent:
    """Agent chuyên crawl web, sử dụng prompt tiếng Việt."""

    def __init__(self, model: Any, parser: ArticleParser, timeout: int = 10):
        self.parser = parser
        self.search_tools = [
            GoogleSearchTools(),
            DDGS(),
            ArxivTools(),
            BaiduSearchTools(),
            HackerNewsTools(),
            PubmedTools(),
            # SearxngTools(
            #     host="http://localhost:8888",
            #     engines=["google", "bing"],
            #     fixed_max_results=5,
            #     news=True,
            #     science=True),
            WikipediaTools(),
        ]
        self.search_tool_index = 0  # Luân phiên
        self.model = model
        self.agent = None
        self._create_agent()

    def _create_agent(self):
        if self.agent:
            del self.agent  # avoid potential lingering transport
        current_tool = self.search_tools[self.search_tool_index]

        self.agent = Agent(
            name="MediaCrawler",
            role="Web Crawler và Content Extractor",
            model=self.model,
            tools=[Crawl4aiTools(max_length=2000), current_tool],
            instructions=[
                "Bạn là một chuyên gia crawl web để theo dõi truyền thông tại Việt Nam.",
                "Nhiệm vụ: Crawl các website báo chí để tìm bài viết về các đối thủ cạnh tranh dựa trên keywords.",
                "Ưu tiên tin tức mới nhất trong khoảng thời gian được chỉ định.",
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
        keywords: List[str],
        date_range_days: int = 30,
    ) -> CrawlResult:
        start_time = datetime.now()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=date_range_days)
        date_filter = f"từ ngày {start_date.strftime('%Y-%m-%d')} đến ngày {end_date.strftime('%Y-%m-%d')}"
        domain_url = media_source.domain
        if domain_url and not domain_url.startswith("http"):
            domain_url = f"https://{domain_url}"

        max_keywords_per_query = 3  # Limit to 3 keywords per query
        keyword_groups = [
            keywords[i : i + max_keywords_per_query]
            for i in range(0, len(keywords), max_keywords_per_query)
        ]
        articles = []
        session_id = (
            f"crawler-{media_source.name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )

        try:
            for tool in self.search_tools:
                self._rotate_tool()
                tool_name = type(self.agent.tools[1]).__name__
                logger.info(f"[{media_source.name}] Đang dùng search tool: {tool_name}")

                for group in keyword_groups:
                    keywords_str = ", ".join(group)
                    crawl_query = f"""
                    Crawl website: {domain_url or media_source.name}
                    Tìm các bài báo có chứa từ khóa: {keywords_str}
                    Thời gian: {date_filter}
                    Yêu cầu:
                    - Trích xuất tiêu đề, tóm tắt, ngày đăng, link gốc.
                    - Chỉ lấy bài viết liên quan đến ngành FMCG và từ khóa.
                    - Format: Tiêu đề | Ngày | Tóm tắt | Link
                    - Ngày phải là ngày xuất hiện trong nội dung bài báo.
                    - Cố gắng trích xuất ngày từ nội dung bài báo. Ưu tiên dòng chứa ngày rõ ràng như Ngày: hoặc Published: hoặc trong đoạn đầu bài viết. Nếu không tìm được ngày rõ ràng, vẫn giữ bài báo.
                    """

                    try:
                        logger.info(
                            f"[{media_source.name}] Searching with keywords: {keywords_str}"
                        )
                        response = await self.agent.arun(
                            crawl_query, session_id=session_id
                        )
                        await asyncio.sleep(1)  # Avoid overwhelming the API

                        if response and response.content:
                            # Check for blocked content or captcha
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
                            if parsed_articles:
                                articles.extend(parsed_articles)
                                logger.info(
                                    f"[{media_source.name}] Found {len(parsed_articles)} articles with {tool_name} for keywords: {keywords_str}"
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

                    # Free memory after each keyword group
                    gc.collect()

                # Stop if articles are found to avoid redundant searches
                if articles:
                    logger.info(
                        f"[{media_source.name}] Stopping search as articles were found"
                    )
                    break

            # Remove duplicates based on article URL
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
            gc.collect()  # Final memory cleanup


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
        batch_size = 20  # Process 20 articles per batch to reduce memory usage

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
                2. Với mỗi bài viết, xác định các nhãn hàng competitors nào được đề cập trong nội dung từ danh sách các nhãn hàng.
                3. Phân loại cụm nội dung (`cum_noi_dung`): Dựa trên nội dung bài viết và từ khóa tương ứng:
                {json.dumps({k.value: v for k, v in content_clusters.items()}, ensure_ascii=False, indent=2)}
                - Nếu không khớp với cụm nào, chọn '{ContentCluster.OTHER}'.
                4. Trích xuất keywords tìm thấy trong bài.
                5. Tóm tắt nội dung (`tom_tat_noi_dung`): Tạo tóm tắt ngắn gọn (dưới 100 từ), rõ ràng, tập trung vào nội dung FMCG.
                6. Chỉ giữ bài viết liên quan đến ngành FMCG (Dầu ăn, Gia vị, Sữa, v.v.) dựa trên từ khóa trong `keywords_config`. Loại bỏ bài không liên quan (e.g., chính trị, sức khỏe không liên quan).
                7. Ngày đăng (`ngay_phat_hanh`): Đảm bảo định dạng DD-MM-YYYY. Nếu không có, để trống.

                Định dạng đầu ra:
                Trả về một danh sách JSON hợp lệ chứa các đối tượng Article đã được xử lý. Cấu trúc JSON của mỗi đối tượng phải khớp với Pydantic model:
                [
                    {{
                        "stt": 1,
                        "ngay_phat_hanh": "2025-07-01",
                        "dau_bao": "VNEXPRESS",
                        "cum_noi_dung": "Chiến dịch Marketing",
                        "tom_tat_noi_dung": "Vinamilk tung chiến dịch Tết 2025...",
                        "link_bai_bao": "https://vnexpress.net/...",
                        "nganh_hang": "Sữa",
                        "nhan_hang": ["Vinamilk"],
                        "keywords_found": ["Tết", "TV quảng cáo", "Vinamilk"]
                    }}
                ]
                """

                try:
                    response = await self.agent.arun(analysis_prompt)
                    if response and response.content:
                        try:
                            processed_data = json.loads(response.content)
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
            ],
            markdown=True,
        )

    async def generate_report(
        self, articles: List[Article], date_range: str
    ) -> CompetitorReport:
        """
        Generates a competitor analysis report from a list of articles for a given date range.
        Optimizes memory usage by processing articles in batches and freeing memory after each batch.

        Args:
            articles: List of processed Article objects.
            date_range: String representing the date range (e.g., "Từ ngày 01/07/2025 đến ngày 07/07/2025").

        Returns:
            A CompetitorReport object containing the analysis.
        """
        if not articles:
            logger.info("No articles provided for report generation.")
            return self._create_basic_report([], date_range)

        batch_size = 20  # Process 20 articles per batch to reduce memory usage
        report = None
        industry_summaries = []
        processed_articles = []

        try:
            # Process articles in batches
            for i in range(0, len(articles), batch_size):
                batch = articles[i : i + batch_size]
                logger.info(
                    f"Generating report for batch {i // batch_size + 1} with {len(batch)} articles"
                )

                # Create prompt for the batch
                report_prompt = f"""
                Tạo một báo cáo phân tích đối thủ cạnh tranh từ {len(batch)} bài báo sau đây cho khoảng thời gian: {date_range}.
                
                Dữ liệu đầu vào:
                - Danh sách bài báo: {json.dumps([a.model_dump(mode='json') for a in batch], ensure_ascii=False, indent=2)}

                Yêu cầu nhiệm vụ:
                1. Tạo một 'overall_summary' (tóm tắt tổng quan) với các thống kê chung cho lô bài báo này.
                2. Tạo một danh sách 'industry_summaries' (tóm tắt theo ngành), mỗi ngành một mục.
                3. Bao gồm các phân tích về đối thủ cạnh tranh hàng đầu, các nhận định về thị trường và xu hướng.
                
                Định dạng đầu ra:
                Trả về một đối tượng JSON duy nhất, hợp lệ và tuân thủ nghiêm ngặt cấu trúc của model 
                CompetitorReport(
                    overall_summary=overall_summary,
                    industry_summaries=industry_summaries,
                    articles=articles,
                    total_articles=len(articles),
                    date_range=date_range
                ).
                """

                try:
                    response = await self.agent.arun(report_prompt)
                    if response and response.content:
                        try:
                            batch_report = CompetitorReport(
                                **json.loads(response.content)
                            )
                            if report is None:
                                report = batch_report
                            else:
                                report.articles.extend(batch_report.articles)
                                report.industry_summaries.extend(
                                    batch_report.industry_summaries
                                )
                                report.total_articles += batch_report.total_articles
                            processed_articles.extend(batch_report.articles)
                            industry_summaries.extend(batch_report.industry_summaries)
                            logger.info(
                                f"Batch {i // batch_size + 1} processed: {len(batch_report.articles)} articles"
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
                        f"Error generating report for batch {i // batch_size + 1}: {e}",
                        exc_info=True,
                    )
                    processed_articles.extend(
                        batch
                    )  # Keep raw articles if processing fails

                # Free memory after each batch
                gc.collect()

            # If no valid report was generated, fall back to basic report
            if report is None:
                logger.warning(
                    "No valid report generated, falling back to basic report."
                )
                return self._create_basic_report(articles, date_range)

            # Update overall summary
            report.overall_summary = OverallSummary(
                thoi_gian_trich_xuat=date_range,
                industries=industry_summaries,
                tong_so_bai=len(processed_articles),
            )
            report.articles = processed_articles
            report.total_articles = len(processed_articles)
            report.date_range = date_range

            logger.info(
                f"Report generation completed. Total articles: {report.total_articles}"
            )
            return report

        except Exception as e:
            logger.error(f"Report generation failed: {e}", exc_info=True)
            return self._create_basic_report(articles, date_range)
        finally:
            gc.collect()  # Final memory cleanup

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

            async def bounded_crawl(media_source, keywords):
                async with semaphore:
                    return await self.crawler.crawl_media_source(
                        media_source=media_source,
                        keywords=keywords,
                        date_range_days=self.config.date_range_days,
                    )

            # Prepare all keywords
            all_keywords = list(
                set(kw for kws in self.config.keywords.values() for kw in kws)
            )
            tasks = [
                bounded_crawl(media_source, all_keywords)
                for media_source in self.config.media_sources
            ]

            # Execute crawl tasks
            for i, task in enumerate(asyncio.as_completed(tasks)):
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
            batch_size = 20  # Process 20 articles per batch
            processed_articles = []
            for i in range(0, len(all_articles), batch_size):
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

            if self.stop_event.is_set():
                raise InterruptedError("Pipeline stopped by user after processing.")

            # Step 3: Generate report in batches
            self.status.current_task = "Generating final report"
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.config.date_range_days)
            date_range_str = f"Từ ngày {start_date.strftime('%d/%m/%Y')} đến ngày {end_date.strftime('%d/%m/%Y')}"

            report = None
            for i in range(0, len(processed_articles), batch_size):
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
            gc.collect()  # Final memory cleanup

    def get_status(self) -> BotStatus:
        return self.status
