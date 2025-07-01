"""
agents.py - Multi-Agent System cho Media Tracker Bot
Sử dụng Agno framework để coordinate giữa các agents
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from agno.agent import Agent
from agno.models.openai import OpenAIChat
from agno.models.groq import Groq
from agno.tools.crawl4ai import Crawl4aiTools
from agno.tools.googlesearch import GoogleSearchTools
from agno.team import Team

from models import (
    MediaSource,
    Article,
    IndustrySummary,
    OverallSummary,
    CompetitorReport,
    CrawlResult,
    CrawlConfig,
    BotStatus,
    IndustryType,
    ContentCluster,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CrawlerAgent:
    """Agent chuyên crawler web sử dụng Crawl4AI"""

    def __init__(self, model_provider: str = "openai", model_id: str = "gpt-4o-mini"):
        """
        Initialize CrawlerAgent

        Args:
            model_provider: "openai" hoặc "groq"
            model_id: Model ID để sử dụng
        """
        # Chọn model provider
        if model_provider == "groq":
            model = Groq(id=model_id)
        else:
            model = OpenAIChat(id=model_id)

        self.agent = Agent(
            name="MediaCrawler",
            role="Web Crawler và Content Extractor",
            model=model,
            tools=[Crawl4aiTools(max_length=2000), GoogleSearchTools()],
            instructions=[
                "Bạn là chuyên gia crawl web cho tracking truyền thông Việt Nam.",
                "Nhiệm vụ: Crawl các website báo/tạp chí/media để tìm tin tức về competitors.",
                "Chỉ trích xuất những bài viết có liên quan đến keywords được cung cấp.",
                "Luôn cung cấp link gốc và tóm tắt nội dung chính xác.",
                "Ưu tiên tin tức mới nhất trong khoảng thời gian được chỉ định.",
                "Trả về kết quả dưới dạng text có cấu trúc rõ ràng.",
                "Bỏ qua các bài viết không liên quan hoặc spam.",
                "Xử lý gracefully các lỗi crawling và báo cáo chi tiết.",
                "Focus on direct website crawling instead of search engines.",
            ],
            show_tool_calls=True,
            markdown=True,
            add_datetime_to_instructions=True,
        )

    async def crawl_media_source(
        self,
        media_source: MediaSource,
        keywords: List[str],
        date_range_days: int = 30,
        start_date: str = None,
        end_date: str = None,
    ) -> CrawlResult:
        """
        Crawl một media source để tìm articles liên quan

        Args:
            media_source: MediaSource object
            keywords: List keywords để tìm kiếm
            date_range_days: Số ngày tìm về trước (nếu không có start_date/end_date)
            start_date: Ngày bắt đầu (format: YYYY-MM-DD)
            end_date: Ngày kết thúc (format: YYYY-MM-DD)

        Returns:
            CrawlResult với articles tìm được
        """
        start_time = datetime.now()

        try:
            # Determine date range
            if start_date and end_date:
                date_filter = f"từ {start_date} đến {end_date}"
            else:
                end_date_obj = datetime.now()
                start_date_obj = end_date_obj - timedelta(days=date_range_days)
                date_filter = f"từ {start_date_obj.strftime('%Y-%m-%d')} đến {end_date_obj.strftime('%Y-%m-%d')}"

            # Build domain URL for direct crawling
            domain_url = media_source.domain
            if domain_url and not domain_url.startswith("http"):
                domain_url = f"https://{domain_url}"

            # Xây dựng crawl query - shorter and more focused
            keywords_str = ", ".join(
                keywords[:5]
            )  # Limit to 5 keywords to avoid long URLs

            crawl_query = f"""
            Crawl website: {domain_url or media_source.name}
            
            Tìm các bài báo có chứa từ khóa: {keywords_str}
            Thời gian: {date_filter}
            
            Yêu cầu:
            - Trích xuất tiêu đề, tóm tắt, ngày đăng, link gốc
            - Chỉ lấy bài viết liên quan đến keywords
            - Format: Tiêu đề | Ngày | Tóm tắt | Link
            
            Media: {media_source.name}
            """

            # Gọi agent để crawl
            session_id = (
                f"crawler-{media_source.name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            )
            response = await self.agent.arun(crawl_query, session_id=session_id)

            # Parse response thành CrawlResult
            duration = (datetime.now() - start_time).total_seconds()

            if response and response.content:
                return CrawlResult(
                    source_name=media_source.name,
                    source_type=media_source.type,
                    url=media_source.domain,
                    articles_found=self._parse_articles_from_response(
                        response.content, media_source
                    ),
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
                    error_message="No response from crawler",
                    crawl_duration=duration,
                )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Crawl failed for {media_source.name}: {str(e)}")

            return CrawlResult(
                source_name=media_source.name,
                source_type=media_source.type,
                url=media_source.domain,
                articles_found=[],
                crawl_status="failed",
                error_message=str(e),
                crawl_duration=duration,
            )

    def _parse_articles_from_response(
        self, content: str, media_source: MediaSource
    ) -> List[Article]:
        """Parse nội dung response thành list Article objects"""
        articles = []

        try:
            # Thử parse JSON nếu có
            if content.strip().startswith("{") or content.strip().startswith("["):
                data = json.loads(content)
                if isinstance(data, list):
                    for item in data:
                        article = self._create_article_from_dict(item, media_source)
                        if article:
                            articles.append(article)
            else:
                # Parse text format
                articles = self._parse_text_articles(content, media_source)

        except Exception as e:
            logger.warning(f"Failed to parse articles from response: {str(e)}")

        return articles

    def _create_article_from_dict(
        self, data: Dict, media_source: MediaSource
    ) -> Optional[Article]:
        """Tạo Article object từ dictionary"""
        try:
            return Article(
                stt=data.get("stt", 1),
                ngay_phat_hanh=data.get("ngay_phat_hanh", datetime.now().date()),
                dau_bao=media_source.name,
                cum_noi_dung=data.get("cum_noi_dung", ContentCluster.OTHER),
                tom_tat_noi_dung=data.get("tom_tat_noi_dung", ""),
                link_bai_bao=data.get("link_bai_bao", "https://example.com"),
                nganh_hang=data.get("nganh_hang", IndustryType.DAU_AN),
                nhan_hang=data.get("nhan_hang", []),
                keywords_found=data.get("keywords_found", []),
            )
        except Exception as e:
            logger.warning(f"Failed to create article from dict: {str(e)}")
            return None

    def _parse_text_articles(
        self, content: str, media_source: MediaSource
    ) -> List[Article]:
        """Parse articles từ text content"""
        articles = []

        # Basic parsing logic - có thể cải thiện thêm
        lines = content.split("\n")
        current_article = {}

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Detect article boundaries và extract info
            if "tiêu đề:" in line.lower() or "title:" in line.lower():
                current_article["title"] = line.split(":", 1)[1].strip()
            elif "link:" in line.lower() or "url:" in line.lower():
                current_article["url"] = line.split(":", 1)[1].strip()
            elif "ngày:" in line.lower() or "date:" in line.lower():
                current_article["date"] = line.split(":", 1)[1].strip()
            elif "tóm tắt:" in line.lower() or "summary:" in line.lower():
                current_article["summary"] = line.split(":", 1)[1].strip()

                # Khi có đủ thông tin, tạo Article
                if len(current_article) >= 3:
                    try:
                        article = Article(
                            stt=len(articles) + 1,
                            ngay_phat_hanh=datetime.now().date(),
                            dau_bao=media_source.name,
                            cum_noi_dung=ContentCluster.OTHER,
                            tom_tat_noi_dung=current_article.get(
                                "summary", current_article.get("title", "")
                            ),
                            link_bai_bao=current_article.get(
                                "url", "https://example.com"
                            ),
                            nganh_hang=IndustryType.DAU_AN,  # Default, sẽ được ProcessorAgent phân loại lại
                            nhan_hang=[],
                            keywords_found=[],
                        )
                        articles.append(article)
                    except Exception as e:
                        logger.warning(f"Failed to create article: {str(e)}")

                    current_article = {}

        return articles


class ProcessorAgent:
    """Agent chuyên xử lý và phân tích nội dung"""

    def __init__(self, model_provider: str = "openai", model_id: str = "gpt-4o"):
        """Initialize ProcessorAgent với model mạnh hơn cho analysis"""

        if model_provider == "groq":
            model = Groq(id=model_id)
        else:
            model = OpenAIChat(id=model_id)

        self.agent = Agent(
            name="ContentProcessor",
            role="Content Analyst và Classifier",
            model=model,
            instructions=[
                "Bạn là chuyên gia phân tích nội dung truyền thông cho ngành FMCG Việt Nam.",
                "Nhiệm vụ: Phân tích và phân loại nội dung bài báo theo ngành hàng và nhãn hiệu.",
                "Xác định chính xác: Ngành hàng, Nhãn hàng, Cụm nội dung chính.",
                "Các ngành hàng chính: Dầu ăn, Gia vị, Gạo & Ngũ cốc, Sữa (UHT), Baby Food, Home Care.",
                "Các cụm nội dung: Hoạt động doanh nghiệp, CSR, Marketing, Ra mắt sản phẩm, Hợp tác, Báo cáo tài chính.",
                "Trích xuất keywords và nhãn hiệu competitors một cách chính xác.",
                "Đánh giá tầm quan trọng và impact của từng bài viết.",
                "Loại bỏ các bài viết không liên quan hoặc chất lượng thấp.",
                "Trả về kết quả dưới dạng text có cấu trúc rõ ràng.",
            ],
            show_tool_calls=False,
            markdown=True,
            add_datetime_to_instructions=True,
        )

    async def process_articles(
        self, raw_articles: List[Article], keywords_config: Dict[str, List[str]]
    ) -> List[Article]:
        """
        Xử lý và phân loại các articles

        Args:
            raw_articles: List articles thô từ crawler
            keywords_config: Config keywords theo ngành hàng

        Returns:
            List articles đã được xử lý và phân loại
        """
        if not raw_articles:
            return []

        try:
            # Tạo prompt cho việc phân tích
            analysis_prompt = f"""
            Phân tích và phân loại {len(raw_articles)} bài báo sau đây:
            
            Keywords config:
            {json.dumps(keywords_config, ensure_ascii=False, indent=2)}
            
            Raw articles:
            {json.dumps([article.dict() for article in raw_articles], ensure_ascii=False, indent=2)}
            
            Yêu cầu:
            1. Phân loại chính xác ngành hàng cho từng bài
            2. Xác định nhãn hàng competitors được đề cập
            3. Phân loại cụm nội dung (Hoạt động doanh nghiệp, CSR, Marketing, etc.)
            4. Trích xuất keywords tìm thấy
            5. Cải thiện tóm tắt nội dung
            6. Loại bỏ các bài không liên quan
            
            Trả về list Article objects đã được xử lý đầy đủ.
            """
            session_id = f"processor-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            response = await self.agent.arun(analysis_prompt, session_id=session_id)

            if response and response.content:
                # Parse response thành list Article
                processed_articles = self._parse_processed_articles(
                    response.content, raw_articles
                )
                logger.info(
                    f"Processed {len(processed_articles)} articles successfully"
                )
                return processed_articles
            else:
                logger.warning("No response from processor agent")
                return raw_articles

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            return raw_articles

    def _parse_processed_articles(
        self, content: str, original_articles: List[Article]
    ) -> List[Article]:
        """Parse processed articles từ agent response"""
        try:
            # Thử parse JSON response
            if content.strip().startswith("["):
                data = json.loads(content)
                processed_articles = []

                for item in data:
                    try:
                        article = Article(**item)
                        processed_articles.append(article)
                    except Exception as e:
                        logger.warning(f"Failed to parse article: {str(e)}")
                        continue

                return processed_articles
            else:
                # Fallback: return original articles
                logger.warning(
                    "Could not parse processed articles, returning originals"
                )
                return original_articles

        except Exception as e:
            logger.error(f"Failed to parse processed articles: {str(e)}")
            return original_articles


class ReportAgent:
    """Agent chuyên tạo reports"""

    def __init__(self, model_provider: str = "openai", model_id: str = "gpt-4o"):
        """Initialize ReportAgent"""

        if model_provider == "groq":
            model = Groq(id=model_id)
        else:
            model = OpenAIChat(id=model_id)

        self.agent = Agent(
            name="ReportGenerator",
            role="Report Generator và Data Analyst",
            model=model,
            instructions=[
                "Bạn là chuyên gia tạo báo cáo truyền thông cho ngành FMCG.",
                "Nhiệm vụ: Tạo báo cáo competitor analysis từ dữ liệu bài báo đã crawl.",
                "Format báo cáo theo template: CALOFIC COMPETITOR REPORT - PR & SOCIAL MONTHLY REPORT.",
                "Tạo tóm tắt tổng quan và chi tiết theo từng ngành hàng.",
                "Thống kê chính xác: số lượng bài, phân bố theo ngành, top media sources.",
                "Insights và recommendations dựa trên dữ liệu.",
                "Đảm bảo format professional và easy-to-read.",
                "Include charts và visualizations nếu có thể.",
                "Trả về kết quả dưới dạng text có cấu trúc rõ ràng.",
            ],
            show_tool_calls=False,
            markdown=True,
            add_datetime_to_instructions=True,
        )

    async def generate_report(
        self, articles: List[Article], date_range: str
    ) -> CompetitorReport:
        """
        Tạo báo cáo từ list articles

        Args:
            articles: List articles đã processed
            date_range: Khoảng thời gian báo cáo

        Returns:
            CompetitorReport object
        """
        try:
            # Tạo prompt để generate report
            report_prompt = f"""
            Tạo báo cáo COMPETITOR REPORT từ {len(articles)} bài báo sau:
            
            Thời gian: {date_range}
            
            Articles data:
            {json.dumps([article.dict() for article in articles], ensure_ascii=False, indent=2)}
            
            Yêu cầu báo cáo:
            1. Overall Summary với thống kê tổng quan
            2. Industry Summaries theo từng ngành hàng
            3. Top competitors và market insights
            4. Phân tích trend và patterns
            5. Recommendations
            
            Format theo CompetitorReport model đã định nghĩa.
            """

            response = await self.agent.arun(report_prompt)

            if response and response.content:
                # Parse response thành CompetitorReport
                report = self._parse_report_response(
                    response.content, articles, date_range
                )
                logger.info(f"Generated report with {len(articles)} articles")
                return report
            else:
                # Fallback: tạo basic report
                return self._create_basic_report(articles, date_range)

        except Exception as e:
            logger.error(f"Report generation failed: {str(e)}")
            return self._create_basic_report(articles, date_range)

    def _parse_report_response(
        self, content: str, articles: List[Article], date_range: str
    ) -> CompetitorReport:
        """Parse report từ agent response"""
        try:
            if content.strip().startswith("{"):
                data = json.loads(content)
                return CompetitorReport(**data)
            else:
                return self._create_basic_report(articles, date_range)
        except Exception as e:
            logger.warning(f"Failed to parse report response: {str(e)}")
            return self._create_basic_report(articles, date_range)

    def _create_basic_report(
        self, articles: List[Article], date_range: str
    ) -> CompetitorReport:
        """Tạo basic report khi parse thất bại"""
        # Group articles by industry
        industry_groups = {}
        for article in articles:
            industry = article.nganh_hang
            if industry not in industry_groups:
                industry_groups[industry] = []
            industry_groups[industry].append(article)

        # Tạo industry summaries
        industry_summaries = []
        for industry, industry_articles in industry_groups.items():
            # Collect unique brands
            brands = set()
            content_clusters = set()
            media_sources = set()

            for article in industry_articles:
                brands.update(article.nhan_hang)
                content_clusters.add(article.cum_noi_dung)
                media_sources.add(article.dau_bao)

            summary = IndustrySummary(
                nganh_hang=industry,
                nhan_hang=list(brands),
                cum_noi_dung=list(content_clusters),
                so_luong_bai=len(industry_articles),
                cac_dau_bao=list(media_sources),
            )
            industry_summaries.append(summary)

        # Tạo overall summary
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
    """Main coordinator team cho toàn bộ workflow"""

    def __init__(self, config: CrawlConfig, model_provider: str = "openai"):
        """
        Initialize MediaTrackerTeam

        Args:
            config: CrawlConfig với media sources và keywords
            model_provider: "openai" hoặc "groq"
        """
        self.config = config
        self.model_provider = model_provider

        # Initialize individual agents
        self.crawler = CrawlerAgent(model_provider=model_provider)
        self.processor = ProcessorAgent(model_provider=model_provider)
        self.reporter = ReportAgent(model_provider=model_provider)

        # Status tracking
        self.status = BotStatus()

        # Setup team coordinator
        self._setup_coordinator()

    def _setup_coordinator(self):
        """Setup team coordinator agent"""
        if self.model_provider == "groq":
            model = Groq(id="llama-3.1-70b-versatile")
        else:
            model = OpenAIChat(id="gpt-4o")

        self.coordinator = Team(
            name="MediaTrackerCoordinator",
            mode="coordinate",
            model=model,
            members=[self.crawler.agent, self.processor.agent, self.reporter.agent],
            description="Coordinator team cho media tracking workflow",
            instructions=[
                "Điều phối workflow: Crawling -> Processing -> Reporting",
                "Monitor progress và handle errors gracefully",
                "Optimize performance và resource usage",
                "Provide detailed status updates",
                "Ensure data quality và accuracy",
            ],
            enable_agentic_context=True,
            share_member_interactions=True,
            show_members_responses=False,
            markdown=True,
        )

    async def run_full_pipeline(self) -> CompetitorReport:
        """
        Chạy toàn bộ pipeline: Crawl -> Process -> Report

        Returns:
            CompetitorReport object
        """
        self.status.is_running = True
        self.status.current_task = "Initializing pipeline"
        self.status.total_sources = len(self.config.media_sources)
        self.status.completed_sources = 0
        self.status.failed_sources = 0

        try:
            logger.info("Starting media tracking pipeline...")

            # Phase 1: Crawling
            self.status.current_task = "Crawling media sources"
            all_articles = []

            for i, media_source in enumerate(self.config.media_sources):
                try:
                    # Determine keywords for this source
                    relevant_keywords = self._get_relevant_keywords(media_source)

                    # Crawl source
                    result = await self.crawler.crawl_media_source(
                        media_source=media_source,
                        keywords=relevant_keywords,
                        date_range_days=self.config.date_range_days,
                    )

                    if result.crawl_status == "success":
                        all_articles.extend(result.articles_found)
                        self.status.completed_sources += 1
                    else:
                        self.status.failed_sources += 1
                        logger.warning(
                            f"Failed to crawl {media_source.name}: {result.error_message}"
                        )

                    # Update progress
                    self.status.progress = (
                        (i + 1) / self.status.total_sources * 30
                    )  # 30% for crawling

                except Exception as e:
                    logger.error(f"Error crawling {media_source.name}: {str(e)}")
                    self.status.failed_sources += 1

            logger.info(f"Crawling completed. Found {len(all_articles)} articles")

            # Phase 2: Processing
            self.status.current_task = "Processing and analyzing content"
            self.status.progress = 40

            processed_articles = await self.processor.process_articles(
                raw_articles=all_articles, keywords_config=self.config.keywords
            )

            self.status.progress = 70
            logger.info(
                f"Processing completed. {len(processed_articles)} articles processed"
            )

            # Phase 3: Report Generation
            self.status.current_task = "Generating report"
            self.status.progress = 80

            date_range = self._generate_date_range()
            report = await self.reporter.generate_report(
                articles=processed_articles, date_range=date_range
            )

            self.status.progress = 100
            self.status.current_task = "Completed"
            self.status.is_running = False
            self.status.last_run = datetime.now()

            logger.info("Pipeline completed successfully!")
            return report

        except Exception as e:
            self.status.is_running = False
            self.status.current_task = f"Failed: {str(e)}"
            logger.error(f"Pipeline failed: {str(e)}")
            raise

    def _get_relevant_keywords(self, media_source: MediaSource) -> List[str]:
        """Get relevant keywords cho media source"""
        # Combine all keywords from config
        all_keywords = []
        for industry_keywords in self.config.keywords.values():
            all_keywords.extend(industry_keywords)

        return all_keywords

    def _generate_date_range(self) -> str:
        """Generate date range string"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.config.date_range_days)

        return f"Từ ngày {start_date.strftime('%d/%m/%Y')} đến ngày {end_date.strftime('%d/%m/%Y')}"

    def get_status(self) -> BotStatus:
        """Get current status của bot"""
        return self.status


# Utility functions
async def create_sample_team() -> MediaTrackerTeam:
    """Tạo sample team để test"""
    from models import MediaSource, MediaType

    # Sample config
    sample_config = CrawlConfig(
        keywords={
            "Dầu ăn": ["Tường An", "Coba", "Nortalic", "dầu ăn"],
            "Gia vị": ["gia vị", "seasoning", "chấm"],
        },
        media_sources=[
            MediaSource(
                stt=1, name="VietnamBiz", type=MediaType.WEBSITE, domain="vietnambiz.vn"
            ),
            MediaSource(
                stt=2, name="VnExpress", type=MediaType.WEBSITE, domain="vnexpress.net"
            ),
        ],
        date_range_days=30,
    )

    return MediaTrackerTeam(config=sample_config)


if __name__ == "__main__":
    # Test agents
    async def test_agents():
        team = await create_sample_team()
        print("✅ Agents created successfully!")
        print(f"Team coordinator: {team.coordinator.name}")
        print(f"Total media sources: {team.status.total_sources}")

        # Test run (comment out for actual run)
        # report = await team.run_full_pipeline()
        # print(f"Report generated: {report.total_articles} articles")

    asyncio.run(test_agents())
