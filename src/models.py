"""
models.py - Pydantic Models cho Media Tracker Bot (ENHANCED VERSION)
Structured data models phù hợp với template report hiện tại
"""

import json

from typing import List, Optional, Dict, Any
from datetime import datetime, date
from enum import Enum
from pydantic import BaseModel, Field, model_validator, field_validator, ConfigDict
from difflib import get_close_matches


class UserLogin(BaseModel):
    email: str
    password: str


# Giả lập database user
USER_DB = {
    "admin": {"password": "123456", "role": "admin"},
    "user1": {"password": "abc", "role": "viewer"},
    "user2": {"password": "xyz", "role": "editor"},
    "khoa": {"password": "debug", "role": "developer"},
}


class MediaType(str, Enum):
    """Loại media theo danh sách gốc"""

    NEWSPAPER = "Báo (Newspaper)"
    MAGAZINE = "Tạp chí (Magazine)"
    WEBSITE = "Website"
    TV_CHANNEL = "TV Channel"


class MediaSource(BaseModel):
    """Model cho nguồn media từ danh sách gốc"""

    model_config = ConfigDict(use_enum_values=True)
    # id: str = Field(..., description="Định danh duy nhất (khớp CSV)")
    stt: int = Field(..., description="Số thứ tự")
    name: str = Field(..., description="Tên media source")
    type: MediaType = Field(..., description="Loại media")
    domain: Optional[str] = Field(None, description="Domain cho website")
    reference_name: Optional[str] = Field(None, description="Tên tham chiếu cho TV")


class IndustryType(str, Enum):
    """Các ngành hàng theo template"""

    DAU_AN = "Dầu ăn"
    GIA_VI = "Gia vị"
    GAO_NGU_COC = "Gạo & Ngũ cốc"
    SUA_UHT = "Sữa (UHT)"
    BABY_FOOD = "Baby Food"
    HOME_CARE = "Homecare"


class ContentCluster(str, Enum):
    """Cụm nội dung chính"""

    HOAT_DONG_DOANH_NGHIEP = "Hoạt động doanh nghiệp và thông tin sản phẩm"
    CHUONG_TRINH_CSR = "Chương trình CSR"
    MARKETING_CAMPAIGN = "Chiến dịch Marketing"
    PRODUCT_LAUNCH = "Ra mắt sản phẩm"
    PARTNERSHIP = "Hợp tác đối tác"
    FINANCIAL_REPORT = "Báo cáo tài chính"
    FOOD_SAFETY = "An toàn thực phẩm"
    OTHER = "Khác"


class KeywordManager:
    def __init__(self, json_path: str):
        with open(json_path, "r", encoding="utf-8") as f:
            self.cluster_keywords: Dict[str, List[str]] = json.load(f)

    def map_to_cluster(self, text: str) -> str:
        text_lower = text.lower()
        for cluster, keywords in self.cluster_keywords.items():
            for kw in keywords:
                if kw.lower() in text_lower:
                    return cluster
        return ContentCluster.OTHER.value


# Note: Removed duplicate MediaSource definition. The MediaSource class is defined once above
# with fields including `id`, `stt`, `name`, `type`, `domain` and `reference_name`. This
# duplicate definition (lacking the `id` field) has been removed to avoid confusion.


class Article(BaseModel):
    """Model cho bài báo được crawl"""

    # Sử dụng ConfigDict thay cho class Config
    model_config = ConfigDict(use_enum_values=True)

    stt: int = Field(..., description="Số thứ tự")
    ngay_phat_hanh: date = Field(..., description="Ngày phát hành bài báo")
    dau_bao: str = Field(..., description="Tên đầu báo/media source")
    cum_noi_dung: Optional[ContentCluster] = Field(
        default=None,
        description="Cụm nội dung chính của bài báo. Nếu không xác định được thì là 'Khác'",
    )
    cum_noi_dung_chi_tiet: Optional[str] = Field(
        default=None, description="Cụm nội dung chi tiết"
    )
    tom_tat_noi_dung: str = Field(..., description="Tóm tắt nội dung bài báo")
    link_bai_bao: str = Field(..., description="Link đến bài báo gốc")
    nganh_hang: IndustryType = Field(..., description="Ngành hàng liên quan")
    nhan_hang: List[str] = Field(default=[], description="Các nhãn hàng được đề cập")
    keywords_found: List[str] = Field(default=[], description="Keywords được tìm thấy")
    crawl_timestamp: Optional[datetime] = Field(
        default_factory=datetime.now, description="Thời gian crawl"
    )

    @field_validator("ngay_phat_hanh", mode="before")
    def parse_ngay_phat_hanh(cls, v):
        if isinstance(v, str):
            # Try to parse some standard format
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
                try:
                    return datetime.strptime(v.strip(), fmt).date()
                except ValueError:
                    continue
            # Thử parse ISO format YYYY-MM-DD (mặc định Pydantic hỗ trợ)
            try:
                return datetime.fromisoformat(v).date()
            except ValueError:
                pass
            raise ValueError(f"Invalid date format for ngay_phat_hanh: {v}")
        return v

    @field_validator("link_bai_bao", mode="before")
    def validate_url(cls, v):
        if isinstance(v, str):
            # Basic URL validation
            if not v.startswith(("http://", "https://")):
                return f"https://{v}" if v else "https://example.com"
            return v
        return str(v) if v else "https://example.com"

    @staticmethod
    def normalize_cluster(value: str) -> str:
        if not value:
            return "Khác"
        value = value.strip().lower()
        for cluster in ContentCluster:
            if cluster.value.lower() == value:
                return cluster.value
        return "Khác"

    @field_validator("cum_noi_dung", mode="before")
    def convert_cum_noi_dung(cls, v):
        if isinstance(v, str):
            normalized = cls.normalize_cluster(v)
            try:
                return ContentCluster(normalized)
            except ValueError:
                return ContentCluster.OTHER
        return v


class IndustrySummary(BaseModel):
    """Model cho tóm tắt theo ngành hàng"""

    # Sử dụng ConfigDict thay cho class Config
    model_config = ConfigDict(use_enum_values=True)

    nganh_hang: IndustryType = Field(..., description="Ngành hàng")
    nhan_hang: List[str] = Field(..., description="Danh sách nhãn hàng đối thủ")
    cum_noi_dung: List[ContentCluster] = Field(
        ..., description="Các cụm nội dung chính"
    )
    so_luong_bai: int = Field(..., description="Tổng số lượng bài báo")

    cac_dau_bao: List[str] = Field(
        default=[], description="Danh sách các đầu báo có đăng tin"
    )

    @field_validator("cum_noi_dung", mode="before")
    @classmethod
    def convert_cluster(cls, v):
        def to_enum(val: str):
            try:
                return ContentCluster(val)
            except ValueError:
                match = get_close_matches(val, [e.value for e in ContentCluster], n=1)
                if match:
                    return ContentCluster(match[0])
                return ContentCluster.OTHER.value

        if isinstance(v, str):
            return [to_enum(v)]
        if isinstance(v, list):
            return [to_enum(c) if isinstance(c, str) else c for c in v]
        raise TypeError("cum_noi_dung phải là danh sách chuỗi hoặc chuỗi đơn")


class OverallSummary(BaseModel):
    """Model cho tóm tắt tổng quan"""

    thoi_gian_trich_xuat: str = Field(..., description="Thời gian trích xuất dữ liệu")
    industries: List[IndustrySummary] = Field(
        ..., description="Tóm tắt theo từng ngành"
    )
    cac_dau_bao: List[str] = Field(
        default=[], description="Danh sách các đầu báo có đăng tin"
    )
    tong_so_bai: int = Field(..., description="Tổng số bài báo toàn bộ")

    @field_validator("thoi_gian_trich_xuat", mode="before")
    def format_time_range(cls, v):
        if not v:
            end_date = datetime.now()
            start_date = end_date.replace(day=1)  # Đầu tháng
            return f"Từ ngày {start_date.strftime('%d/%m/%Y')} đến ngày {end_date.strftime('%d/%m/%Y')}"
        return v


class CompetitorReport(BaseModel):
    """Model chính cho toàn bộ report - match với template hiện tại"""

    title: str = Field(default="CHICOM COMPETITOR REPORT", description="Tiêu đề report")
    # subtitle: str = Field(
    #     default="PR & SOCIAL MONTHLY REPORT", description="Phụ đề report"
    # )

    # Summary sections
    overall_summary: OverallSummary = Field(..., description="Tóm tắt tổng quan")
    industry_summaries: List[IndustrySummary] = Field(
        ..., description="Tóm tắt theo ngành"
    )

    # Detailed articles
    articles: List[Article] = Field(..., description="Chi tiết các bài báo")

    # Metadata
    generated_at: Optional[datetime] = Field(
        default_factory=datetime.now, description="Thời gian tạo report"
    )
    total_articles: int = Field(..., description="Tổng số bài báo")
    date_range: str = Field(..., description="Khoảng thời gian crawl")

    @model_validator(mode="before")
    def calculate_total_articles(cls, v):
        if isinstance(v, dict):
            articles = v.get("articles", [])
            v["total_articles"] = len(articles)
        return v


class CrawlConfig(BaseModel):
    """Model cho cấu hình crawling - ENHANCED"""

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "keywords": {
                    "Dầu ăn": ["Tường An", "Coba", "Nortalic", "dầu ăn", "cooking oil"],
                    "Gia vị": ["gia vị", "seasoning", "spices", "condiment"],
                },
                "media_sources": [
                    {
                        "stt": 1,
                        "name": "VietnamBiz",
                        "type": "Website",
                        "domain": "vietnambiz.vn",
                    }
                ],
                "date_range_days": 30,
                "max_articles_per_source": 50,
                "crawl_timeout": 30,
                "enable_parallel_crawling": True,
                "max_concurrent_sources": 3,
            }
        }
    )

    keywords: Dict[str, List[str]] = Field(..., description="Keywords theo ngành hàng")
    media_sources: List[MediaSource] = Field(..., description="Danh sách media sources")
    date_range_days: int = Field(default=30, description="Số ngày crawl về trước")
    max_articles_per_source: int = Field(
        default=50, description="Max articles mỗi source"
    )
    crawl_timeout: int = Field(
        default=1920, description="Timeout cho mỗi crawl (seconds)"
    )
    total_pipeline_timeout: int = Field(
        default=36000, description="Tổng timeout pipeline (giây)"
    )
    exclude_domains: List[str] = Field(default=[], description="Domains cần loại trừ")

    # Enhanced configuration options
    enable_parallel_crawling: bool = Field(
        default=True, description="Enable parallel crawling"
    )
    max_concurrent_sources: int = Field(
        default=2, description="Max concurrent sources to crawl"
    )
    retry_failed_sources: bool = Field(default=True, description="Retry failed sources")
    max_retries: int = Field(default=2, description="Max retries for failed sources")
    use_cache: bool = Field(default=True, description="Use caching for crawled data")
    cache_duration_hours: int = Field(default=8, description="Cache duration in hours")
    use_hub_page: bool = False


class CrawlResult(BaseModel):
    """Model cho kết quả crawling từ một source"""

    model_config = ConfigDict(use_enum_values=True)

    source_name: str = Field(..., description="Tên media source")
    source_type: MediaType = Field(..., description="Loại media source")
    url: Optional[str] = Field(None, description="URL được crawl")
    articles_found: List[Article] = Field(
        default=[], description="Các bài báo tìm được"
    )
    crawl_status: str = Field(
        ..., description="Trạng thái crawl: success/failed/partial"
    )
    error_message: Optional[str] = Field(None, description="Thông báo lỗi nếu có")
    crawl_duration: float = Field(..., description="Thời gian crawl (seconds)")
    crawl_timestamp: datetime = Field(
        default_factory=datetime.now, description="Thời gian crawl"
    )
    retry_count: int = Field(default=0, description="Số lần retry")


class ReportTemplate(BaseModel):
    """Model cho template định dạng report HTML/PDF"""

    template_name: str = Field(..., description="Tên template")
    css_styles: str = Field(..., description="CSS styles cho report")
    header_logo: Optional[str] = Field(None, description="Logo path")
    footer_text: str = Field(
        default="Generated by Media Tracker Bot", description="Footer text"
    )
    color_scheme: Dict[str, str] = Field(
        default={
            "primary": "#2E8B57",  # Green như template
            "secondary": "#FFD700",  # Gold accent
            "background": "#F8F9FA",
            "text": "#333333",
        },
        description="Màu sắc template",
    )


class BotStatus(BaseModel):
    """Model cho trạng thái bot - ENHANCED"""

    is_running: bool = Field(default=False, description="Bot có đang chạy không")
    current_task: Optional[str] = Field(None, description="Task hiện tại")
    progress: float = Field(default=0.0, description="Tiến độ (0-100)")
    last_run: Optional[datetime] = Field(None, description="Lần chạy cuối")
    next_scheduled_run: Optional[datetime] = Field(
        None, description="Lần chạy tiếp theo"
    )
    total_sources: int = Field(default=0, description="Tổng số sources")
    completed_sources: int = Field(default=0, description="Số sources đã hoàn thành")
    failed_sources: int = Field(default=0, description="Số sources bị lỗi")

    # Enhanced status fields
    current_session_id: Optional[str] = Field(None, description="Current session ID")
    estimated_completion: Optional[datetime] = Field(
        None, description="Estimated completion time"
    )
    error_details: Optional[str] = Field(None, description="Error details if failed")

    @property
    def status_message(self) -> str:
        """Thông báo trạng thái dễ đọc"""
        if self.is_running:
            return f"Đang chạy: {self.current_task} ({self.progress:.1f}%)"
        elif self.last_run:
            return f"Hoàn thành lúc {self.last_run.strftime('%d/%m/%Y %H:%M')}"
        else:
            return "Chưa chạy lần nào"


class SessionInfo(BaseModel):
    """Model cho thông tin session"""

    session_id: str = Field(..., description="Session ID")
    user_id: Optional[str] = Field(None, description="User ID")
    start_time: datetime = Field(default_factory=datetime.now, description="Start time")
    status: str = Field(default="running", description="Session status")
    config: Optional[Dict[str, Any]] = Field(None, description="Session configuration")
    progress: float = Field(default=0.0, description="Progress percentage")
    current_task: Optional[str] = Field(None, description="Current task")
    error_message: Optional[str] = Field(None, description="Error message if failed")


class APIKeyStatus(BaseModel):
    """Model cho status của API keys"""

    openai_configured: bool = Field(
        default=False, description="OpenAI API key configured"
    )
    groq_configured: bool = Field(default=False, description="Groq API key configured")
    default_provider: str = Field(default="gemini", description="Default provider")
    openai_model: str = Field(default="gpt-4o-mini", description="OpenAI model")
    groq_model: str = Field(default="llama-3.1-70b-versatile", description="Groq model")
    last_updated: Optional[datetime] = Field(None, description="Last updated time")


class SystemHealth(BaseModel):
    """Model cho system health check"""

    status: str = Field(..., description="Overall system status")
    api_keys: APIKeyStatus = Field(..., description="API keys status")
    database_connected: bool = Field(default=True, description="Database connection")
    disk_space_mb: Optional[float] = Field(
        None, description="Available disk space in MB"
    )
    memory_usage_mb: Optional[float] = Field(None, description="Memory usage in MB")
    uptime_seconds: Optional[float] = Field(
        None, description="System uptime in seconds"
    )
    last_health_check: datetime = Field(
        default_factory=datetime.now, description="Last health check"
    )


class PipelineConfig(BaseModel):
    """Model cho pipeline configuration"""

    session_id: Optional[str] = Field(None, description="Session ID")
    start_date: Optional[str] = Field(None, description="Start date (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="End date (YYYY-MM-DD)")
    custom_keywords: Optional[Dict[str, List[str]]] = Field(
        None, description="Custom keywords"
    )
    force_refresh: bool = Field(default=False, description="Force refresh cached data")
    notify_on_completion: bool = Field(
        default=True, description="Send notification on completion"
    )
    export_formats: List[str] = Field(
        default=["json", "excel"], description="Export formats"
    )


# Export tất cả models để sử dụng trong các files khác
__all__ = [
    "MediaType",
    "IndustryType",
    "ContentCluster",
    "MediaSource",
    "Article",
    "IndustrySummary",
    "OverallSummary",
    "CompetitorReport",
    "CrawlConfig",
    "CrawlResult",
    "ReportTemplate",
    "BotStatus",
    "SessionInfo",
    "APIKeyStatus",
    "SystemHealth",
    "PipelineConfig",
]


# Utility functions
def create_sample_report() -> CompetitorReport:
    """Tạo sample report để test"""

    sample_article = Article(
        stt=1,
        ngay_phat_hanh=date.today(),
        dau_bao="Vietnam Biz",
        cum_noi_dung=ContentCluster.HOAT_DONG_DOANH_NGHIEP,
        cum_noi_dung_chi_tiet="Thông tin doanh nghiệp: Thương hiệu quốc qua dẫn đầu thị trường thực phẩm mùa Tết 2025",
        tom_tat_noi_dung="Tường An dẫn đầu thị trường thực phẩm Tết 2025 với các sản phẩm dầu ăn chất lượng cao, khẳng định vị thế với loạt giải pháp quà Tết sáng tạo, đáp ứng nhu cầu tiêu dùng thông minh",
        link_bai_bao="https://vietnambiz.vn/sample-article",
        nganh_hang=IndustryType.DAU_AN,
        nhan_hang=["Tường An", "Coba", "Nortalic"],
        keywords_found=["Tường An", "dầu ăn", "Tết 2025"],
    )

    # Add more sample articles for better demo
    sample_articles = [sample_article]

    for i in range(2, 8):
        article = Article(
            stt=i,
            ngay_phat_hanh=date.today(),
            dau_bao=["VnExpress", "Thanh Nien", "Tuoi Tre", "Lao Dong", "Dan Tri"][
                i % 5
            ],
            cum_noi_dung=[
                ContentCluster.MARKETING_CAMPAIGN,
                ContentCluster.PRODUCT_LAUNCH,
                ContentCluster.CHUONG_TRINH_CSR,
                ContentCluster.PARTNERSHIP,
            ][i % 4],
            tom_tat_noi_dung=f"Tóm tắt bài báo số {i} về ngành FMCG và các hoạt động marketing",
            link_bai_bao=f"https://example.com/article-{i}",
            nganh_hang=[IndustryType.DAU_AN, IndustryType.GIA_VI, IndustryType.SUA_UHT][
                i % 3
            ],
            nhan_hang=["Tường An", "Coba", "Nortalic"][: i % 3 + 1],
            keywords_found=["marketing", "sản phẩm", "FMCG"],
        )
        sample_articles.append(article)

    industry_summary = IndustrySummary(
        nganh_hang=IndustryType.DAU_AN,
        nhan_hang=["Tường An", "Coba", "Nortalic"],
        cum_noi_dung=[
            ContentCluster.HOAT_DONG_DOANH_NGHIEP,
            ContentCluster.CHUONG_TRINH_CSR,
            ContentCluster.MARKETING_CAMPAIGN,
        ],
        so_luong_bai=len(
            [a for a in sample_articles if a.nganh_hang == IndustryType.DAU_AN]
        ),
        cac_dau_bao=list(
            set(
                [
                    a.dau_bao
                    for a in sample_articles
                    if a.nganh_hang == IndustryType.DAU_AN
                ]
            )
        ),
    )

    # Add more industry summaries
    industry_summaries = [industry_summary]

    for industry in [IndustryType.GIA_VI, IndustryType.SUA_UHT]:
        industry_articles = [a for a in sample_articles if a.nganh_hang == industry]
        if industry_articles:
            summary = IndustrySummary(
                nganh_hang=industry,
                nhan_hang=list(
                    set(
                        [
                            brand
                            for article in industry_articles
                            for brand in article.nhan_hang
                        ]
                    )
                ),
                cum_noi_dung=list(
                    set([article.cum_noi_dung for article in industry_articles])
                ),
                so_luong_bai=len(industry_articles),
                cac_dau_bao=list(
                    set([article.dau_bao for article in industry_articles])
                ),
            )
            industry_summaries.append(summary)

    overall_summary = OverallSummary(
        thoi_gian_trich_xuat="Từ ngày 01/12/2024 đến ngày 31/12/2024",
        industries=industry_summaries,
        tong_so_bai=len(sample_articles),
    )

    return CompetitorReport(
        overall_summary=overall_summary,
        industry_summaries=industry_summaries,
        articles=sample_articles,
        total_articles=len(sample_articles),
        date_range="01/12/2024 - 31/12/2024",
    )


def create_default_config() -> CrawlConfig:
    """Tạo default configuration"""
    default_keywords = {
        "Dầu ăn": [
            "Tường An",
            "Coba",
            "Nortalic",
            "Ranee",
            "Nakydaco",
            "Zachia",
            "Basso",
            "Latino Bella",
            "Metro Chef",
            "dầu ăn",
            "cooking oil",
            "dầu thực vật",
            "dầu nành",
            "dầu hướng dương",
            "dầu cọ",
            "dầu oliu",
        ],
        "Gia vị": [
            "gia vị",
            "seasoning",
            "spices",
            "condiment",
            "nước mắm",
            "tương ớt",
            "nước tương",
            "hạt nêm",
            "bột ngọt",
            "muối",
            "đường",
            "tiêu",
            "ớt bột",
        ],
        "Gạo & Ngũ cốc": [
            "gạo",
            "rice",
            "ngũ cốc",
            "cereals",
            "yến mạch",
            "lúa mì",
            "bột mì",
            "bánh mì",
            "noodles",
            "miến",
        ],
        "Sữa (UHT)": [
            "sữa",
            "milk",
            "UHT",
            "sữa tươi",
            "sữa tiệt trùng",
            "sữa bột",
            "sữa đặc",
            "sữa chua",
            "yogurt",
        ],
        "Baby Food": [
            "thức ăn trẻ em",
            "baby food",
            "sữa bột trẻ em",
            "dinh dưỡng trẻ em",
            "bột ăn dặm",
            "infant formula",
        ],
        "Home Care": [
            "nước rửa chén",
            "dishwashing liquid",
            "nước giặt",
            "detergent",
            "fabric softener",
            "nước xả vải",
            "chăm sóc nhà cửa",
            "home care",
            "làm sạch",
        ],
    }

    return CrawlConfig(
        keywords=default_keywords,
        media_sources=[],  # Will be populated by media list parser
        date_range_days=30,
        max_articles_per_source=50,
        crawl_timeout=30,
        exclude_domains=[
            # Mạng xã hội
            "facebook.com",
            "twitter.com",
            "instagram.com",
            "linkedin.com",
            "tiktok.com",
            # Thương mại điện tử Việt Nam
            "shopee.vn",
            "lazada.vn",
            "tiki.vn",
            "sendo.vn",
            "adayroi.com",
            "homefarm.vn",
            "kingfoodmart.com",
            "bachhoaxanh.com",
            "namanmarket.com",
            "tomfruits.com",
            "frenchtaste.com.vn"
            # Quốc tế
            "amazon.com",
            "ebay.com",
            "aliexpress.com",
            "walmart.com",
            # Rút gọn link, spam
            "bit.ly",
            "goo.gl",
        ],
        enable_parallel_crawling=True,
        max_concurrent_sources=5,
        retry_failed_sources=True,
        max_retries=2,
        use_cache=True,
        cache_duration_hours=24,
    )


# Remove the test code block. In production, models are imported and used by other modules.
