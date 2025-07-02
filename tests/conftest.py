# tests/conftest.py
"""
File này chứa các fixtures được chia sẻ trên toàn bộ bộ test.
Pytest sẽ tự động tìm và sử dụng các fixtures từ file này.
"""
import pytest
from fastapi.testclient import TestClient
from pathlib import Path
import sys

# Thêm thư mục src vào Python path để có thể import các module
# Điều này rất quan trọng để các lệnh import trong file test hoạt động
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from main import app
from services import pipeline_service
from models import MediaSource, MediaType, CrawlConfig

@pytest.fixture(scope="session")
def client() -> TestClient:
    """
    Tạo một TestClient cho FastAPI app.
    'scope="session"' giúp fixture này chỉ chạy một lần cho mỗi session test.
    """
    return TestClient(app)

@pytest.fixture
def mock_pipeline_service(mocker):
    """
    Mock (giả lập) toàn bộ pipeline_service.
    Điều này giúp chúng ta test các API endpoint mà không cần chạy pipeline thật,
    giúp test chạy ngay lập tức và không phụ thuộc vào các API bên ngoài.
    """
    mocker.patch.object(pipeline_service, "is_running", False)
    mocker.patch.object(pipeline_service, "run_pipeline")
    mocker.patch.object(pipeline_service, "stop_pipeline")
    mocker.patch.object(pipeline_service, "get_status", return_value={"is_running": False, "team_status": None})
    return pipeline_service

@pytest.fixture
def sample_media_source():
    """Tạo một đối tượng MediaSource mẫu để tái sử dụng trong các test."""
    return MediaSource(stt=1, name="Báo Mẫu", type=MediaType.WEBSITE, domain="baomau.com")

@pytest.fixture
def sample_crawl_config():
    """Tạo một đối tượng CrawlConfig mẫu."""
    return CrawlConfig(
        keywords={"Test": ["keyword1"]},
        media_sources=[MediaSource(stt=1, name="Test", type=MediaType.WEBSITE)]
    )