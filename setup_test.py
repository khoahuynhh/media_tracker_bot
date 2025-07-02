# setup_tests.py
"""
Một chương trình tiện ích để tự động tạo cấu trúc thư mục và các file
kiểm thử toàn diện cho dự án Media Tracker Bot.

Phiên bản này bao gồm các test case chi tiết hơn, bao gồm cả các kịch bản
lỗi và kiểm thử cho các hàm bất đồng bộ (async).

Cách chạy:
1. Đặt file này ở thư mục gốc của dự án.
2. Chạy lệnh: python setup_tests.py
"""

import os
from pathlib import Path

# --- Nội dung cho các file test ---

CONFTEST_CONTENT = r'''
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
'''

TEST_PARSING_CONTENT = r'''
# tests/test_parsing.py
import pytest
from datetime import date
from parsing import ArticleParser

@pytest.fixture
def parser():
    """Tạo một instance của ArticleParser cho các test."""
    return ArticleParser()

def test_parse_valid_json_list(parser, sample_media_source):
    """Kiểm tra trường hợp parser nhận được một chuỗi JSON list hợp lệ."""
    json_content = """[{"title": "Bài báo 1", "summary": "Tóm tắt 1", "link": "https://example.com/1", "date": "2025-07-01"}]"""
    articles = parser.parse(json_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Tóm tắt 1"
    assert articles[0].ngay_phat_hanh == date(2025, 7, 1)

def test_parse_structured_text(parser, sample_media_source):
    """Kiểm tra trường hợp parser nhận được văn bản có cấu trúc."""
    text_content = "Tiêu đề: Bài báo Text\nLink: https://example.com/text1\nTóm tắt: Tóm tắt text."
    articles = parser.parse(text_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Tóm tắt text."

def test_parse_empty_and_whitespace_content(parser, sample_media_source):
    """Kiểm tra parser xử lý đúng với input rỗng hoặc chỉ có khoảng trắng."""
    assert parser.parse("", sample_media_source) == []
    assert parser.parse("   \n\t ", sample_media_source) == []

def test_parse_malformed_json_falls_back_to_text(parser, sample_media_source):
    """Kiểm tra parser không bị crash khi nhận JSON không hợp lệ và chuyển sang phân tích dạng text."""
    # JSON này sai cú pháp nhưng chứa các từ khóa mà text parser có thể nhận diện
    malformed_json_content = '{"title": "Bài báo JSON lỗi", "link": "https://example.com/malformed" '
    articles = parser.parse(malformed_json_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Bài báo JSON lỗi"

def test_parse_text_with_missing_link(parser, sample_media_source):
    """Kiểm tra parser bỏ qua các block text thiếu thông tin quan trọng (link)."""
    text_content = "Tiêu đề: Bài báo thiếu link\nTóm tắt: Nội dung không có link."
    articles = parser.parse(text_content, sample_media_source)
    assert len(articles) == 0

def test_parse_content_with_special_chars(parser, sample_media_source):
    """Kiểm tra parser xử lý đúng các ký tự đặc biệt và emoji."""
    json_content = """[{"title": "Test ❤️ & спецсимволы", "summary": "Nội dung có ký tự đặc biệt", "link": "https://example.com/special", "date": "2025-01-01"}]"""
    articles = parser.parse(json_content, sample_media_source)
    assert len(articles) == 1
    assert articles[0].tom_tat_noi_dung == "Nội dung có ký tự đặc biệt"
'''

TEST_API_CONTENT = r'''
# tests/test_api.py
from fastapi.testclient import TestClient
import uuid
from unittest.mock import ANY

def test_get_status_api(client: TestClient, mock_pipeline_service):
    """Kiểm tra endpoint /api/status."""
    mock_pipeline_service.get_status.return_value = {"is_running": False}
    response = client.get("/api/status")
    assert response.status_code == 200
    assert response.json() == {"is_running": False}
    mock_pipeline_service.get_status.assert_called_once()

def test_run_pipeline_api_success(client: TestClient, mock_pipeline_service):
    """Kiểm tra việc gọi /api/run thành công."""
    mock_pipeline_service.is_running = False
    session_id = str(uuid.uuid4())
    response = client.post("/api/run", json={"session_id": session_id})
    assert response.status_code == 202
    mock_pipeline_service.run_pipeline.assert_called_once()

def test_run_pipeline_api_conflict(client: TestClient, mock_pipeline_service):
    """Kiểm tra việc gọi /api/run thất bại khi đã có pipeline đang chạy."""
    mock_pipeline_service.is_running = True
    response = client.post("/api/run", json={})
    assert response.status_code == 409
    mock_pipeline_service.run_pipeline.assert_not_called()

def test_stop_pipeline_api_success(client: TestClient, mock_pipeline_service):
    """Kiểm tra việc dừng pipeline thành công."""
    mock_pipeline_service.is_running = True
    mock_pipeline_service.current_session_id = "test-session-123"
    mock_pipeline_service.stop_pipeline.return_value = True
    
    response = client.post("/api/stop", json={"session_id": "test-session-123"})
    
    assert response.status_code == 200
    assert response.json() == {"message": "Pipeline stop requested."}
    mock_pipeline_service.stop_pipeline.assert_called_once_with("test-session-123")

def test_update_config_api(client: TestClient, mock_pipeline_service, sample_crawl_config, mocker):
    """Kiểm tra việc cập nhật cấu hình qua API."""
    # Mock hàm save_config trong module config để không ghi file thật
    mock_save = mocker.patch('config.AppSettings.save_crawl_config')
    
    config_data = sample_crawl_config.model_dump(mode='json')
    response = client.post("/api/config", json=config_data)
    
    assert response.status_code == 200
    assert response.json()["message"] == "Configuration updated successfully."
    mock_save.assert_called_once()
'''

TEST_CONFIG_CONTENT = r'''
# tests/test_config.py
import pytest
from config import AppSettings
import os

def test_load_config_success(mocker, tmp_path):
    """Kiểm tra việc tải cấu hình thành công từ các file hợp lệ."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (tmp_path / ".env").write_text("OPENAI_API_KEY=test_key")
    (config_dir / "keywords.json").write_text('{"Test": ["keyword1"]}')
    (config_dir / "media_sources.csv").write_text('name,type,domain\nTest,Website,test.com')
    
    mocker.patch('config.PROJECT_ROOT', tmp_path)
    
    settings = AppSettings()
    settings._initialized = False
    settings.__init__()
    
    assert settings.crawl_config.keywords["Test"] == ["keyword1"]
    assert len(settings.crawl_config.media_sources) == 1
    assert settings.crawl_config.media_sources[0].domain == "test.com"

def test_load_config_with_missing_media_file(mocker, tmp_path, caplog):
    """Kiểm tra hệ thống xử lý khi file media_sources.csv bị thiếu."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (tmp_path / ".env").write_text("OPENAI_API_KEY=test_key")
    (config_dir / "keywords.json").write_text('{}')
    
    mocker.patch('config.PROJECT_ROOT', tmp_path)
    
    settings = AppSettings()
    settings._initialized = False
    settings.__init__()
    
    assert len(settings.crawl_config.media_sources) == 0
    assert "media_sources.csv not found" in caplog.text

def test_init_without_api_keys(mocker, tmp_path, caplog):
    """Kiểm tra hệ thống log lỗi khi không có API key nào được cấu hình."""
    (tmp_path / ".env").write_text("OPENAI_API_KEY=your_openai_api_key_here")
    mocker.patch('config.PROJECT_ROOT', tmp_path)
    
    settings = AppSettings()
    settings._initialized = False
    settings.__init__()
    
    assert "No valid API keys found" in caplog.text
'''

TEST_SERVICES_CONTENT = r'''
# tests/test_services.py
import pytest
from services import PipelineService
from config import AppSettings
from unittest.mock import AsyncMock

@pytest.fixture
def mock_settings(mocker, sample_crawl_config):
    """Mock AppSettings để không cần đọc file thật."""
    mock = mocker.MagicMock(spec=AppSettings)
    mock.crawl_config = sample_crawl_config
    return mock

@pytest.fixture
def service_instance(mock_settings):
    """Tạo một instance của PipelineService với settings đã được mock."""
    return PipelineService(app_settings=mock_settings)

def test_service_initial_state(service_instance: PipelineService):
    """Kiểm tra trạng thái ban đầu của service."""
    assert not service_instance.is_running
    assert service_instance.current_session_id is None

@pytest.mark.asyncio
async def test_run_pipeline_calls_team(mocker, service_instance: PipelineService):
    """Kiểm tra xem service có gọi đến phương thức run_full_pipeline của team không."""
    # Mock class MediaTrackerTeam
    mock_team_instance = mocker.MagicMock()
    mock_team_instance.run_full_pipeline = AsyncMock(return_value=None) # Mock hàm async
    mock_team_class = mocker.patch('services.MediaTrackerTeam', return_value=mock_team_instance)
    
    # Mock hàm save_report để không ghi file
    mocker.patch.object(service_instance, '_save_report', new_callable=AsyncMock)

    session_id = "test-session"
    await service_instance.run_pipeline(session_id=session_id)
    
    mock_team_class.assert_called_once() # Đảm bảo team được khởi tạo
    mock_team_instance.run_full_pipeline.assert_awaited_once() # Đảm bảo pipeline được chạy
    assert not service_instance.is_running # Đảm bảo trạng thái được reset
'''

TEST_AGENTS_CONTENT = r'''
# tests/test_agents.py
import pytest
from unittest.mock import AsyncMock, ANY
from agents import CrawlerAgent
from parsing import ArticleParser

@pytest.fixture
def mock_llm_model(mocker):
    """Mock model LLM với một hàm arun bất đồng bộ."""
    mock_model = mocker.MagicMock()
    mock_response = mocker.MagicMock()
    mock_response.content = '[{"title": "Mocked Title", "link": "https://mock.com"}]'
    # Sử dụng AsyncMock cho các hàm bất đồng bộ
    mock_model.arun = AsyncMock(return_value=mock_response)
    return mock_model

@pytest.fixture
def crawler_agent(mock_llm_model):
    """Tạo một CrawlerAgent với model đã được mock."""
    return CrawlerAgent(model=mock_llm_model, parser=ArticleParser())

@pytest.mark.asyncio
async def test_crawler_agent_creates_correct_prompt(crawler_agent: CrawlerAgent, sample_media_source, mock_llm_model):
    """Kiểm tra xem crawler có tạo ra prompt đúng và gọi model không."""
    keywords = ["từ khóa 1", "từ khóa 2"]
    
    # Hành động (Act)
    result = await crawler_agent.crawl_media_source(
        media_source=sample_media_source,
        keywords=keywords,
        date_range_days=7
    )
    
    # Khẳng định (Assert)
    mock_llm_model.arun.assert_awaited_once() # Đảm bảo hàm arun được gọi
    
    # Lấy các tham số đã được dùng để gọi hàm mock
    call_args, call_kwargs = mock_llm_model.arun.call_args
    prompt = call_args[0]
    
    # Kiểm tra nội dung prompt
    assert "crawl trang web" in prompt.lower()
    assert sample_media_source.domain in prompt
    assert keywords[0] in prompt
    
    # Kiểm tra kết quả trả về
    assert result.crawl_status == "success"
    assert len(result.articles_found) == 1
    assert result.articles_found[0].tom_tat_noi_dung == "Mocked Title"
'''

# --- Hàm chính để tạo file ---

def create_test_suite():
    """Hàm chính để tạo thư mục và các file test."""
    project_root = Path(__file__).parent
    tests_dir = project_root / "tests"

    files_to_create = {
        "conftest.py": CONFTEST_CONTENT,
        "test_parsing.py": TEST_PARSING_CONTENT,
        "test_api.py": TEST_API_CONTENT,
        "test_config.py": TEST_CONFIG_CONTENT,
        "test_services.py": TEST_SERVICES_CONTENT,
        "test_agents.py": TEST_AGENTS_CONTENT,
    }

    print("Bắt đầu quá trình thiết lập bộ kiểm thử toàn diện...")
    tests_dir.mkdir(exist_ok=True)
    print(f"Đảm bảo thư mục '{tests_dir.name}' tồn tại.")

    (tests_dir / "__init__.py").touch() # Đảm bảo thư mục tests là một package

    for filename, content in files_to_create.items():
        file_path = tests_dir / filename
        # Luôn cập nhật file để đảm bảo nội dung mới nhất
        print(f"Tạo/Cập nhật file: {file_path}")
        file_path.write_text(content.strip(), encoding='utf-8')
    
    print("\n✅ Hoàn tất! Bộ khung kiểm thử đã được tạo/cập nhật.")
    print("💡 Hãy cài đặt các thư viện cần thiết (nếu chưa có):")
    print("   pip install pytest pytest-mock pytest-asyncio pytest-benchmark")
    print("💡 Chạy bộ kiểm thử bằng lệnh: 'pytest'")

if __name__ == "__main__":
    create_test_suite()
