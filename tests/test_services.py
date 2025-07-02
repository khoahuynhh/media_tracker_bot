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