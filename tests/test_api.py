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