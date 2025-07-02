# tests/test_api.py
from fastapi.testclient import TestClient
import uuid

def test_get_status_api(client: TestClient, mock_pipeline_service):
    mock_pipeline_service.get_status.return_value = {"is_running": False}
    response = client.get("/api/status")
    assert response.status_code == 200
    assert response.json() == {"is_running": False}

def test_run_pipeline_api_success(client: TestClient, mock_pipeline_service):
    mock_pipeline_service.is_running = False
    session_id = str(uuid.uuid4())
    response = client.post("/api/run", json={"session_id": session_id})
    assert response.status_code == 202
    mock_pipeline_service.run_pipeline.assert_called_once()

def test_run_pipeline_api_conflict(client: TestClient, mock_pipeline_service):
    mock_pipeline_service.is_running = True
    response = client.post("/api/run", json={})
    assert response.status_code == 409