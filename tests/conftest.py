# tests/conftest.py
import pytest
from fastapi.testclient import TestClient
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from src.main import app
from src.services import pipeline_service
from src.models import MediaSource, MediaType, CrawlConfig


@pytest.fixture(scope="session")
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture
def mock_pipeline_service(mocker):
    mocker.patch.object(pipeline_service, "is_running", False)
    mocker.patch.object(pipeline_service, "run_pipeline")
    mocker.patch.object(pipeline_service, "stop_pipeline")
    mocker.patch.object(
        pipeline_service,
        "get_status",
        return_value={"is_running": False, "team_status": None},
    )
    return pipeline_service


@pytest.fixture
def sample_media_source():
    return MediaSource(
        stt=1, name="Báo Mẫu", type=MediaType.WEBSITE, domain="baomau.com"
    )


@pytest.fixture
def sample_crawl_config():
    return CrawlConfig(
        keywords={"Test": ["keyword1"]},
        media_sources=[
            MediaSource(stt=1, name="Test", type=MediaType.WEBSITE, domain="test.com")
        ],
    )
