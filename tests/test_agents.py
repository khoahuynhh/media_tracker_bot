# tests/test_agents.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from agents import CrawlerAgent, get_llm_model
from parsing import ArticleParser

@pytest.fixture
def mock_llm_model(mocker):
    """SỬA LỖI: Mock phương thức aresponse của model, không phải arun."""
    mock_model = MagicMock()
    mock_response = MagicMock()
    mock_response.content = '[{"title": "Mocked Title", "link": "https://mock.com"}]'
    # Mock aresponse là một hàm async
    mock_model.aresponse = AsyncMock(return_value=mock_response)
    return mock_model

@pytest.fixture
def crawler_agent(mock_llm_model):
    # Sửa lại cách khởi tạo agent để truyền model đã mock vào
    parser = ArticleParser()
    agent = CrawlerAgent(model=mock_llm_model, parser=parser)
    return agent

@pytest.mark.asyncio
async def test_crawler_agent_calls_model_correctly(crawler_agent: CrawlerAgent, sample_media_source, mock_llm_model):
    """Kiểm tra xem crawler có gọi đúng phương thức của model không."""
    keywords = ["từ khóa 1"]
    
    result = await crawler_agent.crawl_media_source(
        media_source=sample_media_source,
        keywords=keywords,
        date_range_days=7
    )
    
    # Khẳng định
    # Agent sẽ gọi aresponse trên model được truyền vào
    crawler_agent.agent.model.aresponse.assert_awaited_once()
    assert result.crawl_status == "success"
    assert len(result.articles_found) == 1
    assert result.articles_found[0].tom_tat_noi_dung == "Mocked Title"