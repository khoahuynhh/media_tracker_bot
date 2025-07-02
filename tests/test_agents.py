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