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