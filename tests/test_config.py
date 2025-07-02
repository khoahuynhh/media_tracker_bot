# tests/test_config.py
import pytest
from config import AppSettings
import os

def test_load_config_success(mocker, tmp_path):
    """SỬA LỖI: Mock cả CONFIG_DIR để đảm bảo test đọc đúng file tạm."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (tmp_path / ".env").write_text("OPENAI_API_KEY=test_key")
    (config_dir / "keywords.json").write_text('{"Test": ["keyword1"]}')
    (config_dir / "media_sources.csv").write_text('name,type,domain\nTest,Website,test.com')
    
    mocker.patch('config.PROJECT_ROOT', tmp_path)
    mocker.patch('config.CONFIG_DIR', config_dir) # Mock thêm dòng này
    
    settings = AppSettings()
    settings._initialized = False
    settings.__init__()
    
    assert settings.crawl_config.keywords["Test"] == ["keyword1"]
    assert len(settings.crawl_config.media_sources) == 1

def test_init_without_api_keys(mocker, tmp_path, caplog):
    """SỬA LỖI: Đảm bảo test không bị ảnh hưởng bởi các test khác."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (tmp_path / ".env").write_text("OPENAI_API_KEY=your_openai_api_key_here")
    (config_dir / "keywords.json").write_text('{}')
    (config_dir / "media_sources.csv").write_text('')

    mocker.patch('config.PROJECT_ROOT', tmp_path)
    mocker.patch('config.CONFIG_DIR', config_dir)
    
    settings = AppSettings()
    settings._initialized = False
    settings.__init__()
    
    assert "FATAL: No valid API keys found" in caplog.text