# src/config.py
"""
Module to handle all application configuration.
Loads settings from .env and config files.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
from dotenv import load_dotenv
from pydantic import BaseModel

# Import models sau để tránh lỗi import vòng tròn nếu có
from models import CrawlConfig, MediaSource

# Setup logging
logger = logging.getLogger(__name__)

# Define root and config paths
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"


class AppSettings:
    """
    Singleton class to hold all application settings.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AppSettings, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized and hasattr(self, 'crawl_config'):
            return
        
        self.project_root = PROJECT_ROOT
        self.config_dir = CONFIG_DIR
        self.data_dir = DATA_DIR
        self.reports_dir = self.data_dir / "reports"
        self.cache_dir = self.data_dir / "cache"
        
        self._setup_directories()
        self._setup_environment()
        
        self.crawl_config = self._load_crawl_config()
        self._initialized = True

    def _setup_directories(self):
        """Create necessary directories if they don't exist."""
        for directory in [self.config_dir, self.data_dir, self.reports_dir, self.cache_dir, PROJECT_ROOT / "logs"]:
            directory.mkdir(parents=True, exist_ok=True)
        logger.info("Directories setup verified.")

    def _setup_environment(self):
        """Load .env file and validate API keys."""
        env_file = self.project_root / ".env"
        if not env_file.exists():
            env_template = """# API Keys for Media Tracker Bot
OPENAI_API_KEY=your_openai_api_key_here
GROQ_API_KEY=your_groq_api_key_here
"""
            env_file.write_text(env_template, encoding="utf-8")
            logger.warning(f"Created .env template at {env_file}. Please update it with your API keys.")
        
        # SỬA LỖI: Thêm override=True để đảm bảo test có thể ghi đè biến môi trường
        load_dotenv(dotenv_path=env_file, override=True)
        self._validate_api_keys()

    def _validate_api_keys(self) -> bool:
        """Check if at least one API key is configured."""
        openai_key = os.getenv("OPENAI_API_KEY")
        groq_key = os.getenv("GROQ_API_KEY")
        has_openai = openai_key and "your_openai_api_key_here" not in openai_key
        has_groq = groq_key and "your_groq_api_key_here" not in groq_key

        if not has_openai and not has_groq:
            logger.error("FATAL: No valid API keys found in .env file. The application cannot run.")
            return False
        
        logger.info(f"API Key Status: OpenAI {'Configured' if has_openai else 'Not Configured'}, Groq {'Configured' if has_groq else 'Not Configured'}")
        return True

    def _load_crawl_config(self) -> CrawlConfig:
        """Load the main crawling configuration by combining components."""
        try:
            media_sources = self._parse_media_list()
            keywords = self._load_keywords_config()
            config = CrawlConfig(keywords=keywords, media_sources=media_sources)
            self.save_crawl_config(config)
            logger.info(f"Configuration loaded with {len(media_sources)} media sources and {len(keywords)} keyword categories.")
            return config
        except Exception as e:
            logger.error(f"Could not load configuration: {e}")
            return CrawlConfig(keywords={}, media_sources=[])

    def _parse_media_list(self) -> List[MediaSource]:
        """Parse media list from config/media_sources.csv."""
        media_file = self.config_dir / "media_sources.csv"
        if not media_file.exists():
            logger.warning(f"{media_file} not found. No media sources will be loaded.")
            return []
        try:
            df = pd.read_csv(media_file).where(pd.notnull, None)
            return [MediaSource(stt=i + 1, **row) for i, row in enumerate(df.to_dict('records'))]
        except Exception as e:
            logger.error(f"Failed to parse {media_file}: {e}")
            return []

    def _load_keywords_config(self) -> Dict[str, List[str]]:
        """Load keywords from config/keywords.json."""
        keywords_file = self.config_dir / "keywords.json"
        if not keywords_file.exists():
            default_keywords = {"Dầu ăn": ["Tường An"]}
            self.save_keywords_config(default_keywords)
            return default_keywords
        try:
            with open(keywords_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            return {}

    def save_crawl_config(self, config: CrawlConfig):
        """Save the complete configuration to crawl_config.json."""
        config_file = self.config_dir / "crawl_config.json"
        try:
            with open(config_file, "w", encoding="utf-8") as f:
                f.write(config.model_dump_json(indent=2))
            logger.info(f"Crawl config saved to {config_file}")
        except Exception as e:
            logger.error(f"Failed to save crawl config: {e}")

    def save_keywords_config(self, keywords: Dict[str, List[str]]):
        """Save the keywords configuration to keywords.json."""
        keywords_file = self.config_dir / "keywords.json"
        try:
            with open(keywords_file, "w", encoding="utf-8") as f:
                json.dump(keywords, f, ensure_ascii=False, indent=2)
            if hasattr(self, 'crawl_config'):
                self.crawl_config.keywords = keywords
                self.save_crawl_config(self.crawl_config)
            logger.info(f"Keywords saved to {keywords_file}")
        except Exception as e:
            logger.error(f"Failed to save keywords: {e}")

settings = AppSettings()
