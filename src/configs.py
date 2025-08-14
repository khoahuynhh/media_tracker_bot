# src/configs.py
"""
Module to handle all application configuration.
Loads settings from .env and config files.
Phiên bản này đã được cập nhật để hỗ trợ đầy đủ các API endpoint
cần thiết cho frontend, bao gồm cả logic cập nhật API keys.
"""

import json
import logging
import os
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional
from functools import lru_cache

import pandas as pd
from dotenv import load_dotenv, set_key, find_dotenv

# Import models
from .models import CrawlConfig, MediaSource, MediaType

# Setup logging
logger = logging.getLogger(__name__)

# Define root and config paths
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"
MEDIA_SOURCES_FILE = CONFIG_DIR / "media_list.csv"


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
        if self._initialized and hasattr(self, "crawl_config"):
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
        for directory in [
            self.config_dir,
            self.data_dir,
            self.reports_dir,
            self.cache_dir,
            PROJECT_ROOT / "logs",
        ]:
            directory.mkdir(parents=True, exist_ok=True)
        logger.info("Directories setup verified.")

    def _setup_environment(self):
        """Load .env file and validate API keys."""
        env_file = self.project_root / ".env"
        if not env_file.exists():
            env_template = """# API Keys for Media Tracker Bot
            OPENAI_API_KEY=your_openai_api_key_here
            GROQ_API_KEY=your_groq_api_key_here
            DEFAULT_MODEL_PROVIDER=openai
            """
            env_file.write_text(env_template, encoding="utf-8")
            logger.warning(
                f"Created .env template at {env_file}. Please update it with your API keys."
            )

        load_dotenv(dotenv_path=env_file, override=True)
        self._validate_api_keys()

    def _validate_api_keys(self) -> bool:
        """Check if at least one API key is configured."""
        status = self.get_api_key_status()
        if (
            not status["openai_configured"]
            and not status["groq_configured"]
            and not status["google_configured"]
        ):
            logger.error(
                "FATAL: No valid API keys found in .env file. The application cannot run."
            )
            return False

        logger.info(
            f"API Key Status: OpenAI {'Configured' if status['openai_configured'] else 'Not Configured'}, "
            f"Groq {'Configured' if status['groq_configured'] else 'Not Configured'}, "
            f"Google {'Configured' if status['google_configured'] else 'Not Configured'}"
        )
        return True

    def get_api_key_status(self) -> Dict[str, Any]:
        """Get the current status of API key configurations."""
        openai_key = os.getenv("OPENAI_API_KEY", "")
        groq_key = os.getenv("GROQ_API_KEY", "")
        google_key = os.getenv("GOOGLE_API_KEY", "")
        return {
            "openai_configured": openai_key.startswith("sk-"),
            "groq_configured": groq_key.startswith("gsk_"),
            "google_configured": bool(google_key),
            "default_provider": os.getenv("DEFAULT_MODEL_PROVIDER", "openai"),
            "default_model_id": os.getenv("DEFAULT_MODEL_ID", ""),
        }

    # SỬA LỖI: Thêm hàm này để xử lý logic cập nhật API keys
    def update_api_keys(self, api_keys: Dict[str, Optional[str]]) -> Dict[str, Any]:
        """
        Updates the .env file with new API keys and reloads the environment.
        """
        env_file_path = self.project_root / ".env"
        if not env_file_path.exists():
            # Tạo file nếu chưa có
            env_file_path.touch()

        # Cập nhật các key được cung cấp
        if "openai_api_key" in api_keys:
            set_key(env_file_path, "OPENAI_API_KEY", api_keys["openai_api_key"] or "")
        if "groq_api_key" in api_keys:
            set_key(env_file_path, "GROQ_API_KEY", api_keys["groq_api_key"] or "")
        if "google_api_key" in api_keys:
            set_key(env_file_path, "GOOGLE_API_KEY", api_keys["google_api_key"] or "")
        if "default_provider" in api_keys:
            set_key(
                env_file_path,
                "DEFAULT_MODEL_PROVIDER",
                api_keys["default_provider"] or "openai",
            )
        if "default_model" in api_keys:
            set_key(env_file_path, "DEFAULT_MODEL_ID", api_keys["default_model"] or "")

        # Tải lại các biến môi trường từ file .env đã được cập nhật
        load_dotenv(dotenv_path=env_file_path, override=True)

        logger.info("API keys in .env file have been updated.")

        # Trả về trạng thái mới nhất
        return self.get_api_key_status()

    def set_default_provider(self, provider: str):
        env_file_path = self.project_root / ".env"
        set_key(env_file_path, "DEFAULT_MODEL_PROVIDER", provider)
        load_dotenv(dotenv_path=env_file_path, override=True)

    def _load_crawl_config(self) -> CrawlConfig:
        """Load the main crawling configuration by combining components."""
        try:
            media_sources = self._parse_media_list()
            keywords = self._load_keywords_config()

            config_file = self.config_dir / "crawl_config.json"
            other_configs = {}
            if config_file.exists():
                try:
                    with open(config_file, "r", encoding="utf-8") as f:
                        other_configs = json.load(f)
                except json.JSONDecodeError as e:
                    logger.warning(
                        f"Could not parse {config_file}: {e}, using defaults."
                    )

            config = CrawlConfig(
                keywords=keywords,
                media_sources=media_sources,
                date_range_days=other_configs.get("date_range_days", 30),
                max_articles_per_source=other_configs.get(
                    "max_articles_per_source", 50
                ),
                crawl_timeout=other_configs.get("crawl_timeout", 480),
                exclude_domains=other_configs.get("exclude_domains", []),
            )
            self.save_crawl_config(config)
            logger.info(
                f"Configuration loaded with {len(media_sources)} media sources and {len(keywords)} keyword categories."
            )
            return config
        except Exception as e:
            logger.error(f"Could not load configuration: {e}")
            return CrawlConfig(keywords={}, media_sources=[])

    def _parse_media_list(self) -> List[MediaSource]:
        """Parse media list from config/media_list.csv."""
        media_file = self.config_dir / "media_list.csv"
        if not media_file.exists():
            logger.warning(f"{media_file} not found. No media sources will be loaded.")
            return self.get_default_media_sources()
        try:
            df = pd.read_csv(media_file).where(pd.notnull, None)
            df = df.fillna("")
            records = df.to_dict("records")
            sources: List[MediaSource] = []
            for i, row in enumerate(records):
                try:
                    sources.append(MediaSource(stt=i + 1, **row))
                except Exception as row_err:
                    logger.error(
                        "Invalid row in media_list.csv at index %d: %s | row=%s",
                        i,
                        row_err,
                        row,
                    )
            if not sources:
                logger.warning(
                    "media_list.csv parsed but no valid rows were constructed."
                )
                return self.get_default_media_sources()
            return sources
        except Exception as e:
            logger.error(f"Failed to parse {media_file}: {e}", exc_info=True)
            return self.get_default_media_sources()

    def _load_keywords_config(self) -> Dict[str, List[str]]:
        """Load keywords from config/keywords.json."""
        keywords_file = self.config_dir / "keywords.json"
        if not keywords_file.exists():
            default_keywords = {"Dầu ăn": ["Tường An"]}
            self.save_keywords_config(default_keywords)
            logger.info("keywords.json not found. Created default keywords.")
            return default_keywords
        try:
            with open(keywords_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, dict):
                    raise ValueError(
                        "keywords.json must be a JSON object {industry: [keywords...]}"
                    )
                # Chuẩn hóa giá trị về list[str]
                normalized: Dict[str, List[str]] = {}
                for k, v in data.items():
                    if isinstance(v, list):
                        normalized[k] = [str(x) for x in v]
                    elif isinstance(v, str):
                        normalized[k] = [s.strip() for s in v.split(",") if s.strip()]
                    else:
                        logger.warning(
                            "Unexpected value type for '%s' in keywords.json: %r",
                            k,
                            type(v),
                        )
                        normalized[k] = []
                return normalized
        except json.JSONDecodeError as je:
            logger.error("keywords.json is not valid JSON: %s", je, exc_info=True)
            # Fallback an toàn, vẫn cho hệ thống chạy
            return {"Dầu ăn": ["Tường An"]}
        except Exception as e:
            logger.error("Failed to load keywords.json: %s", e, exc_info=True)
            return {"Dầu ăn": ["Tường An"]}

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

            if hasattr(self, "crawl_config"):
                self.crawl_config.keywords = keywords
                self.save_crawl_config(self.crawl_config)

            logger.info(f"Keywords saved to {keywords_file}")
        except Exception as e:
            logger.error(f"Failed to save keywords: {e}")

    @staticmethod
    def load_media_sources():
        sources = []
        with open(MEDIA_SOURCES_FILE, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sources.append(
                    MediaSource(
                        stt=int(row.get("stt") or len(sources) + 1),
                        id=row["id"],
                        name=row["name"],
                        type=row.get("type") or "news",
                        domain=row["domain"],
                    )
                )
        return sources

    def get_default_media_sources(self) -> List[MediaSource]:
        return [
            MediaSource(
                stt=1,
                id="vnexpress",
                name="VnExpress",
                type=MediaType.WEBSITE,
                domain="vnexpress.net",
            ),
            MediaSource(
                stt=2,
                id="thanhnien",
                name="Thanh Niên",
                type=MediaType.WEBSITE,
                domain="thanhnien.vn",
            ),
            MediaSource(
                stt=3,
                id="tuoitre",
                name="Tuổi Trẻ",
                type=MediaType.WEBSITE,
                domain="tuoitre.vn",
            ),
        ]

    def normalize_source_key(self, s: dict) -> str:
        """
        Trả về khóa chuẩn để định danh nguồn.
        Ưu tiên reference_name, nếu không có thì ghép type|domain.
        """
        return s.get("reference_name") or f"{s.get('type')}|{s.get('domain')}"


settings = AppSettings()
