import json
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


class SafeCacheManager:
    def __init__(self, cache_dir="cache/crawl_results", ttl_hours=8, version="1.0"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_hours = ttl_hours
        self.version = version

    def make_cache_key(
        self, media_source_name, industry_name, keywords, start_date, end_date
    ):
        raw = f"{media_source_name}-{industry_name}-{'-'.join(keywords)}-{start_date}-{end_date}"
        return hashlib.md5(raw.encode()).hexdigest()

    def save_cache(self, cache_key: str, crawl_result):
        cache_file = self.cache_dir / f"{cache_key}.json"
        cache_data = {
            "cache_version": self.version,
            "created_at": datetime.now().isoformat(),
            "ttl_hours": self.ttl_hours,
            "crawl_status": crawl_result.crawl_status,
            "data": crawl_result.model_dump(mode="json"),
        }
        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, ensure_ascii=False, indent=2)

    def load_cache(self, cache_key: str) -> Optional[dict]:
        cache_file = self.cache_dir / f"{cache_key}.json"
        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cache = json.load(f)

            # Check version
            if cache.get("cache_version") != self.version:
                return None

            # Check TTL
            created_at = datetime.fromisoformat(cache["created_at"])
            if datetime.now() - created_at > timedelta(
                hours=cache.get("ttl_hours", self.ttl_hours)
            ):
                return None

            # Validate cache success
            if cache.get("crawl_status") != "success":
                return None

            if not cache["data"].get("articles_found"):
                return None

            return cache["data"]

        except Exception:
            return None
