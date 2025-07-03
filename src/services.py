# src/services.py
"""
Service Layer for the Media Tracker Bot.
This layer contains the core business logic and orchestrates the pipeline.
It decouples the API (main.py) from the agents.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional
import threading

from .models import CompetitorReport, CrawlConfig, BotStatus, MediaType
from .agents import MediaTrackerTeam
from .configs import AppSettings, settings

logger = logging.getLogger(__name__)

class PipelineService:
    """
    Manages the execution and state of the media tracking pipeline.
    """
    def __init__(self, app_settings: AppSettings):
        self.settings = app_settings
        self.team: Optional[MediaTrackerTeam] = None
        self.is_running = False
        self.current_session_id: Optional[str] = None
        self.stop_event = threading.Event()

    async def run_pipeline(
        self,
        session_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        custom_keywords: Optional[Dict[str, List[str]]] = None,
    ) -> Optional[CompetitorReport]:
        """
        Runs the full tracking pipeline for a given session.
        """
        if self.is_running:
            logger.warning(f"Attempted to run pipeline for session {session_id}, but a pipeline is already running for session {self.current_session_id}.")
            return None

        self.is_running = True
        self.current_session_id = session_id
        self.stop_event.clear()

        try:
            logger.info(f"[{session_id}] Starting media tracking pipeline...")

            session_config = self.settings.crawl_config.model_copy(deep=True)
            
            website_sources = [
                source for source in session_config.media_sources 
                if source.type == MediaType.WEBSITE
            ]
            session_config.media_sources = website_sources
            logger.info(f"Filtered for website sources only. Total to crawl: {len(website_sources)}")


            if custom_keywords:
                session_config.keywords = custom_keywords
            if start_date and end_date:
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    session_config.date_range_days = (end_dt - start_dt).days
                except ValueError:
                    logger.error(f"[{session_id}] Invalid date format. Using default date range.")

            self.team = MediaTrackerTeam(config=session_config, stop_event=self.stop_event)
            
            report = await self.team.run_full_pipeline()

            if report:
                await self._save_report(report)
                logger.info(f"[{session_id}] Pipeline completed successfully.")
            else:
                logger.warning(f"[{session_id}] Pipeline finished without generating a report.")
            
            return report

        except Exception as e:
            logger.error(f"[{session_id}] Pipeline failed with an unhandled exception: {e}", exc_info=True)
            if self.team:
                self.team.status.current_task = f"Failed: {e}"
            return None
        finally:
            self.is_running = False
            self.current_session_id = None
            if self.team:
                self.team.status.is_running = False

    def stop_pipeline(self, session_id: str) -> bool:
        """Requests a graceful stop of the running pipeline."""
        if not self.is_running or self.current_session_id != session_id:
            logger.warning(f"Stop request for session {session_id} ignored. Running session is {self.current_session_id}.")
            return False
        
        logger.info(f"Requesting pipeline stop for session {self.current_session_id}...")
        self.stop_event.set()
        if self.team:
            self.team.status.current_task = "Stopping..."
        return True

    def get_status(self) -> Dict:
        """Gets the current status of the service and the running team."""
        if self.is_running and self.team:
            team_status = self.team.get_status()
        else:
            team_status = BotStatus(is_running=False, current_task="Idle")
        
        website_sources_count = len([
            source for source in self.settings.crawl_config.media_sources 
            if source.type == MediaType.WEBSITE
        ])

        status_data = {
            "is_running": self.is_running,
            "session_id": self.current_session_id,
            "team_status": team_status.model_dump(),
            "api_keys": self.settings.get_api_key_status(),
            "total_media_sources": website_sources_count
        }
        
        return status_data

    async def _save_report(self, report: CompetitorReport):
        """Saves the report to JSON and Excel files."""
        import pandas as pd
        from pathlib import Path
        
        reports_dir = self.settings.reports_dir
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        try:
            # Save JSON
            json_file = reports_dir / f"report_{timestamp}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                f.write(report.model_dump_json(indent=2))

            # KHÔI PHỤC LOGIC GỐC: Xuất file Excel chi tiết
            excel_file = reports_dir / f"report_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                # Sheet 1: Summary
                summary_data = []
                for industry_summary in report.industry_summaries:
                    summary_data.append({
                        "Ngành hàng": industry_summary.nganh_hang.value,
                        "Nhãn hàng": ", ".join(industry_summary.nhan_hang),
                        "Cụm nội dung": ", ".join(c.value for c in industry_summary.cum_noi_dung),
                        "Số lượng bài": industry_summary.so_luong_bai,
                        "Các đầu báo": ", ".join(industry_summary.cac_dau_bao),
                    })
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name="Summary", index=False)

                # Sheet 2: Articles
                articles_data = []
                for article in report.articles:
                    articles_data.append({
                        "STT": article.stt,
                        "Ngày phát hành": article.ngay_phat_hanh.strftime("%d/%m/%Y"),
                        "Đầu báo": article.dau_bao,
                        "Cụm nội dung": article.cum_noi_dung.value,
                        "Tóm tắt nội dung": article.tom_tat_noi_dung,
                        "Link bài báo": str(article.link_bai_bao),
                        "Ngành hàng": article.nganh_hang.value,
                        "Nhãn hàng": ", ".join(article.nhan_hang),
                        "Keywords": ", ".join(article.keywords_found),
                    })
                df_articles = pd.DataFrame(articles_data)
                df_articles.to_excel(writer, sheet_name="Articles", index=False)

            # Update latest symlinks
            for ext in ["json", "xlsx"]:
                latest_path = reports_dir / f"latest_report.{ext}"
                target_file = reports_dir / f"report_{timestamp}.{ext}"
                if latest_path.exists() or latest_path.is_symlink():
                    latest_path.unlink()
                latest_path.symlink_to(target_file.name)

            logger.info(f"Report saved to {json_file} and {excel_file}")
        except Exception as e:
            logger.error(f"Failed to save report: {e}", exc_info=True)


pipeline_service = PipelineService(app_settings=settings)
