# src/services.py
"""
Service Layer for the Media Tracker Bot.
This layer contains the core business logic and orchestrates the pipeline.
It decouples the API (main.py) from the agents.
"""

import asyncio
import shutil
import logging
import threading
import re
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, DefaultDict
from collections import defaultdict
from jose import jwt, JWTError
from fastapi.security import OAuth2PasswordBearer
from fastapi import HTTPException, Depends


from .models import CompetitorReport, BotStatus, MediaType, Article
from .agents import MediaTrackerTeam
from .configs import AppSettings

logger = logging.getLogger(__name__)

SECRET_KEY = "your_secret_key"
ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")


def create_access_token(data: dict, expires_delta: timedelta = timedelta(hours=12)):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + expires_delta
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload.get("sub")
    except JWTError:
        raise HTTPException(status_code=401, detail="Token is invalid or expired")


class PipelineService:
    """
    Manages the execution and state of the media tracking pipeline.
    """

    def __init__(self, app_settings: AppSettings, user_email: str):
        self.settings = app_settings
        self.team: Optional[MediaTrackerTeam] = None
        self.is_running = False
        self.pause_event = threading.Event()
        self.current_session_id: Optional[str] = None
        self.stop_event = threading.Event()
        self.status_by_source = {}
        self.user_email = user_email

    async def run_pipeline(
        self,
        session_id: str,
        user_email: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        custom_keywords: Optional[Dict[str, List[str]]] = None,
    ) -> Optional[CompetitorReport]:
        """
        Runs the full tracking pipeline for a given session.
        """
        if self.is_running:
            logger.warning(
                f"Attempted to run pipeline for session {session_id}, but a pipeline is already running for session {self.current_session_id}."
            )
            return None

        self.is_running = True
        self.current_session_id = session_id
        self.stop_event.clear()

        try:
            logger.info(f"[{session_id}] Starting media tracking pipeline...")

            session_config = self.settings.crawl_config.model_copy(deep=True)
            website_sources = [
                source
                for source in session_config.media_sources
                if source.type == MediaType.WEBSITE
            ]
            session_config.media_sources = website_sources

            if custom_keywords:
                session_config.keywords = custom_keywords
            if start_date and end_date:
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    session_config.date_range_days = (end_dt - start_dt).days
                except ValueError:
                    logger.error(
                        f"[{session_id}] Invalid date format. Using default date range."
                    )

            self.pause_event.clear()
            self.team = MediaTrackerTeam(
                config=session_config,
                stop_event=self.stop_event,
                start_date=start_dt,
                end_date=end_dt,
                pause_event=self.pause_event,
            )

            report = await asyncio.wait_for(
                self.team.run_full_pipeline(),
                timeout=self.settings.crawl_config.total_pipeline_timeout,
            )

            if report:
                await self._save_report(report, user_email=user_email)
                logger.info(f"[{session_id}] Pipeline completed successfully.")
            else:
                logger.warning(
                    f"[{session_id}] Pipeline finished without generating a report."
                )

            return report

        except Exception as e:
            logger.error(
                f"[{session_id}] Pipeline failed with an unhandled exception: {e}",
                exc_info=True,
            )
            if self.team:
                self.team.status.current_task = f"Failed: {e}"
            return None
        finally:
            self.is_running = False
            self.current_session_id = None
            if self.team:
                self.team.status.is_running = False
            self.team.cleanup()

    def pause_pipeline(self):
        if self.is_running and not self.pause_event.is_set():
            self.pause_event.set()
            logger.info("⏸ Pipeline paused.")

    def resume_pipeline(self):
        if self.is_running and self.pause_event.is_set():
            self.pause_event.clear()
            logger.info("▶️ Pipeline resumed.")

    def stop_pipeline(self, session_id: str) -> bool:
        if self.is_running and self.current_session_id == session_id:
            logger.info("Pipeline cancelled by user.")
            self.stop_event.set()
            self.is_running = False
            self.is_paused = False
            if self.team:
                self.team.cleanup()
            return True
        return False

    def get_status(self) -> Dict:
        """Gets the current status of the service and the running team."""
        # SỬA LỖI: Luôn trả về một đối tượng BotStatus hợp lệ
        if self.is_running and self.team:
            team_status = self.team.get_status()
        else:
            # Nếu không chạy, tạo một đối tượng BotStatus mặc định
            team_status = BotStatus(is_running=False, current_task="Idle")

        status_data = {
            "is_running": self.is_running,
            "session_id": self.current_session_id,  # Đổi tên từ current_session_id
            "team_status": team_status.model_dump(),
            "api_keys": self.settings.get_api_key_status(),
            "total_media_sources": len(self.settings.crawl_config.media_sources),
            "is_Paused": self.pause_event.is_set(),
        }

        return status_data

    def _sanitize_user_name(self, username: str) -> str:
        """Trả về tên thư mục an toàn từ tên user."""
        username = username.strip().lower()
        sanitized = re.sub(r"[^a-zA-Z0-9]+", "_", username)
        return sanitized.strip("_") or "unknown_user"

    async def _save_report(self, report: CompetitorReport, user_email: str):
        """Saves the report to JSON and Excel files."""

        folder_name = self._sanitize_user_name(user_email)
        reports_dir = self.settings.reports_dir / folder_name
        reports_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        try:
            json_file = reports_dir / f"report_{timestamp}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                f.write(report.model_dump_json(indent=2))

            excel_file = reports_dir / f"report_{timestamp}.xlsx"
            # articles_df = pd.DataFrame([a.model_dump() for a in report.articles])
            # summary_df = pd.DataFrame(
            #     [s.model_dump() for s in report.industry_summaries]
            # )

            # Format Summary - Overall
            overall_df = pd.DataFrame(
                [
                    {
                        "Ngành hàng": s.nganh_hang,
                        "Nhãn hàng": ", ".join(s.nhan_hang),
                        "Các đầu báo": ", ".join(s.cac_dau_bao),
                        "Số lượng bài": s.so_luong_bai,
                    }
                    for s in report.overall_summary.industries
                ]
            )

            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                # 1. Summary tổng quan
                overall_df.to_excel(writer, sheet_name="Summary - Overall", index=False)

                # 2. Summary theo ngành (1 sheet mỗi ngành)
                for s in report.industry_summaries:
                    sheet_name = (
                        f"Summary - Ngành {s.nganh_hang[:25]}"  # Giới hạn 31 ký tự
                    )

                    rows = []

                    for brand in s.nhan_hang:
                        # Lọc các bài báo có chứa nhãn hàng này
                        related_articles = [
                            article
                            for article in report.articles
                            if brand in article.nhan_hang
                            and article.nganh_hang == s.nganh_hang
                        ]

                        # Lấy các cụm nội dung xuất hiện trong các bài báo đó (không trùng)
                        clusters = set(
                            article.cum_noi_dung
                            for article in related_articles
                            if article.cum_noi_dung
                        )

                        # Gộp cụm nội dung thành chuỗi có đánh số
                        if clusters:
                            cluster_text = "\n".join(
                                [f"{idx+1}. {c}" for idx, c in enumerate(clusters)]
                            )
                        else:
                            cluster_text = ""

                        # Số lượng bài là tổng số bài có brand này (không phân cụm)
                        count = len(related_articles)

                        rows.append(
                            {
                                "Ngành hàng": s.nganh_hang,
                                "Nhãn hàng": brand,
                                "Cụm nội dung": cluster_text,
                                "Số lượng bài": count,
                            }
                        )

                    # Bổ sung dòng cho các bài không có nhãn hàng
                    no_brand_articles = [
                        article
                        for article in report.articles
                        if not article.nhan_hang and article.nganh_hang == s.nganh_hang
                    ]

                    if no_brand_articles:
                        # Lấy các cụm nội dung của bài không có nhãn
                        clusters_no_brand = set(
                            article.cum_noi_dung
                            for article in no_brand_articles
                            if article.cum_noi_dung
                        )

                        if clusters_no_brand:
                            cluster_text = "\n".join(
                                [
                                    f"{idx+1}. {c}"
                                    for idx, c in enumerate(clusters_no_brand)
                                ]
                            )
                        else:
                            cluster_text = ""

                        count = len(no_brand_articles)

                        rows.append(
                            {
                                "Ngành hàng": s.nganh_hang,
                                "Nhãn hàng": "Không nhãn hàng",
                                "Cụm nội dung": cluster_text,
                                "Số lượng bài": count,
                            }
                        )

                    # Tạo DataFrame và ghi vào Excel
                    df = pd.DataFrame(rows)
                    df.to_excel(writer, index=False, sheet_name=sheet_name)

                # 3. Mỗi nhãn hàng một sheet bài báo

                # Gom bài theo nhãn hàng
                articles_by_brand: DefaultDict[str, List[Article]] = defaultdict(list)
                articles_no_brand: List[Article] = []
                for article in report.articles:
                    if article.nhan_hang:
                        for brand in article.nhan_hang:
                            articles_by_brand[brand].append(article)
                    else:
                        articles_no_brand.append(article)

                # Ghi mỗi nhãn hàng ra 1 sheet
                for brand, articles in articles_by_brand.items():
                    first_article = articles[0]
                    sheet_name = f"Ngành {first_article.nganh_hang[:31]} - {brand[:25]}"  # Giới hạn tên sheet
                    df = pd.DataFrame(
                        [
                            {
                                "STT": a.stt,
                                "Ngày phát hành": a.ngay_phat_hanh.strftime("%d/%m/%Y"),
                                "Đầu báo": a.dau_bao,
                                "Cụm nội dung": a.cum_noi_dung_chi_tiet
                                or a.cum_noi_dung,
                                "Tóm tắt nội dung": a.tom_tat_noi_dung,
                                "Link bài báo": a.link_bai_bao,
                                # "Ngành hàng": a.nganh_hang,
                                # "Nhãn hàng": ", ".join(a.nhan_hang),
                                "Keywords": ", ".join(a.keywords_found),
                            }
                            for a in articles
                        ]
                    )
                    # Sequence STT
                    df["STT"] = range(1, len(df) + 1)
                    # Nếu muốn nó ở đầu tiên:
                    df = df[["STT"] + [col for col in df.columns if col != "STT"]]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

                # 4. Ghi sheet cho các bài không có nhãn hàng
                if articles_no_brand:
                    first_article = articles_no_brand[0]
                    industry = first_article.nganh_hang
                    sheet_name = f"General - {industry}"
                    sheet_name = sheet_name[:31]  # Đảm bảo không quá 31 ký tự

                    df = pd.DataFrame(
                        [
                            {
                                "STT": a.stt,
                                "Ngày phát hành": a.ngay_phat_hanh.strftime("%d/%m/%Y"),
                                "Đầu báo": a.dau_bao,
                                "Cụm nội dung": a.cum_noi_dung_chi_tiet
                                or a.cum_noi_dung,
                                "Tóm tắt nội dung": a.tom_tat_noi_dung,
                                "Link bài báo": a.link_bai_bao,
                            }
                            for a in articles_no_brand
                        ]
                    )
                    df["STT"] = range(1, len(df) + 1)
                    df = df[["STT"] + [col for col in df.columns if col != "STT"]]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

            for ext in ["json", "xlsx"]:
                latest_path = reports_dir / f"latest_report.{ext}"
                target_file = reports_dir / f"report_{timestamp}.{ext}"
                if latest_path.exists() or latest_path.is_symlink():
                    latest_path.unlink()
                shutil.copy(target_file, latest_path)

            logger.info(f"Report saved to {json_file} and {excel_file}")
        except Exception as e:
            logger.error(f"Failed to save report: {e}", exc_info=True)
