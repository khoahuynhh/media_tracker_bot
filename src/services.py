# src/services.py
"""
Service Layer for the Media Tracker Bot.
This layer contains the core business logic and orchestrates the pipeline.
It decouples the API (main.py) from the agents.
"""

import os
import asyncio
import shutil
import logging
import re
import pandas as pd
import traceback
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, DefaultDict
from collections import defaultdict
from jose import jwt, JWTError
from jose.exceptions import ExpiredSignatureError
from fastapi.security import OAuth2PasswordBearer
from fastapi import HTTPException, Depends, Request, status
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment


from .models import CompetitorReport, BotStatus, MediaType, Article
from .agents import MediaTrackerTeam, AgentManager
from .configs import AppSettings
from .task_state import task_manager

# Attempt to import Celery task wrapper. If the `celery` package is not
# installed or the module cannot be imported, we fall back to using
# asyncio.create_task for background execution.
try:
    from .celery_worker import run_pipeline_task  # type: ignore[import]

    CELERY_AVAILABLE = True
except Exception:
    run_pipeline_task = None  # type: ignore[assignment]
    CELERY_AVAILABLE = False

logger = logging.getLogger(__name__)

SECRET_KEY = "your_secret_key"
ALGORITHM = "HS256"
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "120"))


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(request: Request) -> str:
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token missing"
        )

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(
            token, SECRET_KEY, algorithms=[ALGORITHM]
        )  # tự verify 'exp'
        return payload.get("sub")
    except ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired"
        )
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


class PipelineService:
    """
    Core pipeline logic for media tracking. This function handles crawling and report generation.
    It is intended to be called by background task workers.
    """

    def __init__(self, app_settings: AppSettings, user_email: str):
        self.settings = app_settings
        self.team: Optional[MediaTrackerTeam] = None
        self.status_by_source = {}
        self.user_email = user_email
        self.agent_manager = AgentManager.get_instance()

    def cancel_provider(self, session_id: str, provider: str) -> bool:
        try:
            ok = self.agent_manager.cancel_provider(
                session_id=session_id, provider=provider
            )
            if ok:
                import time

                t0 = time.monotonic()
                while time.monotonic() - t0 < 2.0:
                    if not getattr(self.agent_manager, "is_running", lambda *_: False)(
                        session_id, provider
                    ):
                        break
                    time.sleep(0.05)
            return ok
        except Exception:
            logging.exception(
                "cancel_provider failed (session=%s, provider=%s)", session_id, provider
            )
            return False

    def _source_key_of(self, s):
        ref = getattr(s, "reference_name", None)
        if ref:
            return str(ref).strip().lower()
        t = str(getattr(s, "type", "")).strip().lower()
        d = str(getattr(s, "domain", "")).strip().lower()
        return f"{t}|{d}"

    async def run_pipeline_logic(
        self,
        session_id: str,
        user_email: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        custom_keywords: Optional[Dict[str, List[str]]] = None,
        selected_sources: Optional[List[str]] = None,
    ) -> Optional[CompetitorReport]:
        logger.info(f"[{session_id}] Starting media tracking pipeline...")
        self.current_session_id = session_id

        session_config = self.settings.crawl_config.model_copy(deep=True)
        session_config.media_sources = self.settings.crawl_config.media_sources.copy()

        if custom_keywords:
            session_config.keywords = custom_keywords

        # NEW: lọc theo selected_sources nếu user chọn
        if selected_sources:
            sel = {str(x).strip().lower() for x in selected_sources}

            def _match(s):
                return self._source_key_of(s) in sel

            filtered = [s for s in session_config.media_sources if _match(s)]
            session_config.media_sources = filtered

        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                session_config.date_range_days = (end_dt - start_dt).days
            except ValueError:
                logger.error(
                    f"[{session_id}] Invalid date format. Using default date range."
                )
                start_dt = None
                end_dt = None
        else:
            start_dt = None
            end_dt = None

        # Lấy lại task hiện tại
        tasks = task_manager.get_tasks(user_email)
        task = next((t for t in tasks if t["session_id"] == session_id), None)
        articles_so_far = [Article(**a) for a in task.get("articles_so_far", [])]

        self.team = MediaTrackerTeam(
            config=session_config,
            start_date=start_dt,
            end_date=end_dt,
            session_id=session_id,
            user_email=user_email,
            on_progress_update=self._update_task_progress,
            check_cancelled=self.is_cancelled,
            check_paused=self.should_pause,
            articles_so_far=articles_so_far,
            source_status_list=task.get("source_status_list", []),
        )

        report = await asyncio.wait_for(
            self.team.run_full_pipeline(),
            timeout=self.settings.crawl_config.total_pipeline_timeout,
        )

        if report:
            await self._save_report(report, user_email)
            logger.info(f"[{session_id}] Pipeline completed successfully.")
        else:
            logger.warning(
                f"[{session_id}] Pipeline finished without generating a report."
            )
        return report

    def is_cancelled(self):
        tasks = task_manager.get_tasks(self.user_email)
        task = next(
            (t for t in tasks if t["session_id"] == self.current_session_id), None
        )
        return task and task["status"] == "cancelled"

    def should_pause(self):
        tasks = task_manager.get_tasks(self.user_email)
        task = next(
            (t for t in tasks if t["session_id"] == self.current_session_id), None
        )
        return task and task["status"] == "paused"

    def resume_task_worker(self, session_id):
        logger.info(f"[{session_id}] Resuming pipeline execution (resume_task_worker)")
        use_celery = CELERY_AVAILABLE and os.getenv("CELERY_ENABLED", "0") == "1"
        if use_celery:
            try:
                # Dispatch resumed execution to Celery
                run_pipeline_task.delay(
                    session_id=session_id,
                    user_email=self.user_email,
                    start_date=None,
                    end_date=None,
                    custom_keywords=None,
                    selected_sources=None,
                )
                return
            except Exception as exc:
                logger.exception(
                    f"Failed to dispatch Celery resume task for session {session_id}: {exc}"
                )
        # Fallback to local async task
        asyncio.create_task(self._task_worker(session_id))

    def _update_task_progress(
        self, source_name, completed, failed, progress, current_task
    ):
        all_sources = [s.name for s in self.settings.crawl_config.media_sources]

        current_tasks = task_manager.get_tasks(self.user_email)
        task = next(
            (t for t in current_tasks if t["session_id"] == self.current_session_id),
            None,
        )

        if task.get("status") not in ["pending", "running"]:
            return

        if task:
            # Lấy danh sách source đã completed từ team

            completed_or_failed = [
                s["source_name"]
                for s in self.team.source_status_list
                if s["status"] in ("completed", "failed")
            ]

            remaining = [s for s in all_sources if s not in completed_or_failed]

            task_manager.update_task(
                self.user_email,
                self.current_session_id,
                {
                    "current_source": source_name,
                    "completed_sources": completed,
                    "failed_sources": failed,
                    "progress": progress,
                    "current_task": current_task,
                    "status": "running",
                    "remaining_sources": remaining,
                    "source_status_list": self.team.source_status_list,  # Lưu luôn vào task
                },
            )

    async def retry_task_worker(app_settings, user_email, session_id):
        new_service = PipelineService(app_settings, user_email)
        await new_service._task_worker(session_id)

    async def _task_worker(self, session_id):
        tasks = task_manager.get_tasks(self.user_email)
        task = next((t for t in tasks if t["session_id"] == session_id), None)

        if not task:
            logger.info(f"[{session_id}] Task not found, stopping worker.")
            return

        status = task["status"]

        if status == "paused":
            logger.info(f"[{session_id}] Task is paused. Waiting to resume...")
            # Đợi cho đến khi không còn paused
            while True:
                current_status = task_manager.get_task_status(
                    self.user_email, session_id
                )
                if current_status == "paused":
                    await asyncio.sleep(2)
                elif current_status == "cancelled":
                    logger.info(
                        f"[{session_id}] Task was cancelled while paused. Exiting."
                    )
                    return  # 👉 Dừng ngay, không tiếp tục
                else:
                    break  # Resume hợp lệ
            logger.info(
                f"[{session_id}] Resumed. Checking if cancelled before proceeding..."
            )

        # Kiểm tra lại trạng thái trước khi tiếp tục
        current_status = task_manager.get_task_status(self.user_email, session_id)
        if current_status == "cancelled":
            logger.info(f"[{session_id}] Task is cancelled. Exiting worker.")
            return

        if current_status == "pending":
            logger.info(f"[{session_id}] Task is pending. Preparing pipeline setup...")
            task_manager.update_task(
                self.user_email,
                session_id,
                {
                    "status": "pending",
                    "current_task": "Preparing tools and resources...",
                    "progress": 0.0,
                },
            )
            await asyncio.sleep(5)  # Cho FE thấy trạng thái pending

            # Kiểm tra lại trạng thái trước khi chuyển sang running
            current_status = task_manager.get_task_status(self.user_email, session_id)
            if current_status == "cancelled":
                logger.info(
                    f"[{session_id}] Task was cancelled during pending. Exiting."
                )
                return

            task_manager.update_task(
                self.user_email,
                session_id,
                {
                    "status": "running",
                    "current_task": "Crawling... please wait.",
                    "progress": 0.0,
                },
            )

        # Bắt đầu pipeline thực sự
        try:
            logger.info(f"[{session_id}] Starting pipeline execution.")
            params = task["params"]

            await self.run_pipeline_logic(
                session_id,
                self.user_email,
                start_date=params["start_date"],
                end_date=params["end_date"],
                custom_keywords=params["custom_keywords"],
                selected_sources=params.get("selected_sources") or [],
            )
            task_manager.update_task(
                self.user_email, session_id, {"status": "completed", "progress": 100.0}
            )

        except asyncio.CancelledError:
            logger.warning(f"[{session_id}] ⚠️ Task forcefully cancelled.")
            task_manager.update_task(
                self.user_email,
                session_id,
                {
                    "status": "cancelled",
                    "current_task": "Pipeline was cancelled",
                },
            )

        except Exception as e:
            logger.error(
                f"[{session_id}] Pipeline failed: {e}\n{traceback.format_exc()}"
            )
            task["retry_count"] += 1
            if task["retry_count"] < task["max_retries"]:
                logger.info(
                    f"[{session_id}] Retry {task['retry_count']} / {task['max_retries']}"
                )
                task_manager.update_task(
                    self.user_email,
                    session_id,
                    {
                        "status": "pending",
                        "current_source": None,
                        "progress": 0.0,
                        "current_task": "Chuẩn bị retry sau lỗi",
                    },
                )
                await asyncio.sleep(5)
                asyncio.create_task(
                    self._task_worker(session_id)
                )  # Tự gọi lại để retry
            else:
                task_manager.update_task(
                    self.user_email, session_id, {"status": "failed", "error": str(e)}
                )

    def run_background_task(
        self,
        user_email,
        session_id,
        start_date=None,
        end_date=None,
        custom_keywords=None,
        selected_sources=None,
    ):
        if (
            selected_sources
            and isinstance(selected_sources, list)
            and len(selected_sources) > 0
        ):
            sel = {str(x).strip().lower() for x in selected_sources}

            def _match(s):
                return self._source_key_of(s) in sel

            media_sources = [
                s for s in self.settings.crawl_config.media_sources if _match(s)
            ]
        else:
            media_sources = self.settings.crawl_config.media_sources
        total_sources = len(media_sources)
        task_data = {
            "session_id": session_id,
            "status": "pending",
            "start_time": datetime.now().isoformat(),
            "progress": 0.0,
            "retry_count": 0,
            "max_retries": 2,
            # Thông tin cần cho dashboard
            "params": {
                "start_date": start_date,
                "end_date": end_date,
                "custom_keywords": custom_keywords,
                "selected_sources": selected_sources or [],
            },
            "current_source": None,
            "total_sources": total_sources,
            "completed_sources": 0,
            "failed_sources": 0,
            "current_task": "Khởi động pipeline",
            "source_status_list": [],
        }

        task_manager.add_task(self.user_email, task_data)
        task_manager.set_task_attr(
            self.user_email,
            session_id,
            "media_sources",
            [
                {
                    "source_key": self._source_key_of(s),
                    "name": s.name,
                    "type": s.type,
                    "domain": s.domain,
                    "reference_name": getattr(s, "reference_name", None),
                }
                for s in media_sources
            ],
        )

        # và lưu riêng danh sách key (dùng hiển thị/khôi phục)
        task_manager.set_task_attr(
            user_email,
            session_id,
            "selected_source_keys",
            [self._source_key_of(s) for s in media_sources],
        )

        # Choose between Celery and local async task execution
        use_celery = CELERY_AVAILABLE and os.getenv("CELERY_ENABLED", "0") == "1"
        if use_celery:
            # Dispatch to Celery worker; do not await
            try:
                run_pipeline_task.delay(
                    session_id=session_id,
                    user_email=user_email,
                    start_date=start_date,
                    end_date=end_date,
                    custom_keywords=custom_keywords,
                    selected_sources=selected_sources,
                )
            except Exception as exc:
                logger.exception(
                    f"Failed to dispatch Celery task for session {session_id}: {exc}"
                )
                # Fallback to local execution
                asyncio.create_task(self._task_worker(session_id))
        else:
            # Fall back to local asynchronous execution
            asyncio.create_task(self._task_worker(session_id))

        return session_id

    def get_status(self) -> Dict:
        """Gets the current status of the service and the running team."""
        return {"tasks": task_manager.get_tasks(self.user_email)}

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

                    worksheet = writer.sheets[sheet_name]

                    for idx, col_name in enumerate(df.columns):
                        col_letter = get_column_letter(idx + 1)
                        for row in range(2, len(df) + 2):  # Bỏ header
                            cell = worksheet[f"{col_letter}{row}"]
                            if col_name == "Cụm nội dung":
                                cell.alignment = Alignment(
                                    wrap_text=True, vertical="top"
                                )
                            else:
                                # Các cột khác: chỉ middle align
                                cell.alignment = Alignment(vertical="center")

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
                                # "Ngành hàng": a.nganh_hang,
                                # "Nhãn hàng": ", ".join(a.nhan_hang),
                                "Keywords": ", ".join(a.keywords_found),
                            }
                            for a in articles
                        ]
                    )
                    # Sequence STT
                    df["STT"] = range(1, len(df) + 1)
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
