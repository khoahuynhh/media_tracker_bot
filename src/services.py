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
import time

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, DefaultDict
from collections import defaultdict
from jose import jwt, JWTError
from jose.exceptions import ExpiredSignatureError
from fastapi.security import OAuth2PasswordBearer
from fastapi import HTTPException, Request, status
from openpyxl.styles import Alignment


from .models import CompetitorReport, Article
from .agents import MediaTrackerTeam, AgentManager
from .configs import AppSettings
from .task_state import task_manager
from .tasks import run, complete, mark_cancelled, fail


def _get_bool_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


# Attempt to import Celery task wrapper. If the `celery` package is not
# installed or the module cannot be imported, we fall back to using
# asyncio.create_task for background execution.
try:
    from .celery_worker import run_pipeline_task  # type: ignore[import]

    _celery_import_ok = True
except Exception:
    run_pipeline_task = None  # type: ignore[assignment]
    _celery_import_ok = False

# Final switch comes from .env: if CELERY_AVAILABLE=true and celery import is ok,
# Celery will be used; otherwise local async execution is used.
CELERY_AVAILABLE = _get_bool_env("CELERY_AVAILABLE", False) and _celery_import_ok

logger = logging.getLogger(__name__)

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your_secret_key")
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
        return task and task["status"] in ("cancelling", "cancelled")

    def should_pause(self):
        tasks = task_manager.get_tasks(self.user_email)
        task = next(
            (t for t in tasks if t["session_id"] == self.current_session_id), None
        )
        return task and task["status"] == "paused"

    def resume_task_worker(self, session_id):
        logger.info(f"[{session_id}] Resuming pipeline execution (resume_task_worker)")
        use_celery = CELERY_AVAILABLE
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

            # Compute a monotonic, source-based progress estimate to keep UI tight
            try:
                total_sources = max(1, len(all_sources))
                auto_progress = (len(completed_or_failed) / total_sources) * 100.0
                safe_progress = float(progress or 0.0)
                progress = max(min(100.0, auto_progress), min(100.0, safe_progress))
            except Exception:
                pass

            extra_fields = {}
            if isinstance(current_task, dict) and current_task.get("message_key"):
                extra_fields["message_key"] = current_task.get("message_key")
                extra_fields["message_params"] = current_task.get("params") or {}
                current_task_val = None
            else:
                # Heuristic: map common free-text messages to structured i18n keys
                current_task_val = current_task
                try:
                    s = str(current_task or "")
                    sl = s.lower()
                    if "crawling" in sl and source_name:
                        extra_fields["message_key"] = "task.crawling_source"
                        extra_fields["message_params"] = {"source": source_name}
                        current_task_val = None
                    elif "keyword" in sl and source_name:
                        # Attempt to parse keywords after ':'
                        kw = None
                        idx = sl.find("keyword")
                        if idx != -1:
                            part = s[idx:]
                            if ":" in part:
                                kw = part.split(":", 1)[1].strip()
                        extra_fields["message_key"] = "task.crawling_keywords"
                        extra_fields["message_params"] = {
                            "source": source_name,
                            "keywords": kw or "",
                        }
                        current_task_val = None
                    elif "retry" in sl:
                        import re

                        m = re.search(r"(\d+).*?(\d+)", sl)
                        if m:
                            extra_fields["message_key"] = "task.retrying"
                            extra_fields["message_params"] = {
                                "attempt": int(m.group(1)),
                                "max": int(m.group(2)),
                            }
                            current_task_val = None
                except Exception:
                    pass
            run(
                self.user_email,
                self.current_session_id,
                current_source=source_name,
                completed_sources=completed,
                failed_sources=failed,
                progress=progress,
                current_task=current_task_val,
                remaining_sources=remaining,
                source_status_list=self.team.source_status_list,
                **extra_fields,
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
                    self.agent_manager.cancel_all_providers(session_id)
                    raise asyncio.CancelledError()
                else:
                    break  # Resume hợp lệ
            logger.info(
                f"[{session_id}] Resumed. Checking if cancelled before proceeding..."
            )

        # Kiểm tra lại trạng thái trước khi tiếp tục
        current_status = task_manager.get_task_status(self.user_email, session_id)
        if current_status in ("cancelling", "cancelled"):
            logger.info(f"[{session_id}] Task is cancelled. Exiting worker.")
            self.agent_manager.cancel_all_providers(session_id)
            raise asyncio.CancelledError()

        if current_status == "pending":
            logger.info(f"[{session_id}] Task is pending. Preparing pipeline setup...")
            run(
                self.user_email,
                session_id,
                message_key="task.crawling_wait",
                message_params={},
                progress=0.0,
            )
            await asyncio.sleep(5)

            # Kiểm tra lại trạng thái trước khi chuyển sang running
            current_status = task_manager.get_task_status(self.user_email, session_id)
            if current_status in ("cancelling", "cancelled"):
                logger.info(
                    f"[{session_id}] Task was cancelled during pending. Exiting."
                )
                return

            # Do not override the status here; it has been set to "running" above.

        # Bắt đầu pipeline thực sự
        t0 = time.perf_counter()
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

            # Re-check in case cancellation came in right at the end
            current_status = task_manager.get_task_status(self.user_email, session_id)
            if current_status in ("cancelling", "cancelled"):
                mark_cancelled(self.user_email, session_id)
                return

            complete(self.user_email, session_id)

        except asyncio.CancelledError:
            logger.warning(f"[{session_id}] ⚠️ Task forcefully cancelled.")
            mark_cancelled(self.user_email, session_id)

        except Exception as e:
            logger.error(
                f"[{session_id}] Pipeline failed: {e}\n{traceback.format_exc()}"
            )
            task["retry_count"] += 1
            if task["retry_count"] < task["max_retries"]:
                logger.info(
                    f"[{session_id}] Retry {task['retry_count']} / {task['max_retries']}"
                )
                # Do NOT set a terminal state; keep task running for retry
                run(
                    self.user_email,
                    session_id,
                    message_key="task.retrying",
                    message_params={
                        "attempt": task["retry_count"],
                        "max": task["max_retries"],
                    },
                    progress=task.get("progress", 0.0),
                )
                await asyncio.sleep(5)
                asyncio.create_task(
                    self._task_worker(session_id)
                )  # Tự gọi lại để retry
            else:
                fail(self.user_email, session_id, str(e))

        finally:
            t1 = time.perf_counter()
            elapsed = t1 - t0
            logger.info(
                f"[{session_id}] Task worker finished in {elapsed:.2f} seconds."
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
        # Ensure task has updated_at and normalized state via transition helper
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
        use_celery = CELERY_AVAILABLE
        fallback_ok = os.getenv("CELERY_FALLBACK_LOCAL", "1") == "1"

        if use_celery:
            # Dispatch to Celery worker; do not await
            try:
                logger.info(
                    "[%s] Dispatching to Celery (CELERY_AVAILABLE=%s, broker=%s)",
                    session_id,
                    CELERY_AVAILABLE,
                    os.getenv("CELERY_BROKER_URL"),
                )
                run_pipeline_task.delay(
                    session_id=session_id,
                    user_email=user_email,
                    start_date=start_date,
                    end_date=end_date,
                    custom_keywords=custom_keywords,
                    selected_sources=selected_sources,
                )
                # Important: return here to avoid creating a local task
                return session_id
            except Exception as exc:
                logger.exception(
                    "Failed to dispatch Celery task for session %s: %s",
                    session_id,
                    exc,
                )
                if not fallback_ok:
                    # Do not fallback locally; surface the error to caller/UI
                    raise
                logger.warning(
                    "[%s] Falling back to local background task (CELERY_FALLBACK_LOCAL=1)",
                    session_id,
                )
                asyncio.create_task(self._task_worker(session_id))
        else:
            logger.info(
                "[%s] Running locally (CELERY_AVAILABLE=%s)",
                session_id,
                CELERY_AVAILABLE,
            )
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

        def _clean_one_line(s: str) -> str:
            s = re.sub(r"\s+", " ", str(s or "")).strip()
            return s

        def _dedup_preserve_order(items):
            seen = set()
            out = []
            for x in items:
                if not x:
                    continue
                if x not in seen:
                    seen.add(x)
                    out.append(x)
            return out

        # (tuỳ nhu cầu) thứ tự ngành cố định theo template của bạn
        TEMPLATE_INDUSTRIES = [
            "Dầu ăn",
            "Gia vị",
            "Gạo & Ngũ cốc",
            "Sữa (UHT)",
            "Baby Food",
            "Home Care",
        ]

        folder_name = self._sanitize_user_name(user_email)
        reports_dir = self.settings.reports_dir / folder_name
        reports_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        try:
            # ---------------- JSON ----------------
            json_file = reports_dir / f"report_{timestamp}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                f.write(report.model_dump_json(indent=2))

            # ---------------- Build Overall (mỗi ngành = 1 dòng, giữ TÊN đầu báo) ----------------
            brands_by_ind = defaultdict(list)  # industry -> [brand, ...]
            papers_by_ind = defaultdict(set)  # industry -> {paper, ...}
            count_by_ind = defaultdict(int)  # industry -> total articles

            for a in report.articles:
                ind = _clean_one_line(getattr(a, "nganh_hang", None))
                if not ind:
                    continue

                # brands
                has_brand = False
                for b in getattr(a, "nhan_hang", None) or []:
                    b = _clean_one_line(b)
                    if b:
                        has_brand = True
                        brands_by_ind[ind].append(b)
                if not has_brand:
                    brands_by_ind[ind].append("Không có")

                # papers
                if getattr(a, "dau_bao", None):
                    papers_by_ind[ind].add(_clean_one_line(a.dau_bao))

                # count
                count_by_ind[ind] += 1

            rows = []
            # Trước tiên ghi các ngành theo template (kể cả không có bài)
            for ind in TEMPLATE_INDUSTRIES:
                brands = _dedup_preserve_order(brands_by_ind.get(ind, []))
                nhan_hang_txt = ", ".join(brands)  # 1 dòng
                paper_names = _dedup_preserve_order(list(papers_by_ind.get(ind, set())))
                paper_names_txt = ", ".join(paper_names)  # 1 dòng
                rows.append(
                    {
                        "Ngành hàng": ind,
                        "Nhãn hàng": nhan_hang_txt,
                        "Các đầu báo": paper_names_txt,  # giữ TÊN
                        "Số lượng bài": count_by_ind.get(ind, 0),
                    }
                )

            # Sau đó thêm các ngành phát sinh (nếu có) ngoài template
            # extra_inds = sorted(set(brands_by_ind.keys()) - set(TEMPLATE_INDUSTRIES))
            # for ind in extra_inds:
            #     brands = _dedup_preserve_order(brands_by_ind[ind])
            #     nhan_hang_txt = ", ".join(brands)
            #     paper_names = _dedup_preserve_order(list(papers_by_ind[ind]))
            #     paper_names_txt = ", ".join(paper_names)
            #     rows.append({
            #         "Ngành hàng": ind,
            #         "Nhãn hàng": nhan_hang_txt,
            #         "Các đầu báo": paper_names_txt,
            #         "Số lượng bài": count_by_ind[ind],
            #     })

            overall_df = pd.DataFrame(rows)

            # ---------------- EXCEL ----------------
            excel_file = reports_dir / f"report_{timestamp}.xlsx"
            with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
                # 1) Summary - Overall
                overall_df.to_excel(writer, sheet_name="Summary - Overall", index=False)

                ws = writer.sheets["Summary - Overall"]
                # Định dạng: hạn chế xuống dòng, co chữ nếu cần
                ws.column_dimensions["A"].width = 18  # Ngành hàng
                ws.column_dimensions["B"].width = 48  # Nhãn hàng
                ws.column_dimensions["C"].width = 70  # Các đầu báo (tên dài)
                ws.column_dimensions["D"].width = 14  # Số lượng bài
                for r in range(2, ws.max_row + 1):
                    ws[f"A{r}"].alignment = Alignment(vertical="center")
                    ws[f"B{r}"].alignment = Alignment(
                        wrap_text=False, vertical="center"
                    )
                    ws[f"C{r}"].alignment = Alignment(
                        wrap_text=False, shrink_to_fit=True, vertical="center"
                    )
                    ws[f"D{r}"].alignment = Alignment(vertical="center")

                # 2) Summary theo ngành (1 sheet / ngành)
                for s in report.industry_summaries:
                    sheet_name = f"Summary - Ngành {s.nganh_hang[:25]}"
                    rows_ind = []

                    brand_candidates = _dedup_preserve_order(list(s.nhan_hang or []))
                    has_brandless = any(
                        (not (article.nhan_hang or []))
                        and article.nganh_hang == s.nganh_hang
                        for article in report.articles
                    )
                    if has_brandless and "Không có" not in brand_candidates:
                        brand_candidates.append("Không có")

                    for brand in brand_candidates:
                        # Lọc bài theo brand & ngành
                        if brand == "Không có":
                            related_articles = [
                                article
                                for article in report.articles
                                if not (article.nhan_hang or [])
                                and article.nganh_hang == s.nganh_hang
                            ]
                        else:
                            brand_clean = _clean_one_line(brand)
                            related_articles = [
                                article
                                for article in report.articles
                                if article.nganh_hang == s.nganh_hang
                                and any(
                                    _clean_one_line(b) == brand_clean
                                    for b in (article.nhan_hang or [])
                                )
                            ]

                        if not related_articles:
                            continue

                        # Cụm nội dung unique, sắp xếp ổn định
                        clusters = sorted(
                            {
                                _clean_one_line(article.cum_noi_dung)
                                for article in related_articles
                                if getattr(article, "cum_noi_dung", None)
                            }
                        )
                        cluster_text = (
                            "\n".join(f"{i+1}. {c}" for i, c in enumerate(clusters))
                            if clusters
                            else ""
                        )

                        rows_ind.append(
                            {
                                "Nhãn hàng": _clean_one_line(brand),
                                "Cụm nội dung": cluster_text,
                                "Số lượng bài": len(related_articles),
                            }
                        )

                    df_ind = pd.DataFrame(rows_ind)
                    df_ind.to_excel(writer, sheet_name=sheet_name, index=False)
                    ws_ind = writer.sheets[sheet_name]
                    ws_ind.column_dimensions["A"].width = 35
                    ws_ind.column_dimensions["B"].width = 70
                    ws_ind.column_dimensions["C"].width = 16
                    # wrap cho cột “Cụm nội dung”
                    for r in range(2, ws_ind.max_row + 1):
                        ws_ind[f"A{r}"].alignment = Alignment(
                            wrap_text=False, vertical="center"
                        )
                        ws_ind[f"B{r}"].alignment = Alignment(
                            wrap_text=True, vertical="top"
                        )
                        ws_ind[f"C{r}"].alignment = Alignment(vertical="center")

                # 3) Mỗi nhãn hàng một sheet bài báo (đặt ngành theo đa số)
                articles_by_brand: DefaultDict[str, List[Article]] = defaultdict(list)
                articles_no_brand: List[Article] = []

                for article in report.articles:
                    if article.nhan_hang:
                        for b in article.nhan_hang:
                            articles_by_brand[b].append(article)
                    else:
                        articles_no_brand.append(article)

                for brand, articles in articles_by_brand.items():
                    # Ngành theo đa số
                    if articles:
                        ind_majority = Counter(
                            _clean_one_line(a.nganh_hang)
                            for a in articles
                            if getattr(a, "nganh_hang", None)
                        ).most_common(1)[0][0]
                    else:
                        ind_majority = ""

                    sheet_name = f"Ngành {ind_majority} - {brand}"
                    sheet_name = sheet_name[:31]  # Excel limit

                    df_brand = pd.DataFrame(
                        [
                            {
                                "STT": a.stt,
                                "Ngày phát hành": a.ngay_phat_hanh.strftime("%d/%m/%Y"),
                                "Đầu báo": a.dau_bao,
                                "Cụm nội dung": a.cum_noi_dung_chi_tiet
                                or a.cum_noi_dung,
                                "Tóm tắt nội dung": a.tom_tat_noi_dung,
                                "Link bài báo": a.link_bai_bao,
                                "Keywords": ", ".join(a.keywords_found or []),
                            }
                            for a in articles
                        ]
                    )
                    df_brand["STT"] = range(1, len(df_brand) + 1)
                    df_brand = df_brand[
                        ["STT"] + [c for c in df_brand.columns if c != "STT"]
                    ]
                    df_brand.to_excel(writer, sheet_name=sheet_name, index=False)

                # 4) Sheet cho các bài không có nhãn hàng
                if articles_no_brand:
                    ind_first = _clean_one_line(articles_no_brand[0].nganh_hang)
                    sheet_name = f"General - {ind_first}"
                    sheet_name = sheet_name[:31]

                    df_gen = pd.DataFrame(
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
                    df_gen["STT"] = range(1, len(df_gen) + 1)
                    df_gen = df_gen[["STT"] + [c for c in df_gen.columns if c != "STT"]]
                    df_gen.to_excel(writer, sheet_name=sheet_name, index=False)

            # ---------------- latest symlink/copy ----------------
            for ext in ["json", "xlsx"]:
                latest_path = reports_dir / f"latest_report.{ext}"
                target_file = reports_dir / f"report_{timestamp}.{ext}"
                if latest_path.exists() or latest_path.is_symlink():
                    latest_path.unlink()
                shutil.copy(target_file, latest_path)

            logger.info(f"Report saved to {json_file} and {excel_file}")
        except Exception as e:
            logger.error(f"Failed to save report: {e}", exc_info=True)
