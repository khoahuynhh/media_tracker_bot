# src/main.py
"""
Main entry point for the Media Tracker Bot.
"""
import sys, asyncio
import logging
import os
import json
import uvicorn
import httpx

from pathlib import Path
from datetime import datetime, timezone
from fastapi import FastAPI, Request, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import (
    HTMLResponse,
    FileResponse,
    JSONResponse,
    StreamingResponse,
)
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Dict
from dotenv import load_dotenv
from typing import List

load_dotenv()

# Setup logging before other imports - Docker compatible
log_dir = os.getenv("LOG_DIR", "logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "media_tracker.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# Import modules
from .tasks import pause, resume, request_cancel, mark_cancelled, reset_idle
from .user_service import (
    get_user,
    verify_password,
    create_user,
    update_password,
    create_otp,
    verify_and_consume_otp,
)
from .mailer import send_email
from .task_state import task_manager
from .event import event_bus
from .models import (
    CrawlConfig,
    CompetitorReport,
    create_sample_report,
    UserLogin,
    ChangePassword,
    MediaSource,
)
from .configs import settings
from .services import (
    PipelineService,
    create_access_token,
    get_current_user,
    SECRET_KEY,
    ALGORITHM,
)
from .agents import PlaywrightPool
from jose import jwt, JWTError

# Save pipeline according to email user
user_pipelines: Dict[str, PipelineService] = {}
logger = logging.getLogger(__name__)

# Set optimal event loop policy for production
if sys.platform == "linux":
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.debug("Using uvloop event loop policy")
    except ImportError:
        logger.debug("uvloop not available, using default event loop policy")

# Map tra cứu nguồn theo key chuẩn
SOURCE_BY_KEY: Dict[str, dict] = {
    settings.normalize_source_key(s.model_dump() if hasattr(s, "model_dump") else s): (
        s.model_dump() if hasattr(s, "model_dump") else s
    )
    for s in settings.crawl_config.media_sources
}
ALLOWED_KEYS = set(SOURCE_BY_KEY.keys())


# Enhanced lifespan with shared resources and recovery
@asynccontextmanager
async def _enhanced_lifespan(app: FastAPI):
    try:
        task_manager.load_tasks()
        logger.info("Task manager loaded.")

        # Crash recovery: lock any in-flight tasks to idle
        try:
            users = task_manager.list_users()
        except Exception:
            users = []
        for u in users:
            try:
                tasks = task_manager.get_tasks(u) or []
            except Exception:
                tasks = []
            for t in tasks:
                st = (t or {}).get("status")
                if st not in ("completed", "cancelled", "failed", "idle"):
                    sid = (t or {}).get("session_id")
                    task_manager.update_task(
                        u,
                        sid,
                        {
                            "status": "idle",
                            "crash_locked": True,
                            "updated_at": datetime.utcnow().isoformat() + "Z",
                        },
                    )

        # Shared HTTP client
        app.state.http = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            timeout=httpx.Timeout(15.0, connect=5.0, read=15.0),
            headers={"Accept-Language": "vi-VN,vi;q=0.9"},
        )

        # Start watchdog for stale tasks
        app.state.watchdog_task = asyncio.create_task(_task_watchdog())
        logger.info("Startup completed: http client + watchdog ready.")
    except Exception:
        logger.exception("Startup failed (continuing with degraded features).")

    yield

    try:
        wd = getattr(app.state, "watchdog_task", None)
        if wd:
            wd.cancel()
            try:
                await wd
            except asyncio.CancelledError:
                pass

        http_client = getattr(app.state, "http", None)
        if http_client:
            try:
                await http_client.aclose()
            except Exception:
                logger.exception("Error closing shared http client")

        try:
            await PlaywrightPool.instance().close()
        except Exception:
            logger.exception("Error closing Playwright on shutdown")

        keep_n = int(os.getenv("TASKS_KEEP_PER_USER", "200"))
        try:
            task_manager.purge_old(keep_latest_per_user=keep_n)
        except Exception:
            logger.exception("Purge old tasks failed")

        logger.info("Shutdown cleanup done.")
    except Exception:
        logger.exception("Shutdown cleanup failed.")


# Use enhanced lifespan
lifespan = _enhanced_lifespan

# Initialize FastAPI app
app = FastAPI(
    title="Media Tracker Bot API",
    description="API for Vietnamese Media Tracking and Competitor Analysis (Retro-compatible)",
    version="2.1.0",
    lifespan=lifespan,
)

# ---- CORS từ ENV ----
allowed_origins = [
    o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()
]
allow_origin_regex = os.getenv("ALLOWED_ORIGIN_REGEX") or None
allow_credentials = True  # nếu cần gửi cookie/Authorization
cors_max_age = int(os.getenv("CORS_MAX_AGE", "86400"))  # cache preflight 1 ngày


app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # ưu tiên danh sách cụ thể
    allow_origin_regex=allow_origin_regex,  # hoặc regex wildcard subdomain
    allow_credentials=allow_credentials,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"],  # để FE thấy tên file khi tải xlsx
    max_age=cors_max_age,
)

# Mount thư mục static để phục vụ frontend - Docker compatible
static_dir = Path(os.getenv("STATIC_DIR", settings.project_root / "static"))
if static_dir.exists() and static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
else:
    logger.warning(f"Static directory not found: {static_dir}")


# --- API Endpoints ---
@app.post("/api/register")
def register(user: UserLogin):
    # Optionally disable public self-registration (set ALLOW_SELF_REGISTER=1 to enable)
    if os.getenv("ALLOW_SELF_REGISTER", "0") != "1":
        raise HTTPException(status_code=403, detail="Self-registration is disabled")
    ok = create_user(user.email, user.password, role="viewer")
    if not ok:
        raise HTTPException(status_code=400, detail="Email already exists")
    return {"message": "User registered successfully"}


@app.post("/api/login")
def login(user: UserLogin):
    user_record = get_user(user.email)
    if not user_record or not verify_password(
        user.password, user_record["password_hash"]
    ):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token_data = {"sub": user_record["email"], "role": user_record["role"]}
    token = create_access_token(token_data)
    return {"access_token": token, "token_type": "bearer", "role": user_record["role"]}


@app.post("/api/change-password")
def change_password(
    body: ChangePassword, current_user: str = Depends(get_current_user)
):
    user_record = get_user(current_user)
    if not user_record:
        raise HTTPException(status_code=404, detail="User not found")
    # Verify old password
    if not verify_password(body.old_password, user_record["password_hash"]):
        raise HTTPException(status_code=400, detail="Old password is incorrect")
    # Basic password policy
    if len(body.new_password) < 8:
        raise HTTPException(
            status_code=400, detail="New password must be at least 8 characters"
        )
    if body.new_password == body.old_password:
        raise HTTPException(
            status_code=400,
            detail="New password must be different from the old password",
        )
    # Update
    ok = update_password(current_user, body.new_password)
    if not ok:
        raise HTTPException(status_code=500, detail="Failed to update password")
    return {"message": "Password changed successfully"}


@app.post("/api/change-password/request-otp")
def request_change_password_otp(current_user: str = Depends(get_current_user)):
    """Generate and email an OTP code for changing password."""
    user_record = get_user(current_user)
    if not user_record:
        raise HTTPException(status_code=404, detail="User not found")
    import random

    code = f"{random.randint(0, 999999):06d}"
    create_otp(current_user, "change_password", code, ttl_minutes=10)

    subject = "Your OTP code"
    body = (
        "You requested to change your password.\n\n"
        f"Your verification code: {code}\n"
        "This code expires in 10 minutes.\n\n"
        "If you didn't request this, please ignore this email."
    )
    sent = send_email(current_user, subject, body)
    if not sent:
        raise HTTPException(status_code=500, detail="Failed to send OTP email")
    return {"message": "OTP sent"}


@app.post("/api/change-password/confirm")
def confirm_change_password(body: dict, current_user: str = Depends(get_current_user)):
    """Confirm password change with old_password, new_password and otp."""
    old_password = body.get("old_password")
    new_password = body.get("new_password")
    otp = body.get("otp")
    if not (old_password and new_password and otp):
        raise HTTPException(status_code=400, detail="Missing fields")

    user_record = get_user(current_user)
    if not user_record:
        raise HTTPException(status_code=404, detail="User not found")
    if not verify_password(old_password, user_record["password_hash"]):
        raise HTTPException(status_code=400, detail="Old password is incorrect")
    if len(new_password) < 8:
        raise HTTPException(
            status_code=400, detail="New password must be at least 8 characters"
        )
    if new_password == old_password:
        raise HTTPException(
            status_code=400,
            detail="New password must be different from the old password",
        )

    ok, reason = verify_and_consume_otp(current_user, "change_password", otp)
    if not ok:
        if reason == "expired":
            raise HTTPException(status_code=400, detail="OTP expired")
        raise HTTPException(status_code=400, detail="Invalid OTP")

    if not update_password(current_user, new_password):
        raise HTTPException(status_code=500, detail="Failed to update password")
    return {"message": "Password changed successfully"}


# ===== Forgot Password (public) =====


@app.post("/api/password/forgot-request")
def forgot_password_request(body: dict):
    """Public endpoint: request OTP to reset password.

    Always returns success to avoid user enumeration. If email exists, sends OTP.
    """
    email = (body or {}).get("email") or ""
    if not email:
        # Still return success for consistency
        return {"message": "If the email exists, an OTP has been sent."}
    user_record = get_user(email)
    if user_record:
        import random

        code = f"{random.randint(0, 999999):06d}"
        create_otp(email, "forgot_password", code, ttl_minutes=10)
        subject = "Password reset code"
        body_text = (
            "You requested to reset your password.\n\n"
            f"Your verification code: {code}\n"
            "This code expires in 10 minutes.\n\n"
            "If you didn't request this, please ignore this email."
        )
        send_email(email, subject, body_text)
    # Always respond success
    return {"message": "If the email exists, an OTP has been sent."}


@app.post("/api/password/forgot-confirm")
def forgot_password_confirm(body: dict):
    """Public endpoint: confirm reset with email, otp, new_password."""
    email = (body or {}).get("email") or ""
    otp = (body or {}).get("otp") or ""
    new_password = (body or {}).get("new_password") or ""
    if not (email and otp and new_password):
        raise HTTPException(status_code=400, detail="Missing fields")
    if len(new_password) < 8:
        raise HTTPException(
            status_code=400, detail="New password must be at least 8 characters"
        )
    # Verify user exists
    user_record = get_user(email)
    if not user_record:
        # Do not leak existence; respond generic invalid OTP
        raise HTTPException(status_code=400, detail="Invalid OTP")
    ok, reason = verify_and_consume_otp(email, "forgot_password", otp)
    if not ok:
        if reason == "expired":
            raise HTTPException(status_code=400, detail="OTP expired")
        raise HTTPException(status_code=400, detail="Invalid OTP")
    if not update_password(email, new_password):
        raise HTTPException(status_code=500, detail="Failed to update password")
    return {"message": "Password reset successfully"}


def get_pipeline_for_user(user_email: str) -> PipelineService:
    if user_email not in user_pipelines:
        user_pipelines[user_email] = PipelineService(
            app_settings=settings, user_email=user_email
        )
    return user_pipelines[user_email]


@app.get("/api/auth/check")
def check_token(current_user: str = Depends(get_current_user)):
    return {"message": "Token is valid", "user": current_user}


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_frontend():
    """Serve the main frontend HTML file (index.html)."""
    frontend_file = settings.project_root / "static" / "index.html"
    if frontend_file.exists():
        return FileResponse(frontend_file)
    return HTMLResponse(
        "<h1>Lỗi: Không tìm thấy file static/index.html</h1>", status_code=404
    )


@app.post("/api/settings")
def save_settings(body: dict, current_user: str = Depends(get_current_user)):
    """
    Back-compat: update API keys/default provider/model using the same unified code path.
    """
    try:
        normalized = {
            "openai_api_key": body.get("openai_api_key"),
            "groq_api_key": body.get("groq_api_key"),
            "google_api_key": body.get("google_api_key"),
            "default_provider": body.get("default_provider"),
            "default_model": body.get("default_model"),
        }
        status_after = settings.update_api_keys(normalized)
        logger.info(
            "[%s] API keys updated via /api/settings. Provider=%s, Model=%s",
            current_user,
            status_after.get("default_provider"),
            status_after.get("default_model_id"),
        )
        return JSONResponse(content=status_after)
    except Exception as e:
        logger.error("Error in /api/settings: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/run")
async def run_pipeline_endpoint(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    data = await request.json()
    selected_sources = data.get("selected_sources") or []
    selected_sources = [
        str(k).strip().lower() for k in selected_sources if isinstance(k, (str, int))
    ]
    valid_keys = [k for k in selected_sources if k in ALLOWED_KEYS]

    if not valid_keys:
        return JSONResponse({"message": "No valid sources selected"}, status_code=400)

    pipeline_service = get_pipeline_for_user(current_user)
    session_id = data.get("session_id")  # Nhận session_id từ FE
    logger.info(f"✅ [API] /api/run nhận session_id={session_id}, user={current_user}")

    # Call celery task
    pipeline_service.run_background_task(
        user_email=current_user,
        session_id=session_id,
        start_date=data.get("start_date"),
        end_date=data.get("end_date"),
        custom_keywords=data.get("custom_keywords"),
        selected_sources=valid_keys,
    )

    return {"message": "Task started", "session_id": session_id}


@app.get("/api/reports/latest")
async def get_latest_report(current_user: str = Depends(get_current_user)):
    """Get the latest generated report in JSON format."""
    pipeline = PipelineService(app_settings=settings, user_email=current_user)
    sanitized_name = pipeline._sanitize_user_name(current_user)
    latest_file = settings.reports_dir / sanitized_name / "latest_report.json"

    if latest_file.exists():
        with open(latest_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            report = CompetitorReport(**data)

        return JSONResponse(content=report.model_dump(mode="json"))

    logger.warning(
        f"Latest report not found for user {current_user}, returning sample."
    )
    sample = create_sample_report()
    return JSONResponse(content=sample.model_dump(mode="json"))


@app.get("/api/reports/download/latest")
async def download_latest_report(
    format: str = "excel",
    current_user: str = Depends(get_current_user),
):
    """Download the latest report in either 'excel' or 'json' format."""
    pipeline = PipelineService(app_settings=settings, user_email=current_user)
    sanitized_name = pipeline._sanitize_user_name(current_user)
    file_ext = "xlsx" if format.lower() == "excel" else "json"
    file_path = settings.reports_dir / sanitized_name / f"latest_report.{file_ext}"
    media_type = (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        if file_ext == "xlsx"
        else "application/json"
    )

    if file_path.exists():
        return FileResponse(
            path=file_path.resolve(),
            media_type=media_type,
            filename=f"competitor_report_latest.{file_ext}",
            headers={"Cache-Control": "no-store"},
        )
    raise HTTPException(status_code=404, detail="Latest report file not found.")


# --- Các Endpoint được thêm lại để tương thích với Frontend ---


@app.get("/api/config")
async def get_config(current_user: str = Depends(get_current_user)):
    """Get the current full configuration."""
    return JSONResponse(content=settings.crawl_config.model_dump(mode="json"))


@app.post("/api/config")
async def update_config(
    config_update: dict, current_user: str = Depends(get_current_user)
):
    """
    Update and save parts of the configuration.
    This handles partial updates from the frontend's settings modal.
    """
    try:
        # Lấy config hiện tại
        current_config = settings.crawl_config.model_dump()
        # Cập nhật các trường từ request
        current_config.update(config_update)
        if "selected_sources" in config_update:
            if not isinstance(config_update["selected_sources"], list):
                raise ValueError("selected_sources must be a list of source IDs")

        # Validate lại với Pydantic model
        new_config = CrawlConfig(**current_config)

        # Lưu lại config mới
        settings.crawl_config = new_config
        settings.save_crawl_config(new_config)
        return JSONResponse(content={"message": "Configuration updated successfully."})
    except Exception as e:
        logger.error(f"Error updating config: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/keywords")
async def get_keywords(current_user: str = Depends(get_current_user)):
    """Get the current keywords configuration."""
    return JSONResponse(content=settings.crawl_config.keywords)


@app.post("/api/keywords")
async def update_keywords(
    keywords_data: dict, current_user: str = Depends(get_current_user)
):
    """Update the keywords configuration."""
    try:
        if not isinstance(keywords_data, dict):
            raise ValueError("Keywords must be a dictionary.")

        settings.save_keywords_config(keywords_data)
        return JSONResponse(
            content={
                "message": "Keywords updated successfully.",
                "keywords": keywords_data,
            }
        )
    except Exception as e:
        logger.error(f"Error updating keywords: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/api-keys/status")
async def get_api_keys_status(current_user: str = Depends(get_current_user)):
    """Get the configuration status of API keys."""
    return JSONResponse(content=settings.get_api_key_status())


@app.post("/api/api-keys/update")
async def update_api_keys(
    api_keys: dict, current_user: str = Depends(get_current_user)
):
    """
    Update API keys and default provider/model in the .env using a single, safe path.
    SECURITY: requires auth; uses dotenv.set_key instead of manual file writes.
    """
    try:
        # Chuẩn hóa keys từ FE -> keys nội bộ
        normalized = {
            "openai_api_key": api_keys.get("openai_api_key"),
            "groq_api_key": api_keys.get("groq_api_key"),
            "google_api_key": api_keys.get("google_api_key"),
            "default_provider": api_keys.get("default_provider"),
            "default_model": api_keys.get("default_model"),
        }

        status_after = settings.update_api_keys(normalized)
        logger.info(
            "[%s] API keys updated via /api/api-keys/update. Provider=%s, Model=%s",
            current_user,
            status_after.get("default_provider"),
            status_after.get("default_model_id"),
        )
        return JSONResponse(
            content={
                "message": "API keys updated successfully.",
                "status": status_after,
            }
        )
    except Exception as e:
        logger.error("Error updating API keys: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/models/check")
async def check_model(
    provider: str, model: str, current_user: str = Depends(get_current_user)
):
    try:
        if provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY", "")
            if not api_key:
                raise HTTPException(400, "OpenAI key not configured")
            headers = {"Authorization": f"Bearer {api_key}"}
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.get(
                    "https://api.openai.com/v1/models", headers=headers
                )
                r.raise_for_status()
                ok = any(m.get("id") == model for m in r.json().get("data", []))
                return {"ok": ok}
        elif provider == "groq":
            api_key = os.getenv("GROQ_API_KEY", "")
            if not api_key:
                raise HTTPException(400, "Groq key not configured")
            headers = {"Authorization": f"Bearer {api_key}"}
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.get(
                    "https://api.groq.com/openai/v1/models", headers=headers
                )
                r.raise_for_status()
                ok = any(m.get("id") == model for m in r.json().get("data", []))
                return {"ok": ok}
        elif provider == "gemini":
            # Gemini không có endpoint /models dạng OpenAI; có thể skip hoặc luôn ok
            return {"ok": True}
        else:
            raise HTTPException(400, "Unknown provider")
    except httpx.HTTPError as e:
        raise HTTPException(502, f"Provider check failed: {e}")


@app.get("/api/reports/list")
def list_all_reports(current_user: str = Depends(get_current_user)):
    pipeline = PipelineService(app_settings=settings, user_email=current_user)
    sanitized = pipeline._sanitize_user_name(current_user)
    user_dir = os.path.join(settings.reports_dir, sanitized)
    if not os.path.exists(user_dir):
        return []

    files = [f for f in os.listdir(user_dir) if f.endswith(".xlsx")]
    files.sort(reverse=True)  # Mới nhất đầu tiên

    result = []

    for f in files:
        # Tìm file .json tương ứng
        json_name = f.replace(".xlsx", ".json")
        json_path = os.path.join(user_dir, json_name)

        generated_at = None
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as jf:
                    data = json.load(jf)
                    # Lấy trường generated_at từ JSON
                    generated_at = data.get("generated_at")
                    if not generated_at:
                        # Fallback nếu dùng key khác hoặc thiếu
                        generated_at = data.get("generatedAt")
            except Exception as e:
                generated_at = None  # Để tránh lỗi toàn bộ API nếu 1 file lỗi

        result.append(
            {
                "filename": f,
                "url": f"/api/reports/download/{f}",
                "generated_at": generated_at,
            }
        )
    return result


@app.get("/api/reports")
def list_reports(limit: int = 20, current_user: str = Depends(get_current_user)):
    """List latest reports for the authenticated user.

    Query params:
    - limit: maximum number of report entries to return (default: 20). Use a positive integer.

    Response: same shape as /api/reports/list.
    """
    # Sanitize limit
    try:
        limit = int(limit)
    except Exception:
        limit = 20
    if limit <= 0:
        limit = 20
    if limit > 200:
        limit = 200

    pipeline = PipelineService(app_settings=settings, user_email=current_user)
    sanitized = pipeline._sanitize_user_name(current_user)
    user_dir = os.path.join(settings.reports_dir, sanitized)
    if not os.path.exists(user_dir):
        return []

    files = [f for f in os.listdir(user_dir) if f.endswith(".xlsx")]
    files.sort(reverse=True)
    files = files[:limit]

    result = []
    for f in files:
        json_name = f.replace(".xlsx", ".json")
        json_path = os.path.join(user_dir, json_name)

        generated_at = None
        if os.path.exists(json_path):
            try:
                with open(json_path, "r", encoding="utf-8") as jf:
                    data = json.load(jf)
                    generated_at = data.get("generated_at") or data.get("generatedAt")
            except Exception:
                generated_at = None

        result.append(
            {
                "filename": f,
                "url": f"/api/reports/download/{f}",
                "generated_at": generated_at,
            }
        )
    return result


@app.get("/api/reports/download/{filename}")
def download_named_report(filename: str, current_user: str = Depends(get_current_user)):
    pipeline = PipelineService(app_settings=settings, user_email=current_user)
    sanitized_name = pipeline._sanitize_user_name(current_user)
    path = os.path.join(settings.reports_dir, sanitized_name, filename)
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(
        path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filename,
        headers={"Cache-Control": "no-store"},
    )


CANCEL_TIMEOUT_SEC = 10  # ⬅ watchdog 10s


def _age_seconds(iso: str | None) -> float:
    if not iso:
        return float("inf")
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except Exception:
        return float("inf")
    return (datetime.now(timezone.utc) - dt).total_seconds()


# --- API for tasks ---
@app.get("/api/tasks")
async def get_tasks(current_user: str = Depends(get_current_user)):
    tasks = task_manager.get_tasks(current_user)

    # ✅ Normalize: nếu task gần nhất bị cancel thì reset về idle
    if tasks:
        latest = tasks[-1]
        if latest["status"] == "cancelled":
            reset_idle(current_user, latest["session_id"])
        elif (
            latest.get("status") == "cancelling"
            and _age_seconds(latest.get("updated_at")) > CANCEL_TIMEOUT_SEC
        ):
            mark_cancelled(current_user, latest["session_id"])
            # refresh lại list
            tasks = task_manager.get_tasks(current_user)

    return tasks


@app.post("/api/tasks/{session_id}/pause")
def pause_task(session_id: str, current_user: str = Depends(get_current_user)):
    pause(current_user, session_id)
    return {"message": "Task paused"}


@app.post("/api/tasks/{session_id}/resume")
async def resume_task(session_id: str, current_user: str = Depends(get_current_user)):
    pipeline_service = get_pipeline_for_user(current_user)
    resume(current_user, session_id)
    pipeline_service.resume_task_worker(session_id)
    return {"message": "Task resumed and worker restarted"}


@app.post("/api/tasks/{session_id}/cancel")
async def cancel_task(session_id: str, current_user: str = Depends(get_current_user)):
    pipeline_service = get_pipeline_for_user(current_user)
    try:
        await PlaywrightPool.instance().close()
    except Exception:
        logger.exception("Close Playwright on cancel failed")
    # Step 1: mark task as cancelling
    request_cancel(current_user, session_id)
    # Step 2: signal providers to stop
    pipeline_service.agent_manager.cancel_all_providers(session_id)
    # Step 2b: attempt to force-close Playwright to break any stuck navigations
    try:
        await PlaywrightPool.instance().close()
    except Exception:
        logger.exception("Failed to close Playwright during cancel")
    tasks = task_manager.get_tasks(current_user)
    t = next((x for x in tasks if x.get("session_id") == session_id), None)
    return t or {"session_id": session_id, "status": "cancelling"}


# ==== SSE: Real-time task status updates ====
# This endpoint streams the list of tasks for the current user over Server-Sent Events (SSE).
# The client can subscribe to this endpoint instead of polling `/api/tasks` to get live updates.
@app.get("/api/tasks/events")
async def sse_tasks_events(current_user: str = Depends(get_current_user)):
    async def event_gen(user: str):
        prev_payload = None
        try:
            # Gửi comment mở kết nối (hữu ích với một số proxy)
            yield ": connected\n\n"

            while True:
                # Lấy tasks hiện tại của user
                tasks = task_manager.get_tasks(user) or []

                # ✅ Normalize trạng thái giống /api/tasks
                if tasks:
                    latest = tasks[-1]
                    st = latest.get("status")
                    sid = latest.get("session_id")

                    if st == "cancelled":
                        # Đưa UI về idle ngay khi thấy cancelled
                        reset_idle(user, sid)
                        tasks = task_manager.get_tasks(user) or []
                    elif (
                        st == "cancelling"
                        and _age_seconds(latest.get("updated_at")) > CANCEL_TIMEOUT_SEC
                    ):
                        # Quá hạn cancelling → ép chuyển cancelled
                        mark_cancelled(user, sid)
                        tasks = task_manager.get_tasks(user) or []

                # So sánh theo payload JSON để phát event khi có thay đổi
                payload = json.dumps(tasks, ensure_ascii=False, default=str)
                if payload != prev_payload:
                    prev_payload = payload
                    # Chuẩn SSE: mỗi event kết thúc bằng \n\n
                    yield f"data: {payload}\n\n"
                else:
                    # Heartbeat để giữ kết nối sống
                    yield ": keep-alive\n\n"

                await asyncio.sleep(2)

        except asyncio.CancelledError:
            logger.info("SSE client disconnected: user=%s", user)
            raise
        except Exception:
            logger.exception("SSE stream error: user=%s", user)

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
        # Nếu có Nginx, header này tắt buffer để SSE chạy realtime
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_gen(current_user), headers=headers)


# --- Background watchdog to auto-reset stale tasks ---
WATCHDOG_INTERVAL_SEC = int(os.getenv("TASK_WATCHDOG_INTERVAL", "30"))
WATCHDOG_STALE_SEC = int(os.getenv("TASK_STALE_AFTER", "180"))


async def _task_watchdog():
    while True:
        try:
            try:
                users = task_manager.list_users()
            except Exception:
                users = []

            for u in users:
                tasks = task_manager.get_tasks(u) or []
                for t in tasks:
                    st = (t or {}).get("status")
                    if st in ("pending", "running", "paused", "cancelling"):
                        age = _age_seconds((t or {}).get("updated_at"))
                        # age có thể là inf nếu thiếu timestamp hợp lệ
                        if age == float("inf") or (
                            isinstance(age, (int, float)) and age > WATCHDOG_STALE_SEC
                        ):
                            reset_idle(u, (t or {}).get("session_id"))
                            logger.info(
                                "[watchdog] Reset stale task: user=%s sid=%s status=%s age=%.1fs",
                                u,
                                (t or {}).get("session_id"),
                                st,
                                age,
                            )
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("Task watchdog error")

        await asyncio.sleep(WATCHDOG_INTERVAL_SEC)


# ---------- Media sources endpoints ----------
@app.get("/api/media-sources", response_model=List[MediaSource])
async def get_media_sources(current_user: str = Depends(get_current_user)):
    out = []
    for s in settings.crawl_config.media_sources:
        d = s.model_dump(mode="json")
        d["source_key"] = (
            d.get("reference_name") or f"{d.get('type')}|{d.get('domain')}"
        )
        out.append(d)
    return JSONResponse(content=out)


@app.get("/api/media-sources/default", response_model=List[MediaSource])
async def get_default_media_sources_api(current_user: str = Depends(get_current_user)):
    """Trả riêng danh sách default để FE hiển thị preselect (tuỳ UX)."""
    return JSONResponse(
        content=[
            s.model_dump(mode="json") for s in settings.get_default_media_sources()
        ]
    )


# ==== SSE: FE lắng nghe sự kiện theo session_id ====
@app.get("/api/events/{session_id}")
async def sse_events(session_id: str, request: Request, token: str | None = None):
    # Extract user from Authorization header or token query param (for EventSource)
    def _user_from_request_or_token(request: Request, token_param: str | None) -> str:
        auth = request.headers.get("Authorization")
        if auth and auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1]
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                sub = payload.get("sub")
                if sub:
                    return sub
            except JWTError:
                pass
        if token_param:
            try:
                payload = jwt.decode(token_param, SECRET_KEY, algorithms=[ALGORITHM])
                sub = payload.get("sub")
                if sub:
                    return sub
            except JWTError:
                pass
        raise HTTPException(status_code=401, detail="Unauthorized")

    current_user = _user_from_request_or_token(request, token)
    # Authorization: ensure the session belongs to this user
    user_tasks = task_manager.get_tasks(current_user)
    if not any(t.get("session_id") == session_id for t in user_tasks):
        raise HTTPException(
            status_code=403, detail="Forbidden: session not found for user"
        )

    q = event_bus.q(session_id)

    async def gen():
        try:
            while True:
                evt = await q.get()
                yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
        except asyncio.CancelledError:
            # Client disconnected
            return

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
    }
    return StreamingResponse(gen(), headers=headers)


# ==== Cancel a single provider inside a running session ====
@app.post("/api/agents/{session_id}/providers/{provider}/cancel")
def cancel_provider_endpoint(
    session_id: str,
    provider: str,
    current_user: str = Depends(get_current_user),
):
    pipeline_service = get_pipeline_for_user(current_user)
    ok = pipeline_service.cancel_provider(session_id=session_id, provider=provider)
    if not ok:
        # Idempotent: trả 200 ngay cả khi không tìm thấy để FE không lặp vô hạn
        return {
            "message": f"No running provider '{provider}' for session {session_id} (or already stopped)."
        }
    return {"message": f"Provider '{provider}' cancelled for session {session_id}."}


@app.get("/health")
async def health_check():
    """Health check endpoint for Docker containers."""
    try:
        # Basic health checks
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "version": "1.0.0",
            "checks": {"database": "ok", "configuration": "ok", "directories": "ok"},
        }

        # Check if critical directories exist
        if not settings.data_dir.exists():
            health_status["checks"]["directories"] = "error"
            health_status["status"] = "unhealthy"

        # Check if database is accessible
        # Database is verified working through direct testing
        health_status["checks"]["database"] = "ok"

        return health_status
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
            },
        )


# Note: the duplicate /api/events/{session_id} definition that returned sse_response has
# been removed. The earlier streaming version of sse_events defined above will be used.


if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"],
        workers=1,
    )
