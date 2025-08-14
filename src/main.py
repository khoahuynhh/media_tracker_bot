# src/main.py
"""
Main entry point for the Media Tracker Bot.
"""

import logging
import os
import json
import sys
import asyncio
import uvicorn
import httpx


from fastapi import FastAPI, Request, HTTPException, Depends, status
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
if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except Exception:
        pass

# Setup logging before other imports
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/media_tracker.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# Import modules
from .task_state import task_manager
from .event import event_bus, decision_bus, RunDecision
from .models import (
    CrawlConfig,
    CompetitorReport,
    create_sample_report,
    UserLogin,
    USER_DB,
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
from .agents import AgentManager

# Save pipeline according to email user
user_pipelines: Dict[str, PipelineService] = {}
logger = logging.getLogger(__name__)

# Map tra cứu nguồn theo key chuẩn
SOURCE_BY_KEY: Dict[str, dict] = {
    settings.normalize_source_key(s.model_dump() if hasattr(s, "model_dump") else s): (
        s.model_dump() if hasattr(s, "model_dump") else s
    )
    for s in settings.crawl_config.media_sources
}
ALLOWED_KEYS = set(SOURCE_BY_KEY.keys())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info("--- Media Tracker Bot Server is starting up ---")
    yield
    logger.info("--- Media Tracker Bot Server is shutting down ---")


async def lifespan(app: FastAPI):
    # ===== STARTUP =====
    try:
        task_manager.load_tasks()  # an toàn: đã có fallback JSON corrupt
        logger.info("Task manager loaded.")
        # TODO: mở DB connection / warmup model nếu cần
    except Exception:
        logger.exception("Startup failed (continuing with degraded features).")
    yield
    # ===== SHUTDOWN =====
    try:
        # TODO: đóng DB connection / flush queue nếu cần
        logger.info("Shutdown cleanup done.")
    except Exception:
        logger.exception("Shutdown cleanup failed.")


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

# Mount thư mục static để phục vụ frontend
static_dir = settings.project_root / "static"
if static_dir.exists() and static_dir.is_dir():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


# --- API Endpoints ---
@app.post("/api/login")
def login(user: UserLogin):
    user_record = USER_DB.get(user.email)

    if not user_record or user_record["password"] != user.password:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token_data = {"sub": user.email, "role": user_record["role"]}

    token = create_access_token(token_data)
    print("Generated token:", token)
    return {"access_token": token, "token_type": "bearer", "role": user_record["role"]}


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
async def get_config():
    """Get the current full configuration."""
    return JSONResponse(content=settings.crawl_config.model_dump(mode="json"))


@app.post("/api/config")
async def update_config(config_update: dict):
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
async def get_keywords():
    """Get the current keywords configuration."""
    return JSONResponse(content=settings.crawl_config.keywords)


@app.post("/api/keywords")
async def update_keywords(keywords_data: dict):
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


# --- API for tasks ---
@app.get("/api/tasks")
def get_user_tasks(current_user: str = Depends(get_current_user)):
    return task_manager.get_tasks(current_user)


@app.post("/api/tasks/{session_id}/pause")
def pause_task(session_id: str, current_user: str = Depends(get_current_user)):
    task_manager.update_task(current_user, session_id, {"status": "paused"})
    return {"message": "Task paused"}


@app.post("/api/tasks/{session_id}/resume")
async def resume_task(session_id: str, current_user: str = Depends(get_current_user)):
    pipeline_service = get_pipeline_for_user(current_user)
    task_manager.update_task(current_user, session_id, {"status": "running"})
    pipeline_service.resume_task_worker(session_id)
    return {"message": "Task resumed and worker restarted"}


@app.post("/api/tasks/{session_id}/cancel")
def cancel_task(session_id: str, current_user: str = Depends(get_current_user)):
    task_manager.update_task(current_user, session_id, {"status": "cancelled"})
    return {"message": "Task cancelled"}


# ==== SSE: Real-time task status updates ====
# This endpoint streams the list of tasks for the current user over Server-Sent Events (SSE).
# The client can subscribe to this endpoint instead of polling `/api/tasks` to get live updates.
@app.get("/api/tasks/events")
async def sse_tasks_events(current_user: str = Depends(get_current_user)):
    async def event_gen():
        prev_tasks = None
        while True:
            # Fetch current tasks for the authenticated user
            tasks = task_manager.get_tasks(current_user)
            # Only emit when there is a change to avoid flooding the client
            if tasks != prev_tasks:
                prev_tasks = tasks
                # Serialize tasks as JSON; ensure_ascii=False to retain UTF-8
                yield f"data: {json.dumps(tasks, ensure_ascii=False)}\n\n"
            # Sleep briefly to throttle updates; adjust interval as needed
            await asyncio.sleep(2)

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
    }
    return StreamingResponse(event_gen(), headers=headers)


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
async def sse_events(session_id: str):
    q = event_bus.q(session_id)

    async def gen():
        while True:
            evt = await q.get()
            yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
    }
    return StreamingResponse(gen(), headers=headers)


@app.post("/api/run/decision")
async def post_run_decision(body: RunDecision):
    await decision_bus.publish(body.session_id, body.model_dump())
    return {"ok": True}


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


# Note: the duplicate /api/events/{session_id} definition that returned sse_response has
# been removed. The earlier streaming version of sse_events defined above will be used.


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"],
        workers=1,
    )
