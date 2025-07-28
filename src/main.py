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

from fastapi import FastAPI, Request, HTTPException, Depends, status
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from typing import Dict
from jose import JWTError, jwt

if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Setup logging before other imports
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
from .models import (
    CrawlConfig,
    CompetitorReport,
    create_sample_report,
    UserLogin,
    USER_DB,
)
from .configs import settings
from .services import (
    PipelineService,
    create_access_token,
    get_current_user,
    SECRET_KEY,
    ALGORITHM,
)

# Save pipeline according to email user
user_pipelines: Dict[str, PipelineService] = {}

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info("--- Media Tracker Bot Server is starting up ---")
    yield
    logger.info("--- Media Tracker Bot Server is shutting down ---")


# Initialize FastAPI app
app = FastAPI(
    title="Media Tracker Bot API",
    description="API for Vietnamese Media Tracking and Competitor Analysis (Retro-compatible)",
    version="2.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
    return {"access_token": token, "token_type": "bearer", "role": user_record["role"]}


def get_pipeline_for_user(user_email: str) -> PipelineService:
    if user_email not in user_pipelines:
        user_pipelines[user_email] = PipelineService(
            app_settings=settings, user_email=user_email
        )
    return user_pipelines[user_email]


@app.get("/api/auth/check")
def check_token(request: Request):
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Token missing"
        )

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return {"message": "Token is valid", "user": payload.get("sub")}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token"
        )


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_frontend():
    """Serve the main frontend HTML file (index.html)."""
    frontend_file = settings.project_root / "static" / "index.html"
    if frontend_file.exists():
        return FileResponse(frontend_file)
    return HTMLResponse(
        "<h1>Lỗi: Không tìm thấy file static/index.html</h1>", status_code=404
    )


@app.post("/api/run")
async def run_pipeline_endpoint(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    data = await request.json()
    pipeline_service = get_pipeline_for_user(current_user)

    session_id = data.get("session_id")  # Nhận session_id từ FE
    logger.info(f"✅ [API] /api/run nhận session_id={session_id}, user={current_user}")

    # Call celery task
    pipeline_service.run_background_task(
        session_id=session_id,
        start_date=data.get("start_date"),
        end_date=data.get("end_date"),
        custom_keywords=data.get("custom_keywords"),
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
    Update API keys in the .env file.
    WARNING: This is a potential security risk in a production environment.
    It's included for compatibility with the existing frontend.
    """
    try:
        env_file = settings.project_root / ".env"

        # Đọc file .env hiện tại
        env_content = {}
        if env_file.exists():
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        key, value = line.split("=", 1)
                        env_content[key.strip()] = value.strip()

        # Cập nhật với các giá trị mới
        if "openai_api_key" in api_keys:
            env_content["OPENAI_API_KEY"] = api_keys["openai_api_key"]
        if "groq_api_key" in api_keys:
            env_content["GROQ_API_KEY"] = api_keys["groq_api_key"]
        if "default_provider" in api_keys:
            env_content["DEFAULT_MODEL_PROVIDER"] = api_keys["default_provider"]

        # Ghi lại vào file .env
        with open(env_file, "w", encoding="utf-8") as f:
            for key, value in env_content.items():
                f.write(f"{key}={value}\n")

        # Tải lại biến môi trường
        settings._setup_environment()

        return JSONResponse(
            content={
                "message": "API keys updated successfully. The application might need a restart to use new keys.",
                "status": settings.get_api_key_status(),
            }
        )
    except Exception as e:
        logger.error(f"Error updating API keys: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/list")
def list_all_reports(current_user: str = Depends(get_current_user)):
    user_dir = os.path.join(settings.reports_dir, current_user)
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


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True, reload_dirs=["src"])
