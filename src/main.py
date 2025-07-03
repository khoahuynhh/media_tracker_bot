# src/main.py
"""
Main entry point for the Media Tracker Bot.
Phiên bản này đã được điều chỉnh để tương thích ngược với file index.html gốc,
bằng cách thêm lại các API endpoint đã được refactor trước đó.
"""

import logging
import os
import uuid
from pathlib import Path
import json
import sys
import asyncio # Thêm import asyncio

# SỬA LỖI: Thêm đoạn code này để giải quyết lỗi "NotImplementedError" của asyncio trên Windows
# Lỗi này xảy ra khi Playwright cố gắng tạo một tiến trình con.
if sys.platform == "win32" and sys.version_info >= (3, 8):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import uvicorn
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import subprocess

subprocess.run(["playwright", "install"], check=False)
# Setup logging before other imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/media_tracker.log", encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
)
logger = logging.getLogger(__name__)

# Import refactored components
from .models import CrawlConfig, create_sample_report
from .configs import settings
from .services import pipeline_service

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

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_frontend():
    """Serve index.html page"""
    try:
        frontend_file = settings.project_root / "static" / "index.html"
        if frontend_file.exists():
            return HTMLResponse(content=frontend_file.read_text(encoding="utf-8"))
        else:
            return HTMLResponse(
                content="""
            <html>
                <head><title>Media Tracker Bot</title></head>
                <body>
                    <h1>Media Tracker Bot</h1>
                    <p>Demo page not found. Please create demo.html file.</p>
                    <p><a href="/api/docs">API Documentation</a></p>
                </body>
            </html>
            """
            )
    except Exception as e:
        logger.error(f"Error serving demo page: {str(e)}")
        return HTMLResponse(
            content=f"<html><body><h1>Error: {str(e)}</h1></body></html>"
        )

@app.get("/api/status")
async def get_status():
    """Get the current status of the bot and any running pipeline."""
    status = pipeline_service.get_status()
    return JSONResponse(content=status)

@app.post("/api/run")
async def run_pipeline_endpoint(background_tasks: BackgroundTasks, request: Request):
    """Start a new media tracking pipeline run in the background."""
    if pipeline_service.is_running:
        raise HTTPException(
            status_code=409,
            detail=f"A pipeline is already running with session ID: {pipeline_service.current_session_id}"
        )
    
    try:
        data = await request.json()
    except json.JSONDecodeError:
        data = {}

    session_id = data.get("session_id", str(uuid.uuid4()))
    
    background_tasks.add_task(
        pipeline_service.run_pipeline,
        session_id=session_id,
        start_date=data.get("start_date"),
        end_date=data.get("end_date"),
        custom_keywords=data.get("custom_keywords"),
    )
    return JSONResponse(
        content={"message": "Pipeline started in background.", "session_id": session_id},
        status_code=202
    )

@app.post("/api/stop")
async def stop_pipeline_endpoint(request: Request):
    """Request to stop the currently running pipeline."""
    if not pipeline_service.is_running:
        raise HTTPException(status_code=400, detail="No pipeline is currently running.")
    
    try:
        data = await request.json()
        session_id = data.get("session_id")
        if not session_id or session_id != pipeline_service.current_session_id:
            raise HTTPException(status_code=400, detail="Invalid or missing session_id for active pipeline.")
    except (json.JSONDecodeError, KeyError):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object with a 'session_id' key.")
        
    success = pipeline_service.stop_pipeline(session_id)
    if success:
        return JSONResponse(content={"message": "Pipeline stop requested."})
    else:
        raise HTTPException(status_code=500, detail="Failed to request pipeline stop.")

@app.get("/api/reports/latest")
async def get_latest_report():
    """Get the latest generated report in JSON format."""
    latest_file = settings.reports_dir / "latest_report.json"
    if latest_file.is_symlink() and os.path.exists(latest_file):
        return FileResponse(latest_file)
    
    logger.warning("Latest report not found, returning a sample report.")
    sample = create_sample_report()
    return JSONResponse(content=sample.model_dump(mode='json'))

@app.get("/api/reports/download/latest")
async def download_latest_report(format: str = "excel"):
    """Download the latest report in either 'excel' or 'json' format."""
    file_ext = "xlsx" if format.lower() == "excel" else "json"
    file_path = settings.reports_dir / f"latest_report.{file_ext}"
    media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if file_ext == "xlsx" else "application/json"
    
    if file_path.is_symlink() and os.path.exists(file_path):
        return FileResponse(path=file_path, media_type=media_type, filename=f"competitor_report_latest.{file_ext}")
    raise HTTPException(status_code=404, detail="Latest report file not found.")

# --- Các Endpoint được thêm lại để tương thích với Frontend ---

@app.get("/api/config")
async def get_config():
    """Get the current full configuration."""
    return JSONResponse(content=settings.crawl_config.model_dump(mode='json'))

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
        return JSONResponse(content={"message": "Keywords updated successfully.", "keywords": keywords_data})
    except Exception as e:
        logger.error(f"Error updating keywords: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/api-keys/status")
async def get_api_keys_status():
    """Get the configuration status of API keys."""
    return JSONResponse(content=settings.get_api_key_status())

@app.post("/api/api-keys/update")
async def update_api_keys(api_keys: dict):
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

        return JSONResponse(content={
            "message": "API keys updated successfully. The application might need a restart to use new keys.",
            "status": settings.get_api_key_status()
        })
    except Exception as e:
        logger.error(f"Error updating API keys: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"]
    )
