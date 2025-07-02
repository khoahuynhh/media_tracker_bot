# src/main.py
"""
Main entry point for the Media Tracker Bot.
This file sets up the FastAPI application and its endpoints.
It delegates all business logic to the PipelineService.
"""

import logging
import os
import uuid
from pathlib import Path
import json

import uvicorn
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# Setup logging before other imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/media_tracker.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# Import refactored components
from models import CrawlConfig, create_sample_report
from config import settings
from services import pipeline_service

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info("--- Media Tracker Bot Server is starting up ---")
    # The pipeline_service is already initialized, so we can use it directly.
    # Any async setup for the service could go here.
    yield
    logger.info("--- Media Tracker Bot Server is shutting down ---")

# Initialize FastAPI app
app = FastAPI(
    title="Media Tracker Bot API",
    description="API for Vietnamese Media Tracking and Competitor Analysis (Refactored)",
    version="2.0.0",
    lifespan=lifespan,
)

# Add CORS middleware to allow all origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Endpoints ---

@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def get_frontend():
    """Serve the main frontend HTML file."""
    frontend_file = settings.project_root / "static" / "index.html"
    if frontend_file.exists():
        return FileResponse(frontend_file)
    return HTMLResponse("<h1>Media Tracker Bot</h1><p>Frontend file not found.</p><a href='/docs'>API Docs</a>", status_code=404)

@app.get("/api/status")
async def get_status():
    """Get the current status of the bot and any running pipeline."""
    status = pipeline_service.get_status()
    return JSONResponse(content=status)

@app.post("/api/run")
async def run_pipeline(background_tasks: BackgroundTasks, request: Request):
    """Start a new media tracking pipeline run in the background."""
    if pipeline_service.is_running:
        raise HTTPException(
            status_code=409,  # Conflict
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
        status_code=202  # Accepted
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

@app.get("/api/config")
async def get_config():
    """Get the current full configuration."""
    return JSONResponse(content=settings.crawl_config.model_dump(mode='json'))

@app.post("/api/config")
async def update_config(new_config: CrawlConfig):
    """Update and save the entire configuration."""
    try:
        settings.crawl_config = new_config
        settings.save_crawl_config(new_config)
        return JSONResponse(content={"message": "Configuration updated successfully."})
    except Exception as e:
        logger.error(f"Error updating config: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))

if __name__ == "__main__":
    # This block allows running the server directly for development
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["src"] # Reload when any file in src changes
    )
