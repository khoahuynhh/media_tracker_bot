"""
Celery worker configuration and tasks for Media Tracker Bot.

This module defines a Celery application bound to a message broker (e.g., Redis)
and exposes a task to run the pipeline asynchronously. When enabled via
environment variables and installed dependencies, tasks will be executed by
dedicated worker processes instead of blocking the main FastAPI server.

To activate Celery, ensure that:
  - The `celery` and `redis` packages are installed (see requirements.txt).
  - `CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND` are configured in your
    environment (.env file). Defaults point to a local Redis instance.
  - The environment variable `CELERY_ENABLED` is set to "1".
  - A Celery worker process is running, e.g. via `celery -A main.celery_app worker --loglevel=info`.

Note: This task function wraps the asynchronous pipeline logic using
`asyncio.run()` to execute it within the synchronous Celery context. If your
pipeline becomes CPU bound, consider offloading heavy computation to a
separate service or scaling worker concurrency accordingly.
"""

from __future__ import annotations

import os
import asyncio
from celery import Celery

# Local imports must be relative for Celery autodiscovery
from .services import PipelineService
from .configs import settings
from .task_state import task_manager


# Configure Celery application. The broker and result backend default to
# Redis running on localhost if not specified via environment variables.
BROKER_URL = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

celery_app = Celery(
    "media_tracker",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
)


@celery_app.task(name="run_pipeline_task")
def run_pipeline_task(
    session_id: str,
    user_email: str,
    start_date: str | None = None,
    end_date: str | None = None,
    custom_keywords: dict | None = None,
    selected_sources: list | None = None,
) -> bool:
    """Celery task wrapper around the pipeline service.

    This task updates the task status to "running" and then executes the
    asynchronous pipeline logic. Upon completion or failure, it updates the
    task status accordingly. The return value indicates success.
    """
    # Update the task status to running at the start of execution
    task_manager.update_task(user_email, session_id, {"status": "running"})
    pipeline_service = PipelineService(app_settings=settings, user_email=user_email)
    try:
        # Execute the asynchronous pipeline; wrap in asyncio.run for Celery
        asyncio.run(
            pipeline_service.run_pipeline_logic(
                session_id=session_id,
                user_email=user_email,
                start_date=start_date,
                end_date=end_date,
                custom_keywords=custom_keywords,
                selected_sources=selected_sources,
            )
        )
        # Mark task as completed
        task_manager.update_task(user_email, session_id, {"status": "completed"})
        return True
    except Exception as e:  # broad catch to ensure state is updated on failure
        # Record failure and error message
        task_manager.update_task(
            user_email,
            session_id,
            {"status": "failed", "error": str(e)},
        )
        return False


__all__ = ["celery_app", "run_pipeline_task"]
