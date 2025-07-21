# src/tasks.py
from celery import Celery
from .services import PipelineService
from .configs import settings

celery_app = Celery(
    "media_tracker",
    broker="redis://localhost:6379/0",  # Redis broker
    backend="redis://localhost:6379/0",  # Optional: để lưu trạng thái task
)


@celery_app.task
def run_pipeline_task(
    user_email: str,
    session_id: str,
    start_date=None,
    end_date=None,
    custom_keywords=None,
):
    pipeline = PipelineService(app_settings=settings, user_email=user_email)
    pipeline.run_pipeline(session_id, user_email, start_date, end_date, custom_keywords)
