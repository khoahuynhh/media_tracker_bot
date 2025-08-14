"""
Centralized task manager instance.

This module exposes a singleton `task_manager` that is used throughout the
application. Importing this module ensures that the underlying database
connection is established exactly once.
"""

from .background_tasks import TaskManager

# Instantiate a global task manager for the application. Using SQLite ensures
# that tasks are persisted across server restarts and accessible from multiple
# threads (within a single process). If deploying across multiple processes or
# machines, consider using a distributed queue like Celery with Redis or
# RabbitMQ instead.
task_manager = TaskManager()

__all__ = ["task_manager"]
