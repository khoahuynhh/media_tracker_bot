import json
import threading
import time
from datetime import datetime
from pathlib import Path


class TaskManager:
    def __init__(self, storage_path="data/task_queue.json"):
        self.storage_path = Path(storage_path)
        self.tasks = {}  # user_email: [tasks]
        self.load_tasks()

    def add_task(self, user_email, task_data):
        self.tasks.setdefault(user_email, []).append(task_data)
        self.save_tasks()

    def get_tasks(self, user_email):
        return self.tasks.get(user_email, [])

    def update_task(self, user_email, session_id, updates):
        for task in self.tasks.get(user_email, []):
            if task["session_id"] == session_id:
                task.update(updates)
        self.save_tasks()

    def load_tasks(self):
        if self.storage_path.exists():
            with open(self.storage_path, "r", encoding="utf-8") as f:
                self.tasks = json.load(f)

    def save_tasks(self):
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(self.tasks, f, ensure_ascii=False, indent=2)
