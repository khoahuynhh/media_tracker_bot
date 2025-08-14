import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Any, Optional


class TaskManager:
    """
    A production-ready task manager that persists tasks to a SQLite database.

    Tasks are keyed by `(user_email, session_id)` and stored as JSON blobs. A timestamp
    is recorded for ordering and for purging old tasks. All methods are thread-safe.
    """

    def __init__(self, db_path: str = "data/tasks.db"):
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._connect()
        self._init_db()

    def _connect(self) -> None:
        """Establish a connection to the SQLite database."""
        os.makedirs(self.db_path.parent, exist_ok=True)
        # Allow connections across threads
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def _init_db(self) -> None:
        """Create the tasks table if it doesn't exist."""
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    user_email TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    data TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    PRIMARY KEY (user_email, session_id)
                );
                """
            )

    def load_tasks(self) -> None:
        """Compatibility no-op: tasks are loaded from SQLite lazily."""
        return

    def save_tasks(self) -> None:
        """Compatibility no-op: commits are handled per-operation."""
        return

    def add_task(self, user_email: str, task_data: Dict[str, Any]) -> None:
        """
        Insert or replace a task for a user.

        Args:
            user_email: The email of the user owning this task.
            task_data: A dictionary representing the task; must include a 'session_id' key.
        """
        session_id = str(task_data.get("session_id"))
        if not session_id:
            raise ValueError("task_data must include a session_id")
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO tasks (user_email, session_id, data, ts) VALUES (?, ?, ?, strftime('%s','now'))",
                (user_email, session_id, json.dumps(task_data)),
            )

    def get_tasks(self, user_email: str) -> List[Dict[str, Any]]:
        """
        Retrieve all tasks for the given user.

        Returns a list of task dictionaries ordered by creation time (oldest first).
        """
        with self._lock:
            cur = self.conn.execute(
                "SELECT data FROM tasks WHERE user_email=? ORDER BY ts ASC",
                (user_email,),
            )
            rows = cur.fetchall()
            return [json.loads(row["data"]) for row in rows]

    def get_task_status(self, user_email: str, session_id: str) -> Optional[str]:
        """
        Get the status field of a specific task.

        Returns None if the task is not found.
        """
        tasks = self.get_tasks(user_email)
        for t in tasks:
            if t.get("session_id") == session_id:
                return t.get("status")
        return None

    def update_task(
        self, user_email: str, session_id: str, updates: Dict[str, Any]
    ) -> bool:
        """
        Merge the provided updates into an existing task.

        Returns True if a task was updated, False if no matching task exists.
        """
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT data FROM tasks WHERE user_email=? AND session_id=?",
                (user_email, session_id),
            ).fetchone()
            if not row:
                return False
            data = json.loads(row["data"])
            data.update(updates)
            self.conn.execute(
                "UPDATE tasks SET data=?, ts=strftime('%s','now') WHERE user_email=? AND session_id=?",
                (json.dumps(data), user_email, session_id),
            )
            return True

    def set_task_attr(
        self, user_email: str, session_id: str, key: str, value: Any
    ) -> bool:
        """Set a single attribute on a task."""
        return self.update_task(user_email, session_id, {key: value})

    def purge_old(self, keep_latest_per_user: int = 100) -> None:
        """
        Keep only the N most recent tasks for each user based on timestamp.

        Older tasks beyond this number will be deleted to prevent unbounded growth.
        """
        if keep_latest_per_user < 1:
            return
        with self._lock, self.conn:
            cur = self.conn.execute(
                "SELECT user_email, COUNT(*) as cnt FROM tasks GROUP BY user_email"
            )
            rows = cur.fetchall()
            for row in rows:
                u_email = row["user_email"]
                cnt = row["cnt"]
                if cnt > keep_latest_per_user:
                    to_delete = cnt - keep_latest_per_user
                    # Delete the oldest tasks (smallest ts)
                    self.conn.execute(
                        "DELETE FROM tasks WHERE rowid IN (SELECT rowid FROM tasks WHERE user_email=? ORDER BY ts ASC LIMIT ?)",
                        (u_email, to_delete),
                    )
