# -*- coding: utf-8 -*-
# tasks.py
from datetime import datetime
from .task_state import task_manager

TERMINAL = {"completed", "cancelled", "failed"}
ALLOWED = {
    "idle": {"pending"},
    "pending": {"running", "cancelling", "failed"},
    "running": {"paused", "cancelling", "completed", "failed"},
    "paused": {"running", "cancelling", "failed"},
    "cancelling": {"cancelled", "failed"},
    "cancelled": {"idle"},
    "completed": set(),
    "failed": set(),
}


# Use the shared singleton task_manager from task_state
def _is_allowed(curr, new):
    return (curr == new) or (new in ALLOWED.get(curr or "idle", set()))


def transition(user_email: str, session_id: str, new_status: str, **fields) -> bool:
    tasks = task_manager.get_tasks(user_email)
    curr = next((t for t in tasks if t.get("session_id") == session_id), None)
    if not curr:
        # Tạo khung task tối thiểu rồi mới update
        task_manager.add_task(
            user_email, {"session_id": session_id, "status": "idle", "progress": 0}
        )

    # Lấy lại trạng thái (hoặc mặc định 'idle')
    curr_status = curr.get("status") if curr else "idle"
    # Guard: if session was locked after a crash/reset, do not allow transitions away from idle
    if curr_status == "idle" and curr and curr.get("crash_locked") and new_status != "idle":
        return False
    if not _is_allowed(curr_status, new_status):
        return False

    payload = {
        "status": new_status,
        "final": new_status in TERMINAL,
        "updated_at": datetime.utcnow().isoformat() + "Z",
        **fields,
    }
    task_manager.update_task(user_email, session_id, payload)
    return True


def reset_idle(user, sid):
    return transition(
        user, sid, "idle", final=True, progress=0, current_task=None, error=None
    )


# ---- Các thao tác idempotent (gọi từ routes & worker) ----
def start(user, sid, total_sources=0, **kw):
    return transition(
        user, sid, "pending", progress=0, total_sources=total_sources, **kw
    )


def run(user, sid, **kw):
    return transition(user, sid, "running", **kw)


def pause(user, sid):
    return transition(user, sid, "paused")


def resume(user, sid):
    return transition(user, sid, "running")


def request_cancel(user, sid):
    return transition(user, sid, "cancelling")


def mark_cancelled(user, sid):
    return transition(user, sid, "cancelled", progress=0)


def complete(user, sid):
    return transition(user, sid, "completed", progress=100)


def fail(user, sid, msg=None):
    return transition(user, sid, "failed", error=msg or "Unknown error")

