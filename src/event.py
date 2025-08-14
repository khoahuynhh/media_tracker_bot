# src/events.py
import asyncio
import json
from fastapi.responses import StreamingResponse
from .models import BaseModel
from collections import defaultdict

fallback_lock_by_session: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)


class EventBus:
    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}

    def q(self, session_id: str) -> asyncio.Queue:
        if session_id not in self._queues:
            self._queues[session_id] = asyncio.Queue()
        return self._queues[session_id]

    async def publish(self, session_id: str, event: dict):
        await self.q(session_id).put(event)


class DecisionBus:
    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}

    def q(self, session_id: str) -> asyncio.Queue:
        if session_id not in self._queues:
            self._queues[session_id] = asyncio.Queue()
        return self._queues[session_id]

    async def publish(self, session_id: str, decision: dict):
        await self.q(session_id).put(decision)

    async def wait(self, session_id: str, timeout: float = 15.0) -> dict | None:
        try:
            return await asyncio.wait_for(self.q(session_id).get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None


# ==== FE gửi quyết định (switch/abort) ====
class RunDecision(BaseModel):
    session_id: str
    action: str  # "switch" | "abort"
    provider: str | None = None  # ví dụ: "openai" | "groq" | "gemini"


event_bus = EventBus()
decision_bus = DecisionBus()


# (tùy chọn) helper để dựng SSE response trong main.py
def sse_response(queue: asyncio.Queue):
    async def gen():
        while True:
            evt = await queue.get()
            yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"

    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "text/event-stream",
        "Connection": "keep-alive",
    }
    return StreamingResponse(gen(), headers=headers)
