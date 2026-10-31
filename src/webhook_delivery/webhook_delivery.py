import asyncio
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import aiohttp

from thread_safe.index import Index


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


@dataclass
class WebhookDelivery:
    url: str
    payload: dict
    event: str
    attempt: int = 0
    max_attempts: int = 3
    status: DeliveryStatus = DeliveryStatus.PENDING
    next_retry: float = field(default_factory=time.time)
    last_error: Optional[str] = None
    id: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "url": self.url,
            "payload": self.payload,
            "event": self.event,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "status": self.status.value,
            "next_retry": self.next_retry,
            "last_error": self.last_error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WebhookDelivery":
        return cls(
            id=data.get("id"),
            url=data["url"],
            payload=data["payload"],
            event=data["event"],
            attempt=data.get("attempt", 0),
            max_attempts=data.get("max_attempts", 3),
            status=DeliveryStatus(data.get("status", "pending")),
            next_retry=data.get("next_retry", time.time()),
            last_error=data.get("last_error"),
        )


class WebhookQueue:
    def __init__(self, namespace: str = "webhook_queue"):
        self._index = Index()
        self._namespace = namespace
        self._lock = asyncio.Lock()

    def _get(self, delivery_id: str) -> Optional[WebhookDelivery]:
        try:
            data = self._index.load_from_index(self._namespace, delivery_id)
            return WebhookDelivery.from_dict(data) if data else None
        except KeyError:
            return None

    def _put(self, delivery: WebhookDelivery) -> None:
        self._index.store_in_index(self._namespace, delivery.id or delivery.url, delivery.to_dict())

    def enqueue(self, url: str, payload: dict, event: str, max_attempts: int = 3) -> WebhookDelivery:
        import uuid

        delivery = WebhookDelivery(
            id=str(uuid.uuid4()),
            url=url,
            payload=payload,
            event=event,
            max_attempts=max_attempts,
            next_retry=time.time(),
        )
        self._put(delivery)
        return delivery

    def list_pending(self) -> list[WebhookDelivery]:
        now = time.time()
        out = []
        try:
            for _, data in self._index.range_index(self._namespace):
                if data is None:
                    continue
                d = WebhookDelivery.from_dict(data)
                if d.status == DeliveryStatus.PENDING and d.next_retry <= now:
                    out.append(d)
        except KeyError:
            pass
        return out

    def mark_sent(self, delivery_id: str) -> None:
        d = self._get(delivery_id)
        if d:
            d.status = DeliveryStatus.SENT
            self._put(d)

    def mark_failed(self, delivery_id: str, error: str) -> None:
        d = self._get(delivery_id)
        if not d:
            return
        d.attempt += 1
        d.last_error = error
        if d.attempt >= d.max_attempts:
            d.status = DeliveryStatus.DEAD_LETTER
        else:
            # Exponential backoff
            d.next_retry = time.time() + (2 ** d.attempt)
            d.status = DeliveryStatus.PENDING
        self._put(d)

    def dead_letter(self) -> list[WebhookDelivery]:
        out = []
        try:
            for _, data in self._index.range_index(self._namespace):
                if data is None:
                    continue
                d = WebhookDelivery.from_dict(data)
                if d.status == DeliveryStatus.DEAD_LETTER:
                    out.append(d)
        except KeyError:
            pass
        return out


class WebhookDeliverer:
    def __init__(self, queue: WebhookQueue, timeout: float = 10.0):
        self.queue = queue
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None

    async def _session_get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self.timeout))
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def deliver(self, delivery: WebhookDelivery) -> bool:
        session = await self._session_get()
        try:
            async with session.post(
                delivery.url,
                json={"event": delivery.event, "payload": delivery.payload},
                headers={"Content-Type": "application/json", "User-Agent": "webhook-deliverer/1.0"},
            ) as resp:
                if 200 <= resp.status < 300:
                    return True
                raise Exception(f"HTTP {resp.status}")
        except Exception as exc:
            self.queue.mark_failed(delivery.id or delivery.url, str(exc))
            return False

    async def process_pending(self) -> dict[str, int]:
        stats = {"sent": 0, "failed": 0, "dead_letter": 0}
        pending = self.queue.list_pending()
        for delivery in pending:
            success = await self.deliver(delivery)
            if success:
                self.queue.mark_sent(delivery.id or delivery.url)
                stats["sent"] += 1
            elif self.queue._get(delivery.id or delivery.url).status == DeliveryStatus.DEAD_LETTER:
                stats["dead_letter"] += 1
            else:
                stats["failed"] += 1
        return stats
