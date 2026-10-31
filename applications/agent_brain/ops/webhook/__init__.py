"""Reliable webhook delivery for agent notifications.

Wraps :class:`src.webhook_delivery.webhook_delivery.WebhookQueue` and
:class:`src.webhook_delivery.webhook_delivery.WebhookDeliverer` to provide
reliable webhook delivery with exponential backoff retries and dead-letter
queue handling.

Designed for agents to notify external systems of events (scrapes complete,
models loaded, anomalies detected, etc.).
"""

from __future__ import annotations

import asyncio
from typing import Optional

_REPO_SRC = str(__import__("pathlib").Path(__file__).resolve().parents[4] / "src")
import sys

if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from webhook_delivery.webhook_delivery import (  # noqa: E402
    DeliveryStatus,
    WebhookDelivery,
    WebhookDeliverer,
    WebhookQueue,
)


class AgentWebhookQueue:
    """Reliable webhook delivery queue.

    Enqueues webhook deliveries with automatic retry (exponential backoff)
    and dead-lettering after max attempts. Integrates with the agent brain's
    async event loop.
    """

    def __init__(self, namespace: str = "agent_brain_webhooks"):
        self._queue = WebhookQueue(namespace=namespace)
        self._deliverer: Optional[WebhookDeliverer] = None

    def enqueue(
        self,
        url: str,
        payload: dict,
        event: str,
        max_attempts: int = 3,
    ):
        """Enqueue a webhook delivery. Returns the delivery object."""
        return self._queue.enqueue(
            url=url, payload=payload, event=event, max_attempts=max_attempts
        )

    def list_pending(self):
        """List deliveries pending retry (next_retry <= now)."""
        return self._queue.list_pending()

    def mark_sent(self, delivery_id: str) -> None:
        """Mark a delivery as successfully sent."""
        self._queue.mark_sent(delivery_id)

    def mark_failed(self, delivery_id: str, error: str) -> None:
        """Mark a delivery as failed; schedules retry or dead-letter."""
        self._queue.mark_failed(delivery_id, error)

    def dead_letter(self):
        """List all dead-letter deliveries."""
        return self._queue.dead_letter()

    def deliverer(self, timeout: float = 10.0) -> WebhookDeliverer:
        """Return (or create) the HTTP deliverer for this queue."""
        if self._deliverer is None:
            self._deliverer = WebhookDeliverer(queue=self._queue, timeout=timeout)
        return self._deliverer

    async def process_pending(self) -> dict:
        """Process all pending deliveries: send, retry, or dead-letter."""
        deliverer = self.deliverer()
        return await deliverer.process_pending()

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._deliverer is not None:
            await self._deliverer.close()


__all__ = [
    "AgentWebhookQueue",
    "WebhookDelivery",
    "WebhookDeliverer",
    "WebhookQueue",
    "DeliveryStatus",
]
