import asyncio

from webhook_delivery.webhook_delivery import (
    DeliveryStatus,
    WebhookDelivery,
    WebhookDeliverer,
    WebhookQueue,
)


def test_enqueue_and_list_pending():
    q = WebhookQueue("test_enqueue")
    d = q.enqueue("https://example.com/hook", {"foo": "bar"}, "order.created")
    assert d.id is not None
    assert d.status == DeliveryStatus.PENDING
    pending = q.list_pending()
    assert len(pending) == 1
    assert pending[0].id == d.id


def test_mark_sent_and_failed():
    q = WebhookQueue("test_mark")
    d = q.enqueue("https://example.com/hook", {"foo": "bar"}, "order.created")
    q.mark_sent(d.id)
    assert q._get(d.id).status == DeliveryStatus.SENT

    d2 = q.enqueue("https://example.com/hook2", {"foo": "bar"}, "order.created")
    q.mark_failed(d2.id, "connection refused")
    d2_updated = q._get(d2.id)
    assert d2_updated.attempt == 1
    assert d2_updated.last_error == "connection refused"
    assert d2_updated.status == DeliveryStatus.PENDING


def test_dead_letter_after_max_attempts():
    q = WebhookQueue("test_dead")
    d = q.enqueue("https://example.com/hook", {"foo": "bar"}, "order.created", max_attempts=1)
    q.mark_failed(d.id, "boom")
    assert q._get(d.id).status == DeliveryStatus.DEAD_LETTER
    dead = q.dead_letter()
    assert len(dead) == 1


def test_backoff_scheduling():
    q = WebhookQueue("test_backoff")
    d = q.enqueue("https://example.com/hook", {"foo": "bar"}, "order.created", max_attempts=5)
    import time

    before = time.time()
    q.mark_failed(d.id, "err1")
    after_first = q._get(d.id).next_retry
    q.mark_failed(d.id, "err2")
    after_second = q._get(d.id).next_retry
    assert after_second > after_first > before


def test_delivery_to_dict_roundtrip():
    d = WebhookDelivery(
        id="abc",
        url="https://example.com/hook",
        payload={"order": 1},
        event="order.created",
        attempt=1,
        max_attempts=3,
        status=DeliveryStatus.PENDING,
        next_retry=12345.0,
        last_error="timeout",
    )
    d2 = WebhookDelivery.from_dict(d.to_dict())
    assert d2.id == d.id
    assert d2.url == d.url
    assert d2.payload == d.payload
    assert d2.status == d.status
    assert d2.last_error == d.last_error


def test_deliverer_process_pending_success(monkeypatch):
    q = WebhookQueue("test_deliver_success")
    d = q.enqueue("https://example.com/hook", {"foo": "bar"}, "order.created")

    class FakeResp:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class FakeSession:
        def __init__(self):
            self.closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def post(self, url, **kwargs):
            return FakeResp()

    deliverer = WebhookDeliverer(q)
    deliverer._session = FakeSession()

    async def run():
        stats = await deliverer.process_pending()
        return stats

    loop = asyncio.new_event_loop()
    try:
        stats = loop.run_until_complete(run())
    finally:
        loop.close()
    assert stats["sent"] == 1
    assert q._get(d.id).status == DeliveryStatus.SENT


def test_deliverer_process_pending_failure(monkeypatch):
    q = WebhookQueue("test_deliver_fail")
    d = q.enqueue("https://example.com/hook", {"foo": "bar"}, "order.created", max_attempts=1)

    class FakeResp:
        status = 500

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class FakeSession:
        def __init__(self):
            self.closed = False

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        def post(self, url, **kwargs):
            return FakeResp()

    deliverer = WebhookDeliverer(q)
    deliverer._session = FakeSession()

    async def run():
        stats = await deliverer.process_pending()
        return stats

    loop = asyncio.new_event_loop()
    try:
        stats = loop.run_until_complete(run())
    finally:
        loop.close()
    assert stats["dead_letter"] == 1
    assert q._get(d.id).status == DeliveryStatus.DEAD_LETTER
