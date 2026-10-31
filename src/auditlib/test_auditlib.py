from auditlib.auditlib import AuditLog


def test_append_and_verify_chain():
    log = AuditLog("test_chain")
    log.append("alice", "create", "order/1", {"amount": 100})
    log.append("bob", "update", "order/1", {"amount": 200})
    assert log.count() == 2
    assert log.verify() is True


def test_tamper_detection():
    log = AuditLog("test_tamper")
    log.append("alice", "create", "order/1", {"amount": 100})
    entries = log.entries()
    tampered = dict(entries[0])
    tampered["details"] = {"amount": 9999}
    log._index.store_in_index(log._name, entries[0]["entry_id"], tampered)
    assert log.verify() is False


def test_broken_chain_detection():
    log = AuditLog("test_chain_break")
    log.append("alice", "create", "r/1", {})
    log.append("bob", "update", "r/2", {})
    e2 = dict(log.entries()[1])
    e2["previous_hash"] = "deadbeef"
    log._index.store_in_index(log._name, e2["entry_id"], e2)
    assert log.verify() is False


def test_get_entry():
    log = AuditLog("test_get")
    entry = log.append("alice", "create", "r/1", {})
    fetched = log.get(entry.entry_id)
    assert fetched is not None
    assert fetched["actor"] == "alice"
    assert fetched["action"] == "create"
