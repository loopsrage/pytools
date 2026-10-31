"""Tests for the Agent Brain email capability (Gmail config + sender + templates)."""

from unittest.mock import MagicMock, patch

import pytest

from applications.agent_brain.core.settings import load, get
from applications.agent_brain.core.email import (
    GmailSender,
    EmailTemplateStore,
    EmailSendError,
)


def test_email_config_loaded():
    load()
    assert get("email.enabled") is True
    assert get("email.provider") == "gmail"
    assert get("email.smtp_host") == "smtp.gmail.com"
    assert get("email.smtp_port") == 587
    assert get("email.use_tls") is True
    assert get("email.username") == "loop123jk@gmail.com"
    assert get("email.from_name") == "Agent Brain"


def test_sender_not_configured_when_disabled():
    load()
    sender = GmailSender()
    assert sender.is_configured() is False


@patch("applications.agent_brain.core.email.sender.smtplib.SMTP")
def test_send_success(mock_smtp_cls):
    load()
    mock_server = MagicMock()
    mock_smtp_cls.return_value = mock_server

    sender = GmailSender()
    sender._enabled = lambda: True  # noqa: SLF001
    sender._username = lambda: "shared.account@gmail.com"  # noqa: SLF001
    sender._app_password = lambda: "app-password-123"  # noqa: SLF001

    result = sender.send("recipient@example.com", "Hello", "Body text")
    assert result["status"] == "sent"
    assert result["to"] == "recipient@example.com"
    mock_smtp_cls.assert_called_once_with("smtp.gmail.com", 587, timeout=30)
    mock_server.starttls.assert_called_once()
    mock_server.login.assert_called_once_with("shared.account@gmail.com", "app-password-123")
    mock_server.send_message.assert_called_once()
    mock_server.quit.assert_called_once()


def test_send_not_enabled_raises():
    sender = GmailSender()
    sender._enabled = lambda: False  # noqa: SLF001
    with pytest.raises(EmailSendError):
        sender.send("recipient@example.com", "S", "B")


def test_render_leaves_unknown_variables_intact():
    sender = GmailSender()
    out = sender._render("Hi {name}, apply to {company}", {"name": "Ada"})  # noqa: SLF001
    assert out == "Hi Ada, apply to {company}"


def test_fi_sms_address():
    sender = GmailSender()
    assert sender._fi_sms_address("8017091149") == "8017091149@msg.fi.google.com"  # noqa: SLF001
    assert sender._fi_sms_address("(801) 709-1149") == "8017091149@msg.fi.google.com"  # noqa: SLF001


def test_template_store_roundtrip(tmp_path):
    store = EmailTemplateStore(qdrant_path=str(tmp_path / ".qdrant_email"))
    stored = store.write_template(
        template_id="job-app-1",
        name="Job Application",
        category="job_application",
        subject="Application for {role} at {company}",
        body="Dear {hiring_manager}, I am applying for the {role} role.",
        variables=["role", "company", "hiring_manager"],
    )
    assert stored["template_id"] == "job-app-1"
    assert stored["category"] == "job_application"

    fetched = store.get_template("job-app-1")
    assert fetched is not None
    assert fetched["name"] == "Job Application"

    rendered = store.render_template(
        "job-app-1", {"role": "Engineer", "company": "Acme", "hiring_manager": "Team"}
    )
    assert rendered["subject"] == "Application for Engineer at Acme"
    assert rendered["body"] == "Dear Team, I am applying for the Engineer role."

    # Unknown variable left intact
    rendered2 = store.render_template("job-app-1", {"role": "Engineer"})
    assert "{company}" in rendered2["subject"]

    templates = store.list_templates()
    assert len(templates) == 1

    assert store.delete_template("job-app-1") is True
    assert store.get_template("job-app-1") is None
    assert store.delete_template("missing") is False


def test_template_store_search(tmp_path):
    store = EmailTemplateStore(qdrant_path=str(tmp_path / ".qdrant_email_search"))
    store.write_template(
        template_id="alert-1",
        name="Low Balance Alert",
        category="alert",
        subject="Alert: low balance",
        body="Your account balance is below the threshold.",
        variables=[],
    )
    store.write_template(
        template_id="apply-1",
        name="Job Apply",
        category="job_application",
        subject="Application for software engineer",
        body="Please consider me for the software engineer position.",
        variables=[],
    )
    results = store.search_templates("job application software engineer", top_k=3)
    assert len(results) == 2
    ids = {r["template_id"] for r in results}
    assert ids == {"alert-1", "apply-1"}
    # The job application template should score higher for the job query.
    by_id = {r["template_id"]: r for r in results}
    assert by_id["apply-1"]["score"] >= by_id["alert-1"]["score"]


@patch("applications.agent_brain.core.email.sender.smtplib.SMTP")
def test_send_sms_routes_to_fi_gateway(mock_smtp_cls):
    mock_server = MagicMock()
    mock_smtp_cls.return_value = mock_server

    sender = GmailSender()
    sender._enabled = lambda: True  # noqa: SLF001
    sender._username = lambda: "loop123jk@gmail.com"  # noqa: SLF001
    sender._app_password = lambda: "app-password-123"  # noqa: SLF001

    result = sender.send_sms("8017091149", "Hello via Fi")
    assert result["status"] == "sent"
    mock_smtp_cls.assert_called_once_with("smtp.gmail.com", 587, timeout=30)
    sent_msg = mock_server.send_message.call_args[0][0]
    assert sent_msg["To"] == "8017091149@msg.fi.google.com"
    assert sent_msg.get_content().strip() == "Hello via Fi"


@patch("applications.agent_brain.core.email.sender.smtplib.SMTP")
def test_send_templated_sms_renders(mock_smtp_cls):
    mock_server = MagicMock()
    mock_smtp_cls.return_value = mock_server

    sender = GmailSender()
    sender._enabled = lambda: True  # noqa: SLF001
    sender._username = lambda: "loop123jk@gmail.com"  # noqa: SLF001
    sender._app_password = lambda: "app-password-123"  # noqa: SLF001

    result = sender.send_templated_sms(
        "8017091149", "Application for {role} at {company} submitted.",
        {"role": "Engineer", "company": "Acme"},
    )
    assert result["status"] == "sent"
    sent_msg = mock_server.send_message.call_args[0][0]
    assert sent_msg.get_content().strip() == "Application for Engineer at Acme submitted."
