"""Gmail SMTP email sender for the Agent Brain.

Reads connection configuration from ``agent_brain.email`` in settings.yaml and
resolves the Gmail app password from a secret (env ``GMAIL_APP_PASSWORD`` or
file ``GMAIL_APP_PASSWORD_FILE``) via :class:`AgentSecrets`. Credentials are
never stored in settings.yaml.
"""

from __future__ import annotations

import smtplib
import sys
from email.message import EmailMessage
from pathlib import Path
from typing import Optional

_REPO_SRC = str(Path(__file__).resolve().parents[4] / "src")
if _REPO_SRC not in sys.path:
    sys.path.insert(0, _REPO_SRC)

from applications.agent_brain.core.settings import get  # noqa: E402
from applications.agent_brain.ops.secrets import AgentSecrets  # noqa: E402


class EmailSendError(RuntimeError):
    """Raised when an email cannot be sent."""


class GmailSender:
    """Send email through a shared Gmail account over SMTP (STARTTLS)."""

    def __init__(self, secrets: Optional[AgentSecrets] = None):
        self._secrets = secrets or AgentSecrets()

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------
    def _enabled(self) -> bool:
        return bool(get("email.enabled", False))

    def _smtp_host(self) -> str:
        return str(get("email.smtp_host", "smtp.gmail.com"))

    def _smtp_port(self) -> int:
        return int(get("email.smtp_port", 587))

    def _use_tls(self) -> bool:
        return bool(get("email.use_tls", True))

    def _username(self) -> str:
        return str(get("email.username", ""))

    def _from_name(self) -> str:
        return str(get("email.from_name", "Agent Brain"))

    def _app_password(self) -> str:
        """Resolve the Gmail app password from a secret."""
        return self._secrets.required("GMAIL_APP_PASSWORD")

    # ------------------------------------------------------------------
    # Sending
    # ------------------------------------------------------------------
    def is_configured(self) -> bool:
        """Return True when email is enabled and credentials are present."""
        if not self._enabled():
            return False
        if not self._username():
            return False
        try:
            self._app_password()
        except Exception:
            return False
        return True

    def send(
        self,
        to: str,
        subject: str,
        body: str,
        *,
        from_name: Optional[str] = None,
        reply_to: Optional[str] = None,
    ) -> dict:
        """Send a plain-text email.

        Returns a dict with ``status`` (``"sent"``) and message metadata, or
        raises :class:`EmailSendError` on failure.
        """
        if not self._enabled():
            raise EmailSendError("Email sending is not enabled in settings")

        username = self._username()
        if not username:
            raise EmailSendError("Gmail username is not configured")

        password = self._app_password()

        msg = EmailMessage()
        msg["From"] = f"{from_name or self._from_name()} <{username}>"
        msg["To"] = to
        msg["Subject"] = subject
        if reply_to:
            msg["Reply-To"] = reply_to
        msg.set_content(body)

        host = self._smtp_host()
        port = self._smtp_port()
        use_tls = self._use_tls()

        try:
            if use_tls:
                server = smtplib.SMTP(host, port, timeout=30)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(host, port, timeout=30)
            server.login(username, password)
            server.send_message(msg)
            server.quit()
        except Exception as exc:
            raise EmailSendError(f"Failed to send email to {to}: {exc}") from exc

        return {
            "status": "sent",
            "to": to,
            "subject": subject,
            "from": username,
            "smtp_host": host,
            "smtp_port": port,
        }

    def send_templated(
        self,
        to: str,
        template_subject: str,
        template_body: str,
        variables: Optional[dict] = None,
        *,
        from_name: Optional[str] = None,
    ) -> dict:
        """Render ``{variable}`` placeholders and send the email."""
        vars_map = dict(variables or {})
        subject = self._render(template_subject, vars_map)
        body = self._render(template_body, vars_map)
        return self.send(to, subject, body, from_name=from_name)

    @staticmethod
    def _render(template: str, variables: dict) -> str:
        """Safely render ``{var}`` placeholders, leaving unknowns intact."""
        from string import Formatter

        class _SafeFormatter(Formatter):
            def get_value(self, key, args, kwargs):
                if isinstance(key, str):
                    return kwargs.get(key, "{" + key + "}")
                return super().get_value(key, args, kwargs)

        return _SafeFormatter().vformat(template, (), variables)

    # ------------------------------------------------------------------
    # SMS via Google Fi email-to-SMS gateway
    # ------------------------------------------------------------------
    @staticmethod
    def _fi_sms_address(phone: str) -> str:
        """Convert a phone number to its Google Fi email-to-SMS address."""
        digits = "".join(ch for ch in phone if ch.isdigit())
        return f"{digits}@msg.fi.google.com"

    def send_sms(
        self,
        to: str,
        body: str,
        *,
        subject: Optional[str] = None,
        from_name: Optional[str] = None,
    ) -> dict:
        """Send an SMS to a phone number via the Google Fi email-to-SMS gateway.

        ``to`` can be a raw digit string (e.g. ``8017091149``) or a full
        ``@msg.fi.google.com`` address.
        """
        if "@" in to:
            recipient = to
        else:
            recipient = self._fi_sms_address(to)
        return self.send(
            recipient,
            subject or "Agent Brain",
            body,
            from_name=from_name,
        )

    def send_templated_sms(
        self,
        to: str,
        template_body: str,
        variables: Optional[dict] = None,
        *,
        subject: Optional[str] = None,
        from_name: Optional[str] = None,
    ) -> dict:
        """Render a template body and send it as an SMS via Google Fi."""
        body = self._render(template_body, variables or {})
        return self.send_sms(to, body, subject=subject, from_name=from_name)
