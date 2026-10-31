"""Email capability for Agent Brain: Gmail sender + Qdrant-backed templates."""

from applications.agent_brain.core.email.sender import GmailSender, EmailSendError
from applications.agent_brain.core.email.templates import EmailTemplateStore

__all__ = ["GmailSender", "EmailSendError", "EmailTemplateStore"]
