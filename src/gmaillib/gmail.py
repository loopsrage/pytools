from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Any

try:
    from googleapiclient.discovery import build
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    build = Any
    Credentials = Any
    Request = Any
    InstalledAppFlow = Any

from pydantic import ConfigDict
from pydantic_settings import BaseSettings


class GmailConfig(BaseSettings):
    model_config = ConfigDict(extra="allow")
    credentials_path: str | None = None
    token_path: str | None = None
    user_id: str = "me"
    scopes: list[str] = [
        "https://www.googleapis.com/auth/gmail.modify",
    ]
    rate_limit_delay: float = .3


class GmailClient:
    def __init__(self, config: GmailConfig | None = None):
        self.config = config or GmailConfig()
        self._service: Any | None = None

    def _credentials(self) -> Any:
        token_path = self.config.token_path or str(Path.home() / ".config" / "toolset" / "gmail_token.json")
        credentials_path = self.config.credentials_path or str(Path.home() / ".config" / "toolset" / "gmail_credentials.json")
        creds: Any = None
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, self.config.scopes)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credentials_path, self.config.scopes)
                if sys.stdin.isatty():
                    creds = flow.run_local_server(port=0)
                else:
                    creds = flow.run_console()
            Path(token_path).parent.mkdir(parents=True, exist_ok=True)
            with open(token_path, "w") as token:
                token.write(creds.to_json())
        return creds

    def service(self) -> Any:
        if self._service is None:
            self._service = build("gmail", "v1", credentials=self._credentials())
        return self._service

    def _rate_limit(self):
        if self.config.rate_limit_delay > 0:
            time.sleep(self.config.rate_limit_delay)

    def list_messages(
        self,
        query: str = "",
        max_results: int | None = 100,
    ) -> list[dict[str, Any]]:
        service = self.service()
        response = (
            service.users()
            .messages()
            .list(userId=self.config.user_id, q=query, maxResults=500)
            .execute()
        )
        self._rate_limit()
        messages = response.get("messages", [])
        while "nextPageToken" in response and (max_results is None or len(messages) < max_results):
            next_max = None if max_results is None else max(500, max_results - len(messages))
            response = (
                service.users()
                .messages()
                .list(
                    userId=self.config.user_id,
                    q=query,
                    maxResults=next_max if next_max else 500,
                    pageToken=response["nextPageToken"],
                )
                .execute()
            )
            self._rate_limit()
            messages.extend(response.get("messages", []))
        if max_results is not None:
            return messages[:max_results]
        return messages

    def list_all_messages(self, query: str = "") -> list[dict[str, Any]]:
        """List every message matching query, paginating through all history."""
        return self.list_messages(query=query, max_results=None)

    def message_ids(self, query: str = "", max_results: int | None = 100) -> list[str]:
        return [m["id"] for m in self.list_messages(query=query, max_results=max_results)]

    def all_message_ids(self, query: str = "") -> list[str]:
        return [m["id"] for m in self.list_all_messages(query=query)]

    def get_message(self, message_id: str, format: str = "metadata") -> dict[str, Any]:
        service = self.service()
        result = (
            service.users()
            .messages()
            .get(userId=self.config.user_id, id=message_id, format=format)
            .execute()
        )
        self._rate_limit()
        return result

    def modify_labels(
        self,
        message_id: str,
        add_label_ids: list[str] | None = None,
        remove_label_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        service = self.service()
        body: dict[str, Any] = {}
        if add_label_ids:
            body["addLabelIds"] = add_label_ids
        if remove_label_ids:
            body["removeLabelIds"] = remove_label_ids
        result = service.users().messages().modify(userId=self.config.user_id, id=message_id, body=body).execute()
        self._rate_limit()
        return result

    def batch_modify(
        self,
        message_ids: list[str],
        add_label_ids: list[str] | None = None,
        remove_label_ids: list[str] | None = None,
        chunk_size: int = 500,
    ) -> list[dict[str, Any]]:
        service = self.service()
        results: list[dict[str, Any]] = []
        for start in range(0, len(message_ids), chunk_size):
            chunk = message_ids[start : start + chunk_size]
            body: dict[str, Any] = {"ids": chunk}
            if add_label_ids:
                body["addLabelIds"] = add_label_ids
            if remove_label_ids:
                body["removeLabelIds"] = remove_label_ids
            results.append(service.users().messages().batchModify(userId=self.config.user_id, body=body).execute())
            self._rate_limit()
        return results

    def archive(self, message_id: str) -> dict[str, Any]:
        return self.modify_labels(message_id, remove_label_ids=["INBOX"])

    def trash(self, message_id: str) -> dict[str, Any]:
        service = self.service()
        result = service.users().messages().trash(userId=self.config.user_id, id=message_id).execute()
        self._rate_limit()
        return result

    def mark_read(self, message_id: str) -> dict[str, Any]:
        return self.modify_labels(message_id, remove_label_ids=["UNREAD"])

    def mark_unread(self, message_id: str) -> dict[str, Any]:
        return self.modify_labels(message_id, add_label_ids=["UNREAD"])

    def list_labels(self) -> list[dict[str, Any]]:
        service = self.service()
        response = service.users().labels().list(userId=self.config.user_id).execute()
        self._rate_limit()
        return response.get("labels", [])

    def label_id(self, name: str) -> str | None:
        for label in self.list_labels():
            if label.get("name") == name:
                return label.get("id")
        return None

    def create_label(
        self,
        name: str,
        label_list_visibility: str = "labelShow",
        message_list_visibility: str = "show",
    ) -> dict[str, Any]:
        existing = self.label_id(name)
        if existing:
            return existing if isinstance(existing, str) else existing.get("id")
        service = self.service()
        body = {
            "name": name,
            "labelListVisibility": label_list_visibility,
            "messageListVisibility": message_list_visibility,
        }
        created = service.users().labels().create(userId=self.config.user_id, body=body).execute()
        self._rate_limit()
        return created.get("id", name)


def _message_headers(msg: dict[str, Any]) -> dict[str, str]:
    return {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}


def _subject_words(subject: str) -> list[str]:
    clean = re.sub(r"[^\w\s]", " ", subject.lower())
    return [w for w in clean.split() if len(w) > 2]


def _sender_domain(sender: str) -> str:
    match = re.search(r"<([^>]+)>", sender)
    if match:
        addr = match.group(1)
    else:
        addr = sender.strip()
    parts = addr.split("@")
    return parts[-1].lower() if len(parts) > 1 else addr.lower()


def _sender_words(sender: str) -> list[str]:
    domain = _sender_domain(sender)
    clean = re.sub(r"[^\w\s]", " ", domain)
    return [w for w in clean.split() if len(w) > 2]


class GmailSorter:
    def __init__(self, client: GmailClient):
        self.client = client

    def sort_inbox(
        self,
        rules: dict[str, str],
        archive: bool = True,
        max_results: int | None = None,
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for label_name, query in rules.items():
            label_id = self.client.label_id(label_name) or self.client.create_label(label_name)
            label_id = label_id if isinstance(label_id, str) else label_id.get("id", label_id)
            message_ids = self.client.message_ids(query=query, max_results=max_results)
            if message_ids:
                remove_ids = ["INBOX"] if archive else None
                self.client.batch_modify(message_ids, add_label_ids=[label_id], remove_label_ids=remove_ids)
            counts[label_name] = len(message_ids)
        return counts

    def sort_by_domain(
        self,
        domain: str,
        label_name: str,
        archive: bool = True,
        max_results: int | None = None,
    ) -> int:
        return self.sort_inbox({label_name: f"from:{domain}"}, archive=archive, max_results=max_results).get(label_name, 0)

    def preview_sort(
        self,
        rules: dict[str, str],
        threshold: int = 50,
        max_results: int | None = None,
        on_progress: Any | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Dry-run: match inbox messages to rules using subject + from only.

        rules maps a label name to keyword phrases. Score = fraction of keyword
        terms found in subject + from. No labels are applied.
        """
        results: dict[str, list[dict[str, Any]]] = {}
        candidate_ids = self.client.message_ids(query="in:inbox", max_results=max_results)
        total = len(candidate_ids)
        messages: list[dict[str, Any]] = []
        for i, msg_id in enumerate(candidate_ids, 1):
            msg = self.client.get_message(msg_id, format="metadata")
            headers = _message_headers(msg)
            subject = headers.get("Subject", "")
            sender = headers.get("From", "")
            messages.append({"id": msg_id, "subject": subject, "from": sender})
            if on_progress:
                on_progress(i, total, subject)

        for label_name, keywords in rules.items():
            keyword_terms = [t for t in keywords.lower().split() if t]
            matches: list[dict[str, Any]] = []
            for m in messages:
                haystack = f"{m['subject']} {m['from']}".lower()
                haystack_words = set(haystack.split())
                if not keyword_terms:
                    continue
                matched = sum(1 for t in keyword_terms if t in haystack_words)
                score = int(round(matched / len(keyword_terms) * 100))
                if score >= threshold:
                    matches.append(
                        {
                            "id": m["id"],
                            "subject": m["subject"],
                            "from": m["from"],
                            "score": score,
                        }
                    )
            matches.sort(key=lambda m: m["score"], reverse=True)
            results[label_name] = matches
        return results

    def auto_categorize_inbox(
        self,
        threshold: int = 50,
        min_group_size: int = 2,
        max_results: int | None = None,
        page_size: int | None = None,
        on_progress: Any | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Auto-create categories from recurring sender domains across all inbox history.

        Scans every inbox message, extracts sender domain words, groups
        messages that share sender domains, and proposes a category label for each
        group where at least `threshold`% of the group's keyword terms match
        and the group has at least `min_group_size` messages.

        If `page_size` is set, processes the inbox page by page: scan a page of
        messages, create labels and archive matches, then move to the next page.
        Otherwise scans all messages first, then applies everything at the end.

        Returns a dict of category name (from sender domain) -> list of matched
        messages with scores. Labels are created and applied.
        """
        candidates = (
            self.client.all_message_ids(query="in:inbox")
            if max_results is None
            else self.client.message_ids(query="in:inbox", max_results=max_results)
        )
        total = len(candidates)

        if page_size:
            return self._auto_categorize_paged(
                candidates, total, threshold, min_group_size, page_size, on_progress
            )

        # Build per-message keyword sets from sender domain only (rate-limited get)
        messages: list[dict[str, Any]] = []
        for i, msg_id in enumerate(candidates, 1):
            msg = self.client.get_message(msg_id, format="metadata")
            headers = _message_headers(msg)
            subject = headers.get("Subject", "")
            sender = headers.get("From", "")
            words = set(_sender_words(sender))
            messages.append(
                {
                    "id": msg_id,
                    "subject": subject,
                    "from": sender,
                    "words": words,
                }
            )
            if on_progress:
                on_progress(i, total, sender)

        final = self._compute_groups(messages, threshold, min_group_size)
        return self._apply_categories(final)

    def _auto_categorize_paged(
        self,
        candidate_ids: list[str],
        total: int,
        threshold: int,
        min_group_size: int,
        page_size: int,
        on_progress: Any | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Process inbox page by page: scan a page, create labels, then next page."""
        from collections import Counter, defaultdict

        # Running state across pages
        category_label_ids: dict[str, str] = {}
        all_matches: dict[str, list[dict[str, Any]]] = defaultdict(list)

        scanned = 0
        for page_start in range(0, total, page_size):
            page_ids = candidate_ids[page_start : page_start + page_size]

            # Get message details for this page
            page_messages: list[dict[str, Any]] = []
            for i, msg_id in enumerate(page_ids, 1):
                msg = self.client.get_message(msg_id, format="metadata")
                headers = _message_headers(msg)
                subject = headers.get("Subject", "")
                sender = headers.get("From", "")
                words = set(_sender_words(sender))
                page_messages.append(
                    {
                        "id": msg_id,
                        "subject": subject,
                        "from": sender,
                        "words": words,
                    }
                )
                scanned += 1
                if on_progress:
                    on_progress(scanned, total, sender)

            # Count words across this page
            term_counter: Counter[str] = Counter()
            for m in page_messages:
                for w in m["words"]:
                    term_counter[w] += 1

            candidate_terms = {t for t, c in term_counter.items() if c >= min_group_size}

            # Group page messages by shared sender domain words
            groups: dict[str, list[dict[str, Any]]] = {}
            for m in page_messages:
                shared = [t for t in _sender_words(m["from"]) if t in candidate_terms]
                if not shared:
                    continue
                key = " ".join(shared)
                groups.setdefault(key, []).append(m)

            # Apply labels for each group in this page
            for key, group in groups.items():
                if len(group) < min_group_size:
                    continue
                key_terms = key.split()
                matches: list[dict[str, Any]] = []
                for m in group:
                    matched = sum(1 for t in key_terms if t in m["words"])
                    score = int(round(matched / len(key_terms) * 100))
                    if score >= threshold:
                        matches.append(
                            {
                                "id": m["id"],
                                "subject": m["subject"],
                                "from": m["from"],
                                "score": score,
                            }
                        )

                if len(matches) >= min_group_size:
                    domain = key.replace(" ", ".")
                    category_name = domain.title()
                    matches.sort(key=lambda m: m["score"], reverse=True)

                    # Create/reuse label
                    if category_name not in category_label_ids:
                        label_id = self.client.create_label(category_name)
                        category_label_ids[category_name] = label_id

                    label_id = category_label_ids[category_name]
                    ids = [m["id"] for m in matches]
                    self.client.batch_modify(ids, add_label_ids=[label_id], remove_label_ids=["INBOX"])

                    all_matches[category_name].extend(matches)

        return dict(all_matches)

    def _apply_categories(
        self,
        categories: dict[str, list[dict[str, Any]]],
    ) -> dict[str, list[dict[str, Any]]]:
        """Create labels and archive messages for each category."""
        for name, matches in categories.items():
            label_id = self.client.create_label(name)
            ids = [m["id"] for m in matches]
            self.client.batch_modify(ids, add_label_ids=[label_id], remove_label_ids=["INBOX"])
        return categories

    def _compute_groups(
        self,
        messages: list[dict[str, Any]],
        threshold: int,
        min_group_size: int,
    ) -> dict[str, list[dict[str, Any]]]:
        """Group messages by shared sender domain words."""
        from collections import Counter

        term_counter: Counter[str] = Counter()
        for m in messages:
            for w in m["words"]:
                term_counter[w] += 1

        candidate_terms = {t for t, c in term_counter.items() if c >= min_group_size}

        groups: dict[str, list[dict[str, Any]]] = {}
        for m in messages:
            shared = [t for t in _sender_words(m["from"]) if t in candidate_terms]
            if not shared:
                continue
            key = " ".join(shared)
            groups.setdefault(key, []).append(m)

        results: dict[str, list[dict[str, Any]]] = {}
        for key, group in groups.items():
            if len(group) < min_group_size:
                continue
            key_terms = key.split()
            matches: list[dict[str, Any]] = []
            for m in group:
                matched = sum(1 for t in key_terms if t in m["words"])
                score = int(round(matched / len(key_terms) * 100))
                if score >= threshold:
                    matches.append(
                        {
                            "id": m["id"],
                            "subject": m["subject"],
                            "from": m["from"],
                            "score": score,
                        }
                    )
            if len(matches) >= min_group_size:
                matches.sort(key=lambda m: m["score"], reverse=True)
                category_name = key.title()
                results[category_name] = matches
        return results
