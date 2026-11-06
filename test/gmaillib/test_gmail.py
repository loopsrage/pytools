import os
import tempfile
import unittest
from unittest import mock

from googleapiclient.errors import HttpError

from gmaillib.gmail import GmailClient, GmailConfig, GmailSorter


class TestGmailConfig(unittest.TestCase):

    def test_defaults(self):
        cfg = GmailConfig()
        self.assertEqual(cfg.user_id, "me")
        self.assertIn("gmail.modify", cfg.scopes[0])

    def test_custom_credentials_and_token(self):
        cfg = GmailConfig(credentials_path="/path/to/creds.json", token_path="/path/to/token.json")
        self.assertEqual(cfg.credentials_path, "/path/to/creds.json")
        self.assertEqual(cfg.token_path, "/path/to/token.json")


class _FakeCreds:
    def __init__(self, valid=True, expired=False, refresh_token=None):
        self.valid = valid
        self.expired = expired
        self.refresh_token = refresh_token

    def to_json(self):
        return '{"access_token": "fake"}'

    def refresh(self, request):
        self.valid = True
        self.expired = False


class TestGmailClientInit(unittest.TestCase):

    def test_config_none_uses_defaults(self):
        client = GmailClient.__new__(GmailClient)
        GmailClient.__init__(client)
        self.assertEqual(client.config.user_id, "me")
        self.assertIsNone(client._service)

    def test_config_provided(self):
        cfg = GmailConfig(user_id="me", scopes=["https://www.googleapis.com/auth/gmail.readonly"])
        client = GmailClient.__new__(GmailClient)
        GmailClient.__init__(client, cfg)
        self.assertEqual(client.config.user_id, "me")
        self.assertEqual(client.config.scopes, ["https://www.googleapis.com/auth/gmail.readonly"])


class TestGmailClientCredentials(unittest.TestCase):

    def test_valid_existing_token_returns_creds(self):
        with tempfile.TemporaryDirectory() as tmp:
            token_path = os.path.join(tmp, "token.json")
            with open(token_path, "w") as f:
                f.write("{}")
            cfg = GmailConfig(token_path=token_path, credentials_path="/fake/creds.json")
            client = GmailClient.__new__(GmailClient)
            client.config = cfg
            with mock.patch("gmaillib.gmail.Credentials.from_authorized_user_file", return_value=_FakeCreds(valid=True)):
                creds = client._credentials()
                self.assertTrue(creds.valid)

    def test_expired_token_with_refresh(self):
        with tempfile.TemporaryDirectory() as tmp:
            token_path = os.path.join(tmp, "token.json")
            with open(token_path, "w") as f:
                f.write("{}")
            cfg = GmailConfig(token_path=token_path, credentials_path="/fake/creds.json")
            client = GmailClient.__new__(GmailClient)
            client.config = cfg
            with mock.patch("gmaillib.gmail.Credentials.from_authorized_user_file", return_value=_FakeCreds(valid=False, expired=True, refresh_token="rt")):
                with mock.patch.object(_FakeCreds, "refresh") as refresh_mock:
                    creds = client._credentials()
                    refresh_mock.assert_called_once()


class TestGmailClientMethods(unittest.TestCase):

    def _client_with_service(self):
        client = GmailClient.__new__(GmailClient)
        client.config = GmailConfig(user_id="me")
        client._service = mock.MagicMock()
        return client

    def test_list_messages(self):
        client = self._client_with_service()
        client._service.users.return_value.messages.return_value.list.return_value.execute.return_value = {
            "messages": [{"id": "1"}, {"id": "2"}],
        }
        result = client.list_messages(query="from:foo", max_results=10)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], "1")

    def test_message_ids(self):
        client = self._client_with_service()
        client.list_messages = mock.MagicMock(return_value=[{"id": "a"}, {"id": "b"}])
        assert client.message_ids(query="x") == ["a", "b"]

    def test_get_message(self):
        client = self._client_with_service()
        client._service.users.return_value.messages.return_value.get.return_value.execute.return_value = {"id": "1"}
        assert client.get_message("1") == {"id": "1"}

    def test_list_all_messages_paginates(self):
        client = self._client_with_service()
        pages = [
            {
                "messages": [
                    {"id": "1", "payload": {"headers": [{"name": "Subject", "value": "Hello"}]}},
                    {"id": "2", "payload": {"headers": [{"name": "Subject", "value": "World"}]}},
                ],
                "nextPageToken": "page2",
            },
            {"messages": [{"id": "3"}]},
        ]
        call_count = [0]

        def fake_execute():
            call_count[0] += 1
            return pages[call_count[0] - 1]

        req = mock.MagicMock()
        req.execute.side_effect = fake_execute
        client._service.users.return_value.messages.return_value.list.return_value = req
        result = client.list_all_messages(query="in:inbox")
        self.assertEqual([m["id"] for m in result], ["1", "2", "3"])
        self.assertEqual(call_count[0], 2)

    def test_all_message_ids(self):
        client = self._client_with_service()
        client.list_all_messages = mock.MagicMock(return_value=[{"id": "a"}, {"id": "b"}])
        assert client.all_message_ids(query="x") == ["a", "b"]

    def test_modify_labels_add_and_remove(self):
        client = self._client_with_service()
        client.modify_labels("1", add_label_ids=["LABEL_1"], remove_label_ids=["UNREAD"])
        client._service.users.return_value.messages.return_value.modify.assert_called_once_with(
            userId="me", id="1", body={"addLabelIds": ["LABEL_1"], "removeLabelIds": ["UNREAD"]}
        )

    def test_modify_labels_add_only(self):
        client = self._client_with_service()
        client.modify_labels("1", add_label_ids=["LABEL_1"])
        client._service.users.return_value.messages.return_value.modify.assert_called_once_with(
            userId="me", id="1", body={"addLabelIds": ["LABEL_1"]}
        )

    def test_batch_modify(self):
        client = self._client_with_service()
        client._service.users.return_value.messages.return_value.batchModify.return_value.execute.return_value = {}
        client.batch_modify(["1", "2"], add_label_ids=["LABEL_1"])
        client._service.users.return_value.messages.return_value.batchModify.assert_called_once_with(
            userId="me", body={"ids": ["1", "2"], "addLabelIds": ["LABEL_1"]}
        )

    def test_archive_removes_inbox(self):
        client = self._client_with_service()
        client.modify_labels = mock.MagicMock()
        client.archive("1")
        client.modify_labels.assert_called_once_with("1", remove_label_ids=["INBOX"])

    def test_trash(self):
        client = self._client_with_service()
        client.trash("1")
        client._service.users.return_value.messages.return_value.trash.assert_called_once_with(userId="me", id="1")

    def test_mark_read_and_unread(self):
        client = self._client_with_service()
        client.modify_labels = mock.MagicMock()
        client.mark_read("1")
        client.modify_labels.assert_called_with("1", remove_label_ids=["UNREAD"])
        client.mark_unread("1")
        client.modify_labels.assert_called_with("1", add_label_ids=["UNREAD"])

    def test_list_labels(self):
        client = self._client_with_service()
        client._service.users.return_value.labels.return_value.list.return_value.execute.return_value = {
            "labels": [{"id": "L1", "name": "Work"}]
        }
        assert client.list_labels() == [{"id": "L1", "name": "Work"}]

    def test_label_id_found_and_not_found(self):
        client = self._client_with_service()
        client.list_labels = mock.MagicMock(return_value=[{"id": "L1", "name": "Work"}])
        self.assertEqual(client.label_id("Work"), "L1")
        self.assertIsNone(client.label_id("Missing"))

    def test_create_label_existing(self):
        client = self._client_with_service()
        client.label_id = mock.MagicMock(return_value="L1")
        result = client.create_label("Work")
        self.assertEqual(result, "L1")
        client._service.users.return_value.labels.return_value.create.assert_not_called()

    def test_create_label_new(self):
        client = self._client_with_service()
        client.label_id = mock.MagicMock(return_value=None)
        client._service.users.return_value.labels.return_value.create.return_value.execute.return_value = {
            "id": "L2",
            "name": "Work",
        }
        result = client.create_label("Work")
        self.assertEqual(result, "L2")
        client._service.users.return_value.labels.return_value.create.assert_called_once()


class TestGmailSorter(unittest.TestCase):

    def _sorter(self):
        client = mock.MagicMock(spec=GmailClient)
        from gmaillib.gmail import GmailSorter
        return GmailSorter(client), client

    def test_sort_inbox_archives(self):
        sorter, client = self._sorter()
        client.label_id.return_value = None
        client.create_label.return_value = "L1"
        client.message_ids.return_value = ["m1", "m2"]
        counts = sorter.sort_inbox({"Work": "from:work.com"}, archive=True, max_results=5)
        self.assertEqual(counts, {"Work": 2})
        client.batch_modify.assert_called_once_with(["m1", "m2"], add_label_ids=["L1"], remove_label_ids=["INBOX"])

    def test_sort_inbox_no_archive(self):
        sorter, client = self._sorter()
        client.label_id.return_value = None
        client.create_label.return_value = "L1"
        client.message_ids.return_value = []
        counts = sorter.sort_inbox({"Work": "from:work.com"}, archive=False, max_results=5)
        self.assertEqual(counts, {"Work": 0})
        client.batch_modify.assert_not_called()

    def test_sort_inbox_uses_existing_label(self):
        sorter, client = self._sorter()
        client.label_id.return_value = "L1"
        client.message_ids.return_value = ["m1"]
        sorter.sort_inbox({"Work": "from:work.com"})
        client.create_label.assert_not_called()
        client.batch_modify.assert_called_once_with(["m1"], add_label_ids=["L1"], remove_label_ids=["INBOX"])

    def test_sort_by_domain(self):
        sorter, client = self._sorter()
        sorter.sort_inbox = mock.MagicMock(return_value={"Example": 3})
        result = sorter.sort_by_domain("example.com", "Example")
        self.assertEqual(result, 3)
        sorter.sort_inbox.assert_called_once_with({"Example": "from:example.com"}, archive=True, max_results=None)

    def test_preview_sort_fuzzy_matches(self):
        sorter, client = self._sorter()
        client.message_ids.return_value = ["m1", "m2", "m3"]

        def fake_get(message_id, format="metadata"):
            return {
                "m1": {
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Your Rocky Mountain Power bill"},
                            {"name": "From", "value": "billing@rockymountainpower.com"},
                        ]
                    },
                },
                "m2": {
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Invoice from Amazon"},
                            {"name": "From", "value": "amazon@amazon.com"},
                        ]
                    },
                },
                "m3": {
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Weekly newsletter"},
                            {"name": "From", "value": "news@example.com"},
                        ]
                    },
                },
            }[message_id]

        client.get_message.side_effect = fake_get
        results = sorter.preview_sort({"Bills": "rocky mountain power bill"}, threshold=50, max_results=100)
        self.assertIn("Bills", results)
        bill_ids = {m["id"] for m in results["Bills"]}
        self.assertIn("m1", bill_ids)
        self.assertNotIn("m2", bill_ids)
        self.assertNotIn("m3", bill_ids)
        self.assertGreaterEqual(results["Bills"][0]["score"], 50)

    def test_auto_categorize_inbox_groups_subjects(self):
        sorter, client = self._sorter()
        client.all_message_ids = mock.MagicMock(return_value=["m1", "m2", "m3", "m4"])

        def fake_get(message_id, format="metadata"):
            return {
                "m1": {
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Your Rocky Mountain Power bill"},
                            {"name": "From", "value": "billing@rockymountainpower.com"},
                        ]
                    },
                },
                "m2": {
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Rocky Mountain Power outage alert"},
                            {"name": "From", "value": "alerts@rockymountainpower.com"},
                        ]
                    },
                },
                "m3": {
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Invoice from Amazon"},
                            {"name": "From", "value": "amazon@amazon.com"},
                        ]
                    },
                },
                "m4": {
                    "payload": {
                        "headers": [
                            {"name": "Subject", "value": "Amazon order confirmation"},
                            {"name": "From", "value": "amazon@amazon.com"},
                        ]
                    },
                },
            }[message_id]

        client.get_message.side_effect = fake_get
        results = sorter.auto_categorize_inbox(threshold=50, min_group_size=2)
        category_names = set(results.keys())
        self.assertTrue(any("Rockymountainpower" in n for n in category_names))
        self.assertTrue(any("Amazon" in n for n in category_names))

    def test_auto_categorize_inbox_below_threshold(self):
        sorter, client = self._sorter()
        client.all_message_ids = mock.MagicMock(return_value=["m1"])
        client.get_message.return_value = {
            "payload": {
                "headers": [
                    {"name": "Subject", "value": "Unique one-off subject"},
                    {"name": "From", "value": "sender@example.com"},
                ]
            },
        }
        results = sorter.auto_categorize_inbox(threshold=50, min_group_size=2)
        self.assertEqual(results, {})


class TestGmailIntegration(unittest.TestCase):
    """Real Gmail API tests. Skipped unless local credentials exist."""

    @classmethod
    def setUpClass(cls):
        creds_path = os.path.expanduser("~/.config/toolset/gmail_credentials.json")
        token_path = os.path.expanduser("~/.config/toolset/gmail_token.json")
        cls.enabled = os.path.exists(creds_path) and os.path.exists(token_path)

    def test_real_list_labels(self):
        if not self.enabled:
            self.skipTest("gmail credentials not present at ~/.config/toolset/")
        client = GmailClient()
        try:
            labels = client.list_labels()
        except HttpError as e:
            if "rateLimitExceeded" in str(e):
                self.skipTest("Gmail API rate limit exceeded")
            raise
        self.assertIsInstance(labels, list)

    def test_real_sort_inbox(self):
        if not self.enabled:
            self.skipTest("gmail credentials not present at ~/.config/toolset/")
        client = GmailClient()
        sorter = GmailSorter(client)
        try:
            counts = sorter.sort_inbox({"TestSort": "is:unread"}, archive=True, max_results=5)
        except HttpError as e:
            if "rateLimitExceeded" in str(e):
                self.skipTest("Gmail API rate limit exceeded")
            raise
        self.assertIsInstance(counts, dict)


if __name__ == "__main__":
    unittest.main()
