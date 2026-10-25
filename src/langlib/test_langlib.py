import unittest
from collections import namedtuple
from langchain_core.tools import tool

from langchain_agent_ltm_stm.agent import pii_types
from langlib.langlib import (
    _RAG_message,
    _system_message,
    asearch_past_memories,
    format_past_memories,
    search_past_memories,
)
from langlib.middleware import hil_middleware, pii_middleware

MemoryItem = namedtuple("MemoryItem", ["value", "score"])


class InMemoryStore:
    def __init__(self):
        self.items = [
            MemoryItem(value={"text": "remember_high"}, score=0.95),
            MemoryItem(value={"text": "ignore_low"}, score=0.40),
        ]

    def search(self, user_id, namespace, query):
        return self.items

    async def asearch(self, user_id, namespace, query):
        return self.items


class TestLangLib(unittest.IsolatedAsyncioTestCase):

    async def test_memory_formatting_and_middlewares(self):
        assert "### CONTEXT" in _system_message("abc").content
        assert "### EXTERNAL CONTEXT" in _RAG_message("xyz").content

        store = InMemoryStore()
        sync_msg = search_past_memories(store, "u1", "ns", "q", threshold=0.9)
        async_msg = await asearch_past_memories(store, "u1", "ns", "q", threshold=0.9)
        assert "[remember_high:0.95]" in sync_msg.content
        assert "ignore_low" not in sync_msg.content
        assert sync_msg.content == async_msg.content

        empty_sync = search_past_memories(None, "u1", "ns", "q", threshold=0.9)
        empty_async = await asearch_past_memories(None, "u1", "ns", "q", threshold=0.9)
        assert empty_sync.content == empty_async.content

        assert format_past_memories([MemoryItem(value={"text": "x"}, score=1.0)]) == "[x:1.00]"

        pii_mws = pii_middleware(pii_types(), strategy="hash")
        assert len(pii_mws) == len(pii_types())

        @tool
        def sample_tool(x: int) -> int:
            """Sample tool"""
            return x

        hil_mw = hil_middleware([sample_tool])
        assert hil_mw is not None


if __name__ == "__main__":
    unittest.main()
