import unittest

import openaiunfused.openaiunfused as unfused_mod


class TestOpenAIUnfused(unittest.IsolatedAsyncioTestCase):

    async def test_unfused_empty_query_uses_onceler_cache(self):
        unfused_mod.one_time.store_once("models", "cached_model_id", lambda: ("model_obj", "tok_obj"))
        res = await unfused_mod.async_unfuzed_parse_response(
            query="",
            adapter=None,
            model="cached_model_id",
            developer_string=lambda: "sys",
        )
        assert res is None


if __name__ == "__main__":
    unittest.main()
