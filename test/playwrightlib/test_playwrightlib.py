import unittest

from fsspecc import FSBase
from meta_models.working_service import WorkerServiceConfig
from playwrightlib.playwright import Playwright, PlaywrightSettings, ScrapeUrls, page_node_storage
from queue_controller.helpers import new_controller


class TestPlaywrightLib(unittest.TestCase):

    def test_playwright_models_and_setup(self):
        url_cfg = ScrapeUrls(url="https://example.com", selectors=["a", "els => els.map(e => e.href)"], filter=r"\.pdf$")
        settings = PlaywrightSettings(
            worker=WorkerServiceConfig(identity="pw_worker"),
            headless=True,
            scrape_urls=[url_cfg],
        )
        assert settings.headless is True
        assert len(settings.scrape_urls) == 1

        fs = FSBase("memory")
        node = page_node_storage(fs, "/downloads")
        pw = Playwright(urls=[url_cfg], output=new_controller(identity="out"))
        assert pw.p_node.identity == "out"
        assert node is not None


if __name__ == "__main__":
    unittest.main()
