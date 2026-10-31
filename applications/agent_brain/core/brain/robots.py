import urllib.robotparser
from typing import Optional


class RobotsChecker:
    """Checks robots.txt to determine if a URL is legal to scrape."""

    def __init__(self):
        self._parsers: dict[str, urllib.robotparser.RobotFileParser] = {}

    def _parser_for(self, url: str) -> urllib.robotparser.RobotFileParser:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin not in self._parsers:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{origin}/robots.txt")
            try:
                rp.read()
            except Exception:
                # If robots.txt is unreadable, allow by default (conservative)
                pass
            self._parsers[origin] = rp
        return self._parsers[origin]

    def is_allowed(self, url: str, user_agent: str = "agent-brain-bot") -> bool:
        """Return True if the URL may be scraped per robots.txt."""
        try:
            rp = self._parser_for(url)
            return rp.can_fetch(user_agent, url)
        except Exception:
            return True

    def crawl_delay(self, url: str, user_agent: str = "agent-brain-bot") -> Optional[float]:
        try:
            rp = self._parser_for(url)
            delay = rp.crawl_delay(user_agent)
            return float(delay) if delay is not None else None
        except Exception:
            return None
