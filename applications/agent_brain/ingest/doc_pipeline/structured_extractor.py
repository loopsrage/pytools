import re
from typing import Optional


def extract_salary(text: str) -> Optional[int]:
    """Extract minimum annual salary in USD from text. Returns None if not found."""
    patterns = [
        r"\$\s*(\d[\d,]*)\s*-\s*\$\s*(\d[\d,]*)",
        r"\$\s*(\d[\d,]*)\s*a year",
        r"\$\s*(\d[\d,]*)\s*per year",
        r"(\d[\d,]*)\s*-\s*(\d[\d,]*)\s*a year",
        r"(\d[\d,]*)\s*per year",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            groups = m.groups()
            nums = []
            for g in groups:
                try:
                    nums.append(int(g.replace(",", "")))
                except ValueError:
                    continue
            if nums:
                return min(nums)
    return None


def extract_hourly(text: str) -> Optional[float]:
    """Extract hourly rate in USD. Converts to annual assuming 2080 hrs/yr."""
    m = re.search(r"\$\s*(\d[\d,]*)\s*(?:an hour|per hour|/hr|/hour)", text, re.I)
    if m:
        try:
            rate = float(m.group(1).replace(",", ""))
            return rate * 2080
        except ValueError:
            return None
    return None


def extract_location(text: str) -> Optional[str]:
    """Extract US location hint from text. Returns 'United States' when a US
    state or city is detected, otherwise None."""
    us_states = [
        "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
        "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
        "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
        "maine", "maryland", "massachusetts", "michigan", "minnesota",
        "mississippi", "missouri", "montana", "nebraska", "nevada",
        "new hampshire", "new jersey", "new mexico", "new york",
        "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
        "pennsylvania", "rhode island", "south carolina", "south dakota",
        "tennessee", "texas", "utah", "vermont", "virginia", "washington",
        "west virginia", "wisconsin", "wyoming",
    ]
    us_cities = [
        "new york", "los angeles", "chicago", "houston", "phoenix",
        "philadelphia", "san antonio", "san diego", "dallas", "san jose",
        "austin", "jacksonville", "fort worth", "columbus", "charlotte",
        "san francisco", "indianapolis", "seattle", "denver", "washington dc",
        "boston", "nashville", "baltimore", "portland", "las vegas",
        "salt lake city", "provo", "lehi", "draper", "ogden",
    ]
    lower = text.lower()
    for city in us_cities:
        if city in lower:
            return "United States"
    for state in us_states:
        if state in lower:
            return "United States"
    return None


def is_remote(text: str) -> Optional[bool]:
    """Detect if the posting indicates remote work."""
    lower = text.lower()
    remote_pos = any(k in lower for k in [
        "remote", "work from home", "wfh", "anywhere", "fully remote",
        "distributed", "telecommute",
    ])
    onsite_pos = any(k in lower for k in [
        "onsite", "on-site", "in office", "in-office", "hybrid",
        "relocation required", "must be located",
    ])
    if remote_pos and not onsite_pos:
        return True
    if onsite_pos and not remote_pos:
        return False
    return None


def requires_drug_test(text: str) -> Optional[bool]:
    """Detect if the posting mentions drug testing."""
    lower = text.lower()
    if any(k in lower for k in [
        "drug test", "drug screening", "background check",
        "must pass drug", "drug-free workplace",
    ]):
        return True
    if any(k in lower for k in [
        "no drug test", "no drug screening", "no background check",
    ]):
        return False
    return None


def requires_portfolio(text: str) -> Optional[bool]:
    """Detect if the posting requires a portfolio."""
    lower = text.lower()
    if any(k in lower for k in [
        "portfolio required", "portfolio needed", "submit portfolio",
        "link to portfolio", "portfolio link", "work samples required",
    ]):
        return True
    if any(k in lower for k in [
        "no portfolio", "portfolio not required", "portfolio optional",
    ]):
        return False
    return None


def extract_structured_fields(text: str) -> dict:
    """Extract all structured job fields from raw text."""
    salary = extract_salary(text) or extract_hourly(text)
    return {
        "salary_min_usd": salary,
        "location": extract_location(text),
        "remote": is_remote(text),
        "drug_testing": requires_drug_test(text) or False,
        "portfolio_required": requires_portfolio(text) or False,
    }
