import asyncio
import json
import random
from typing import Any, Dict, Optional

import httpx
from loguru import logger
from parsel import Selector

BASE_URL = "https://www.walmart.com"

MOBILE_USER_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1",
]


DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "upgrade-insecure-requests": "1",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
}


def build_headers(user_agent: str) -> Dict[str, str]:
    headers = DEFAULT_HEADERS.copy()
    headers["user-agent"] = user_agent
    return headers


def is_blocked(status_code: int, text: str) -> bool:
    lowered = text.lower()
    missing_next_data = "__NEXT_DATA__" not in lowered
    blocked_keywords = any(keyword in lowered for keyword in ["robot or human", "blocked"])
    return status_code == 456 or blocked_keywords or missing_next_data


def extract_next_data(html: str) -> Optional[Dict[str, Any]]:
    selector = Selector(html)
    raw_json = selector.css("script#__NEXT_DATA__::text").get()
    if not raw_json:
        return None
    try:
        return json.loads(raw_json)
    except json.JSONDecodeError:
        logger.warning("Failed to decode __NEXT_DATA__ JSON")
        return None


async def fetch_with_retry(
    client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None, max_attempts: int = 3
) -> Optional[httpx.Response]:
    for attempt in range(1, max_attempts + 1):
        user_agent = MOBILE_USER_AGENTS[(attempt - 1) % len(MOBILE_USER_AGENTS)]
        headers = build_headers(user_agent)
        if attempt > 1:
            delay = random.uniform(1.5, 4.5)
            logger.info(f"Retrying {url} (attempt {attempt}/{max_attempts}) after sleeping {delay:.2f}s with UA {user_agent}")
            await asyncio.sleep(delay)
        try:
            response = await client.get(url, params=params, headers=headers)
        except httpx.HTTPError as exc:
            logger.warning(f"HTTP error during request to {url}: {exc}")
            continue
        if is_blocked(response.status_code, response.text):
            logger.warning(
                f"Potential block detected for {url} (status: {response.status_code}); response length={len(response.text)}"
            )
            continue
        return response
    logger.error(f"Giving up on {url} after {max_attempts} attempts")
    return None


def create_async_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=30.0,
        follow_redirects=True,
        http2=True,
        headers=DEFAULT_HEADERS,
    )
