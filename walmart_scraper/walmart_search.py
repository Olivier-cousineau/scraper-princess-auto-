from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from walmart_scraper.utils import BASE_URL, extract_next_data, fetch_with_retry


def _extract_price(info: Dict[str, Any]) -> Optional[str]:
    price_info = info.get("priceInfo") or {}
    current_price = price_info.get("currentPrice") or {}
    if isinstance(current_price, dict):
        return current_price.get("priceString") or current_price.get("price")
    return None


def _build_product_url(item: Dict[str, Any]) -> Optional[str]:
    for key in ["productPageUrl", "productUrl", "canonicalUrl"]:
        url = item.get(key)
        if url:
            return url if url.startswith("http") else f"{BASE_URL}{url}"
    product_id = item.get("usItemId") or item.get("productId")
    if product_id:
        return f"{BASE_URL}/ip/{product_id}"
    return None


def _normalize_search_item(item: Dict[str, Any]) -> Dict[str, Any]:
    normalized = {
        "id": item.get("usItemId") or item.get("productId"),
        "name": item.get("title") or item.get("name"),
        "price": _extract_price(item) or item.get("price"),
        "rating": item.get("averageRating") or item.get("rating"),
        "reviews": item.get("numberOfReviews") or item.get("reviewsCount"),
        "availability": item.get("availabilityStatus"),
        "image": item.get("imageUrl") or (item.get("imageInfo") or {}).get("thumbnailUrl"),
    }
    normalized["url"] = _build_product_url(item)
    return normalized


def parse_search_items(next_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        stacks = (
            next_data
            .get("props", {})
            .get("pageProps", {})
            .get("initialData", {})
            .get("searchResult", {})
            .get("itemStacks", [])
        )
        if not stacks:
            return []
        items = stacks[0].get("items", [])
    except (AttributeError, IndexError):
        logger.warning("Unexpected searchResult structure in __NEXT_DATA__")
        return []

    normalized_items: List[Dict[str, Any]] = []
    for item in items:
        normalized_items.append(_normalize_search_item(item))
    return normalized_items


async def fetch_search_page(client, query: str, page: int) -> Tuple[List[Dict[str, Any]], List[str]]:
    search_url = f"{BASE_URL}/search"
    params = {"q": query, "page": page}
    response = await fetch_with_retry(client, search_url, params=params)
    if response is None:
        logger.error(f"Failed to retrieve search page {page} for query '{query}'")
        return [], []

    next_data = extract_next_data(response.text)
    if not next_data:
        logger.error(f"Missing __NEXT_DATA__ on search page {page}")
        return [], []

    items = parse_search_items(next_data)
    product_urls = [item.get("url") for item in items if item.get("url")]
    logger.info(f"Search page {page}: extracted {len(items)} items")
    return items, product_urls


async def scrape_search_results(client, query: str, pages: int) -> Tuple[List[Dict[str, Any]], List[str]]:
    all_items: List[Dict[str, Any]] = []
    all_urls: List[str] = []
    total_pages = min(pages, 25)
    logger.info(f"Starting search for '{query}' across {total_pages} page(s)")

    for page in range(1, total_pages + 1):
        items, urls = await fetch_search_page(client, query, page)
        all_items.extend(items)
        all_urls.extend(urls)
    return all_items, list(dict.fromkeys(all_urls))
