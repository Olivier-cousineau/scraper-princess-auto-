import asyncio
from typing import Any, Dict, List, Optional

from loguru import logger

from walmart_scraper.utils import BASE_URL, extract_next_data, fetch_with_retry


def _parse_product(next_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    initial_data = (
        next_data.get("props", {})
        .get("pageProps", {})
        .get("initialData", {})
        .get("data", {})
    )
    product = initial_data.get("product")
    if not product:
        logger.warning("Product data missing in __NEXT_DATA__")
        return None

    normalized: Dict[str, Any] = {
        "id": product.get("id") or product.get("usItemId"),
        "name": product.get("name"),
        "brand": product.get("brand"),
        "manufacturerName": product.get("manufacturerName"),
        "priceInfo": product.get("priceInfo"),
        "imageInfo": product.get("imageInfo"),
        "availabilityStatus": product.get("availabilityStatus"),
        "averageRating": product.get("averageRating"),
        "orderLimit": product.get("orderLimit"),
        "shortDescription": product.get("shortDescription"),
    }

    normalized["reviews"] = initial_data.get("reviews")
    if product.get("canonicalUrl"):
        normalized["productUrl"] = product["canonicalUrl"] if product["canonicalUrl"].startswith("http") else f"{BASE_URL}{product['canonicalUrl']}"
    return normalized


async def fetch_product_details(client, url: str) -> Optional[Dict[str, Any]]:
    response = await fetch_with_retry(client, url)
    if response is None:
        logger.error(f"Failed to fetch product page: {url}")
        return None

    next_data = extract_next_data(response.text)
    if not next_data:
        logger.error(f"__NEXT_DATA__ missing from product page: {url}")
        return None

    product = _parse_product(next_data)
    if product:
        product.setdefault("productUrl", url)
    return product


async def scrape_products(client, urls: List[str], concurrency: int = 5) -> List[Dict[str, Any]]:
    semaphore = asyncio.Semaphore(concurrency)
    results: List[Dict[str, Any]] = []

    async def worker(product_url: str):
        async with semaphore:
            product = await fetch_product_details(client, product_url)
            if product:
                results.append(product)
                logger.info(f"Fetched product: {product_url}")
            else:
                logger.error(f"Unable to parse product: {product_url}")

    await asyncio.gather(*(worker(url) for url in urls))
    return results
