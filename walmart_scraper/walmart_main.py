import argparse
import asyncio
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from walmart_scraper.utils import create_async_client
from walmart_scraper.walmart_product import scrape_products
from walmart_scraper.walmart_search import scrape_search_results

OUTPUT_DIR = Path(__file__).parent / "output"
SEARCH_JSON = OUTPUT_DIR / "walmart_search.json"
PRODUCTS_JSON = OUTPUT_DIR / "walmart_products.json"
CSV_OUTPUT = OUTPUT_DIR / "walmart_products.csv"


def _ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _serialize_json(path: Path, data: Any):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    logger.success(f"Saved JSON: {path}")


def _extract_price(product: Dict[str, Any], fallback: Optional[Dict[str, Any]] = None) -> Optional[str]:
    price_info = (product or {}).get("priceInfo") or {}
    current_price = price_info.get("currentPrice")
    if isinstance(current_price, dict):
        return current_price.get("priceString") or current_price.get("price")
    return (fallback or {}).get("price")


def _extract_image(product: Dict[str, Any], fallback: Optional[Dict[str, Any]] = None) -> Optional[str]:
    image_info = (product or {}).get("imageInfo") or {}
    if isinstance(image_info, dict):
        return image_info.get("thumbnailUrl") or image_info.get("allImages", [{}])[0].get("url")
    return (fallback or {}).get("image")


def _extract_reviews_count(product: Dict[str, Any], fallback: Optional[Dict[str, Any]] = None) -> Optional[int]:
    reviews = (product or {}).get("reviews")
    if isinstance(reviews, dict):
        return reviews.get("reviewsCount") or reviews.get("totalReviewCount")
    return (fallback or {}).get("reviews")


def build_csv_rows(products: List[Dict[str, Any]], search_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    product_map = {p.get("id"): p for p in products if p.get("id")}
    rows: List[Dict[str, Any]] = []

    for item in search_items:
        product = product_map.get(item.get("id"))
        rows.append(
            {
                "id": item.get("id") or (product or {}).get("id"),
                "name": (product or {}).get("name") or item.get("name"),
                "price": _extract_price(product, item),
                "rating": (product or {}).get("averageRating") or item.get("rating"),
                "reviews_count": _extract_reviews_count(product, item),
                "availability": (product or {}).get("availabilityStatus") or item.get("availability"),
                "image": _extract_image(product, item),
                "product_url": (product or {}).get("productUrl") or item.get("url"),
            }
        )

    return rows


def save_csv(rows: List[Dict[str, Any]], path: Path):
    fieldnames = ["id", "name", "price", "rating", "reviews_count", "availability", "image", "product_url"]
    with path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.success(f"Saved CSV: {path}")


async def run(query: str, pages: int, concurrency: int):
    _ensure_output_dir()
    async with create_async_client() as client:
        search_items, product_urls = await scrape_search_results(client, query, pages)
        _serialize_json(SEARCH_JSON, search_items)

        products = await scrape_products(client, product_urls, concurrency=concurrency)
        _serialize_json(PRODUCTS_JSON, products)

        csv_rows = build_csv_rows(products, search_items)
        save_csv(csv_rows, CSV_OUTPUT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Async Walmart.com scraper")
    parser.add_argument("--query", required=True, help="Search query to scrape")
    parser.add_argument("--pages", type=int, default=1, help="Number of search result pages to scrape (max 25)")
    parser.add_argument(
        "--concurrency", type=int, default=5, help="Number of concurrent product fetches"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args.query, args.pages, args.concurrency))
