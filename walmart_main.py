import asyncio

from walmart_scraper.walmart_main import parse_args, run

if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args.query, args.pages, args.concurrency))
