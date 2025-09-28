import asyncio
from src.agents import crawl_with_playwright

async def main():
    url = "https://plo.vn/tim-kiem?q=vinamilk"
    html = await crawl_with_playwright(url)
    print("LEN:", len(html))
    open("debug_plo_search.html","w",encoding="utf-8").write(html)

asyncio.run(main())
