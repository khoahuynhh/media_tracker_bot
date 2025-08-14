import asyncio
from ddgs import DDGS
from crawl4ai import AsyncWebCrawler


# 1. Lấy link đầu từ query (ưu tiên DuckDuckGo hoặc SearxNG)
def get_first_search_link(query: str) -> str | None:
    with DDGS(timeout=60) as ddg:
        results = list(ddg.text(query, max_results=1, backend="lite"))
        print(f"🔍 DuckDuckGo results: {results}")
        if results:
            return results[0]["href"]
    return None


# 2. Hàm chính chạy end-to-end
async def auto_crawl_from_query(query: str, max_links: int = 5):
    hub_url = get_first_search_link(query)
    if not hub_url:
        print("❌ Không tìm thấy link nào từ query")
        return

    print(f"🔍 Found hub URL: {hub_url}")

    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=hub_url, max_links=max_links)
        return result.markdown if result else "❌ Không crawl được dữ liệu"


# 3. Thử nghiệm
if __name__ == "__main__":
    query = "site:laodong.vn Vinamilk tin tức"
    asyncio.run(auto_crawl_from_query(query, max_links=10))
