import asyncio
from playwright.async_api import async_playwright
import random


async def test_crawl_laodong():
    url = "https://laodong.vn/kinh-doanh/lan-dau-tien-vinamilk-duoc-xep-hang-suc-manh-thuong-hieu-xuat-sac-vuot-troi-toan-cau-1559261.ldo"

    async with async_playwright() as p:
        # Launch browser với các args để stealth
        browser = await p.chromium.launch(
            headless=False,  # Hiện browser để debug
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor",
                "--disable-software-rasterizer",
            ],
        )

        # Tạo context mới với viewport ngẫu nhiên
        context = await browser.new_context(
            viewport={
                "width": 1366 + random.randint(-100, 100),
                "height": 768 + random.randint(-100, 100),
            },
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        # Thêm init script để ẩn webdriver
        await context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined,
            });
            window.chrome = {
                runtime: {},
            };
            Object.defineProperty(navigator, 'languages', {
                get: () => ['vi-VN', 'vi', 'en-US', 'en'],
            });
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32',
            });
        """
        )

        page = await context.new_page()

        # Set extra headers
        await page.set_extra_http_headers(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            }
        )

        # Bật logging để debug
        def log_request(request):
            print(f"→ {request.method} {request.url}")

        page.on("request", log_request)

        def log_response(response):
            if response.status >= 400:
                print(f"← {response.status} {response.url}")

        page.on("response", log_response)

        def log_console(msg):
            print(f"CONSOLE: {msg.text}")

        page.on("console", log_console)

        try:
            print(f"🚀 Navigating to: {url}")

            # Dùng domcontentloaded thay vì networkidle
            response = await page.goto(
                url,
                timeout=45000,  # 45 seconds timeout
                wait_until="domcontentloaded",  # QUAN TRỌNG: đổi thành domcontentloaded
            )

            if response:
                print(f"✅ Page loaded. Status: {response.status}")
                print(f"📄 Final URL: {response.url}")
            else:
                print("⚠️ No response object returned")

            # Chờ thêm 3-5 giây để JavaScript load
            print("⏳ Waiting for additional content...")
            await page.wait_for_timeout(5000)

            # Kiểm tra xem có Cloudflare challenge không
            cf_challenge = await page.query_selector("div#challenge-form")
            if cf_challenge:
                print("❌ Cloudflare challenge detected!")
                # Chụp ảnh màn hình để debug
                await page.screenshot(path="cloudflare_challenge.png")
                return None

            # Lấy content
            content = await page.content()
            print(f"📊 Content length: {len(content)} characters")

            # Kiểm tra xem content có hợp lệ không
            if len(content) < 5000:
                print("⚠️ Content seems too short, might be blocked")
                # Chụp ảnh màn hình
                await page.screenshot(path="page_screenshot.png")

                # Lấy text để xem có thông báo block không
                body_text = await page.text_content("body")
                if any(
                    keyword in body_text.lower()
                    for keyword in ["access denied", "blocked", "bot", "captcha"]
                ):
                    print("❌ Page blocking detected!")
                    return None

            # Lưu HTML để inspect
            with open("debug_page.html", "w", encoding="utf-8") as f:
                f.write(content)
            print("💾 HTML saved to debug_page.html")

            await browser.close()
            return content

        except TimeoutError:
            print("⏰ Timeout exceeded! Website might be blocking the request")
            # Chụp ảnh màn hình khi timeout
            await page.screenshot(path="timeout_screenshot.png")
            await browser.close()
            return None

        except Exception as e:
            print(f"❌ Error: {e}")
            # Chụp ảnh màn hình khi có lỗi
            await page.screenshot(path="error_screenshot.png")
            await browser.close()
            return None


# Chạy test
async def main():
    print("🧪 Testing laodong.vn crawl...")
    html = await test_crawl_laodong()

    if html:
        print("🎉 Test successful!")
        # Tìm title để verify content
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        title = soup.find("title")
        if title:
            print(f"📝 Page title: {title.text}")
        else:
            print("ℹ️ No title found")
    else:
        print("💥 Test failed!")


if __name__ == "__main__":
    asyncio.run(main())
