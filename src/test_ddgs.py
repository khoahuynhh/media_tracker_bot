from ddgs import DDGS

sites = [
    "site:baodautu.vn bài viết liên quan Hipp",
    # "site:vnexpress.net Vinamilk tag",
    # "site:tuoitre.vn Vinamilk tag",
]

for query in sites:
    print(f"\n===== Query: {query} =====")
    results = DDGS().text(
        query,
        region="us-en",
        safesearch="off",
        timelimit="y",
        page=1,
        backend="mullvad_google",
    )
    print("results id:", id(results))  # để check object khác nhau

    for r in results:
        title = r.get("title")
        link = r.get("href")
        print(f"- {title}\n  {link}\n")
