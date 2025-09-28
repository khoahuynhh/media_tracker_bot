from bs4 import BeautifulSoup
from urllib.parse import urljoin

base = "https://plo.vn/tim-kiem?q=vinamilk"
html = open("debug_plo_search.html","r",encoding="utf-8").read()
soup = BeautifulSoup(html, "html.parser")
links = []
for card in soup.select("article.story"):
    a = card.select_one("figure.story__thumb a.cms-link") or card.select_one("h2.story__heading a")
    if not a or not a.get("href"): continue
    href = a['href']
    full = urljoin(base, href)
    txt = (card.get_text(' ', strip=True) or '').lower()
    ttl = (a.get('title') or '').lower()
    if 'vinamilk' in txt or 'vinamilk' in ttl:
        links.append(full)

print('FOUND:', len(links))
for u in links[:10]:
    print(u)
