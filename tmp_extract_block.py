def _extract_article_links_from_soup(
            soup: BeautifulSoup, current_page_url: str, ind_norm: set[str]
        ) -> list[str]:
            links = []
            start_d = start_date.date()
            end_d = end_date.date()
            for a in soup.select("a"):
                href = a.get("href", "")
                if not href:
                    continue
                if href.startswith(
                    ("#", "javascript:", "vbscript:", "mailto:", "tel:")
                ):
                    continue

                full_url = urljoin(current_page_url, href)
                p = urlparse(full_url)

                # ch? nh?n http(s) h?p l? + có netloc
                if p.scheme not in ("http", "https") or not p.netloc:
                    continue

                # lo?i trang ch?/du?ng d?n r?ng, trang tag/video
                if p.path in ("", "/") or p.path.startswith(("/tags/", "/video/")):
                    continue

                # kh?p theo keywords/industry
                # has_kw = any(kw.lower() in (p.path or "").lower() for kw in keywords)
                # has_ind = (
                #     any(v in (p.path or "").lower() for v in ind_norm)
                #     if ind_norm
                #     else False
                # )
                # if not (has_kw or has_ind):
                #     continue

                kw_norm = [k.lower() for k in keywords or []]
                ind_norm = [v.lower() for v in (ind_norm or [])]
                relevant = _context_has_kw(a, p, kw_norm, ind_norm)

                # l?c s?m theo ngày n?u u?c lu?ng du?c
                art_date = date_from_url_path(p.path) or date_from_anchor_context(a)
                if art_date is not None and not (start_d <= art_date <= end_d):
                    continue

                if relevant:
                    links.append(full_url)
            return links
