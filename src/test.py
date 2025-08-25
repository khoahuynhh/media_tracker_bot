#!/usr/bin/env python3
"""
Test Google Custom Search (CSE) for a specific domain + keywords.

Usage examples:
  # Basic (uses env var GOOGLE_API_KEY)
  python test_google_cse.py --domain laodong.vn Vinamilk "tin tức"

  # Specify a custom CX (also reads CSE_CX from env if not provided)
  python test_google_cse.py --domain laodong.vn --cx 16c863775f52f42cd Vinamilk "tin tức"

  # Show raw JSON
  python test_google_cse.py --domain laodong.vn Vinamilk "tin tức" --raw

Environment variables:
  - GOOGLE_API_KEY   (required unless you pass --api-key)
  - CSE_CX           (optional; overrides default CX if set)

Install deps:
  pip install requests python-dotenv
"""
from __future__ import annotations
import os
import sys
import json
import argparse
from typing import List, Dict, Any, Optional

try:
    # Optional: load .env if present
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

import requests


DEFAULT_CX = "16c863775f52f42cd"  # you can override with --cx or env CSE_CX


def google_cse_search(
    domain: str,
    keywords: List[str],
    api_key: Optional[str] = None,
    cx: Optional[str] = None,
    num: int = 10,
    add_tag_term: bool = True,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Call Google CSE: site:<domain> <keywords> [tag]
    Returns the parsed JSON dict (even on errors) so you can inspect.
    """
    api_key = api_key or os.getenv("GOOGLE_API_KEY_SEARCH")
    if not api_key:
        raise RuntimeError("Missing GOOGLE_API_KEY. Set env var or pass --api-key.")

    cx = cx or os.getenv("CSE_CX") or DEFAULT_CX

    query_terms = " ".join([t for t in keywords if t]).strip()

    query = f"site:{domain} {query_terms}".strip()

    url = "https://www.googleapis.com/customsearch/v1"
    params: Dict[str, Any] = {"key": api_key, "cx": cx, "q": query, "num": num}

    # Optional tuning (locale)
    # params.update({"hl": "vi"})
    if extra_params:
        params.update(extra_params)

    resp = requests.get(url, params=params, timeout=30)
    data = {}
    try:
        data = resp.json()
    except Exception:
        data = {"_non_json_response": resp.text, "_status_code": resp.status_code}

    # Attach some debug meta
    data["_meta"] = {
        "status_code": resp.status_code,
        "url": resp.url,
        "query": query,
        "params": params,
    }
    return data


def pretty_print_items(items: List[Dict[str, Any]]) -> None:
    for i, it in enumerate(items, 1):
        title = it.get("title", "")
        link = it.get("link", "")
        display = it.get("displayLink", "")
        snippet = it.get("snippet", "")
        print(f"\n[{i}] {title}\n  {link}\n  ({display})\n  {snippet}")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Test Google CSE for domain + keywords"
    )
    parser.add_argument(
        "--domain", required=True, help="Target domain, e.g., laodong.vn"
    )
    parser.add_argument(
        "--api-key", help="Google API key (falls back to env GOOGLE_API_KEY)"
    )
    parser.add_argument(
        "--cx", help="Custom Search Engine ID (falls back to env CSE_CX or default)"
    )
    parser.add_argument(
        "--num", type=int, default=10, help="Number of results to request (1-10)"
    )
    parser.add_argument(
        "--no-tag",
        action="store_true",
        help="Do NOT append the word 'tag' to the query",
    )
    parser.add_argument("--raw", action="store_true", help="Print full raw JSON")
    parser.add_argument(
        "--json", action="store_true", help="Print compact JSON of items only"
    )
    parser.add_argument("keywords", nargs="*", help="Keywords to search")
    args = parser.parse_args(argv)

    try:
        data = google_cse_search(
            domain=args.domain,
            keywords=args.keywords,
            api_key=args.api_key,
            cx=args.cx,
            num=args.num,
            add_tag_term=not args.no_tag,
        )
    except Exception as e:
        print("❌ Error:", e)
        return 1

    # Quick status/debug
    meta = data.get("_meta", {})
    print("Status:", meta.get("status_code"), "| URL:", meta.get("url"))
    print("Query used:", meta.get("query"))

    if args.raw:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0

    if "error" in data:
        print("❌ API Error:", json.dumps(data["error"], ensure_ascii=False, indent=2))
        return 2

    items = data.get("items", [])
    info = data.get("searchInformation", {})
    total = info.get("totalResults")
    print(f"TotalResults(reported): {total} | Returned items: {len(items)}")

    if args.json:
        print(json.dumps(items, ensure_ascii=False, indent=2))
        return 0

    if not items:
        print("⚠️ No items. Full payload (truncated keys):", list(data.keys()))
        return 0

    pretty_print_items(items)
    # Also print first link explicitly (like your original function)
    print("\nFirst link:", items[0].get("link"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
