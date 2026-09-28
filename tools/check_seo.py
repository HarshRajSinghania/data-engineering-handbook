"""Check the search and sharing metadata of the built site (run after `mkdocs build`).

Every page must have its own description, matching Open Graph and Twitter descriptions, a canonical
URL, and (except the home page and the 404 page) valid structured data. The sitemap must list every page.

Usage:
    python tools/check_seo.py [site_dir]
"""
from __future__ import annotations

import json
import re
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path


def meta(html: str, attr: str, name: str) -> str | None:
    m = re.search(rf'<meta {attr}="{re.escape(name)}" content="([^"]*)"', html)
    return m.group(1) if m else None


def main() -> int:
    site = Path(sys.argv[1] if len(sys.argv) > 1 else "site")
    pages = sorted(p for p in site.rglob("index.html"))
    errors: list[str] = []
    descriptions: dict[str, list[str]] = defaultdict(list)

    for page in pages:
        html = page.read_text(encoding="utf-8")
        name = str(page.relative_to(site))
        description = meta(html, "name", "description")
        if not description or len(description) < 30:
            errors.append(f"{name}: missing or very short description")
            continue
        descriptions[description].append(name)
        if len(description) > 300:
            errors.append(f"{name}: description is {len(description)} characters")
        if meta(html, "property", "og:description") != description:
            errors.append(f"{name}: og:description differs from the description")
        if meta(html, "name", "twitter:description") != description:
            errors.append(f"{name}: twitter:description differs from the description")
        if not re.search(r'<link rel="canonical" href="https://[^"]+">', html):
            errors.append(f"{name}: no canonical URL")
        if name == "index.html":
            continue
        blocks = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.S)
        if len(blocks) != 1:
            errors.append(f"{name}: expected one JSON-LD block, found {len(blocks)}")
            continue
        try:
            graph = json.loads(blocks[0])["@graph"]
        except (ValueError, KeyError) as e:
            errors.append(f"{name}: invalid JSON-LD ({e})")
            continue
        types = {item.get("@type") for item in graph}
        if types != {"TechArticle", "BreadcrumbList"}:
            errors.append(f"{name}: JSON-LD types are {sorted(types)}")

    for description, names in descriptions.items():
        if len(names) > 1:
            errors.append(f"{len(names)} pages share the description {description[:60]!r}: {', '.join(names[:3])}")

    sitemap = site / "sitemap.xml"
    if not sitemap.exists():
        errors.append("sitemap.xml is missing")
    else:
        urls = [e.text for e in ET.parse(sitemap).getroot().iter() if e.tag.endswith("}loc")]
        if len(urls) != len(pages):
            errors.append(f"the sitemap lists {len(urls)} URLs but the site has {len(pages)} pages")

    print(f"checked {len(pages)} pages: {len(errors)} problem(s)")
    for e in errors:
        print("  " + e)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
