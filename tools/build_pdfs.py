"""Build downloadable PDFs from the built site: one per guide, and one for the whole handbook.

    pip install -r requirements-pdf.txt && playwright install chromium
    mkdocs build && python tools/build_pdfs.py            # every guide, then the combined PDF
    python tools/build_pdfs.py --sample 3                 # only the first three guides, to check the setup

Each page is rendered with headless Chromium using the site's print stylesheet, so a PDF looks like the
printed page. Links to other pages are rewritten to the public site, because a PDF cannot follow a
relative link. Output: site/pdf/<guide>.pdf and site/sarangs-data-engineering-handbook.pdf.
Exits with code 1 if any page fails to render.
"""
from __future__ import annotations

import argparse
import functools
import http.server
import re
import sys
import threading
from pathlib import Path

import yaml
from playwright.sync_api import sync_playwright
from pypdf import PdfReader, PdfWriter

ROOT = Path(__file__).resolve().parent.parent
COMBINED = "sarangs-data-engineering-handbook.pdf"


def load_config() -> dict:
    """mkdocs.yml, ignoring the !!python/name tags that only MkDocs understands."""
    class Loader(yaml.SafeLoader):
        pass
    Loader.add_multi_constructor("tag:yaml.org,2002:python/", lambda loader, suffix, node: None)
    return yaml.load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"), Loader=Loader)


def nav_pages(nav: list, section: str = "") -> list[tuple[str, str, str]]:
    """Flatten the nav into (section, title, page.md) in reading order."""
    pages = []
    for item in nav:
        for title, value in item.items() if isinstance(item, dict) else [(None, item)]:
            if isinstance(value, list):
                pages += nav_pages(value, title or section)
            elif isinstance(value, str) and value.endswith(".md"):
                pages.append((section, title or value, value))
    return pages


def url_path(md: str) -> str:
    """README.md -> "", a/b.md -> "a/b/", a/index.md -> "a/"."""
    stem = re.sub(r"(^|/)(README|index)\.md$", "", md) if re.search(r"(^|/)(README|index)\.md$", md) else md[:-3]
    return stem + "/" if stem else ""


class Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args) -> None:
        pass


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--site", default=str(ROOT / "site"))
    parser.add_argument("--sample", type=int, default=0, help="render only the first N guides")
    args = parser.parse_args()

    site = Path(args.site)
    config = load_config()
    public = config["site_url"]
    guides = [(s, t, md) for s, t, md in nav_pages(config["nav"])
              if re.match(r"\d\d-", md) and (site / url_path(md) / "index.html").exists()]
    if args.sample:
        guides = guides[: args.sample]
    out = site / "pdf"
    out.mkdir(exist_ok=True)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), functools.partial(Quiet, directory=str(site)))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    origin = f"http://127.0.0.1:{server.server_address[1]}/"
    footer = ('<div style="font-size:8px;width:100%;padding:0 12mm;display:flex;justify-content:space-between;color:#666">'
              '<span>Sarang\'s Data Engineering Handbook</span><span><span class="pageNumber"></span> / <span class="totalPages"></span></span></div>')

    failures, built = [], []
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        for section, title, md in guides:
            slug = Path(md).stem
            try:
                page.goto(origin + url_path(md), wait_until="networkidle")
                page.wait_for_function("() => [...document.querySelectorAll('.mermaid')].every(e => e.getBoundingClientRect().height > 20)", timeout=15000)
                page.evaluate("""([origin, public]) => document.querySelectorAll('.md-content a[href]').forEach(a => {
                    if (a.href.startsWith(origin)) a.href = public + a.href.slice(origin.length); })""", [origin, public])
                page.pdf(path=str(out / f"{slug}.pdf"), format="A4", print_background=True, display_header_footer=True,
                         header_template="<span></span>", footer_template=footer,
                         margin={"top": "15mm", "bottom": "18mm", "left": "12mm", "right": "12mm"})
                built.append((section, title, out / f"{slug}.pdf"))
                print(f"{slug}.pdf")
            except Exception as e:  # noqa: BLE001
                failures.append(f"{md}: {e}")
        browser.close()

    if built and not args.sample:
        writer = PdfWriter()
        outline_sections: dict[str, object] = {}
        for section, title, path in built:
            start = len(writer.pages)
            reader = PdfReader(path)
            for pdf_page in reader.pages:
                writer.add_page(pdf_page)
            parent = outline_sections.get(section)
            if parent is None:
                parent = outline_sections[section] = writer.add_outline_item(section, start)
            writer.add_outline_item(title, start, parent=parent)
        writer.add_metadata({"/Title": "Sarang's Data Engineering Handbook", "/Author": "Sarang Ambekar"})
        with open(site / COMBINED, "wb") as f:
            writer.write(f)
        print(f"{COMBINED}: {len(writer.pages)} pages, {(site / COMBINED).stat().st_size / 1e6:.1f} MB")

    for failure in failures:
        print("FAILED " + failure)
    print(f"{len(built)} PDF(s) built, {len(failures)} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
