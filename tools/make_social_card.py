"""Render the 1200x630 image that link previews show (docs/assets/social-card.png).

    pip install -r requirements-pdf.txt && playwright install chromium
    python tools/make_social_card.py                        # uses the defaults below
    python tools/make_social_card.py --kicker "" --title "Data Engineering Handbook"

It states no counts of guides or labs, so it does not go out of date when content is added.
"""
from __future__ import annotations

import argparse
import html
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent.parent / "docs" / "assets" / "social-card.png"

PAGE = """<!doctype html><meta charset="utf-8"><style>
  * {{ box-sizing: border-box; margin: 0; }}
  body {{ width: 1200px; height: 630px; padding: 80px; position: relative; overflow: hidden;
         font-family: "DejaVu Sans", "Helvetica Neue", Arial, sans-serif; color: #fff;
         background: linear-gradient(180deg, #1e1e4b 0%, #4f46e5 100%); }}
  .kicker {{ font-size: 52px; font-weight: 700; color: #c7d2fe; line-height: 1.1; }}
  .title {{ font-size: 96px; font-weight: 700; line-height: 1.08; margin-top: 8px; max-width: 1000px; }}
  .tagline {{ position: absolute; left: 80px; top: 404px; font-size: 32px; color: #e0e7ff; }}
  .chain {{ position: absolute; left: 68px; top: 486px; width: 530px; height: 46px; }}
  .chain i {{ position: absolute; top: 0; width: 46px; height: 46px; border-radius: 9px; background: #fff; }}
  .chain i:nth-of-type(even) {{ background: #c7d2fe; }}
  .chain b {{ position: absolute; top: 21px; height: 4px; background: #c7d2fe; }}
  .foot {{ position: absolute; left: 80px; bottom: 44px; font-size: 26px; color: #c7d2fe; }}
</style>
<body>
  <div class="kicker">{kicker}</div>
  <div class="title">{title}</div>
  <div class="tagline">{tagline}</div>
  <div class="chain">
    <b style="left:46px;width:114px"></b><b style="left:206px;width:114px"></b><b style="left:366px;width:114px"></b>
    <i style="left:0"></i><i style="left:160px"></i><i style="left:320px"></i><i style="left:480px"></i>
  </div>
  <div class="foot">{footer}</div>
</body>"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kicker", default="Sarang's")
    parser.add_argument("--title", default="Data Engineering Handbook")
    parser.add_argument("--tagline", default="From first query to production pipelines, and the LLMs on top.")
    parser.add_argument("--footer", default="Guides · hands-on labs · interview prep · open source")
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    page_html = PAGE.format(kicker=html.escape(args.kicker), title=html.escape(args.title),
                            tagline=html.escape(args.tagline), footer=html.escape(args.footer))
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1200, "height": 630})
        page.set_content(page_html)
        page.screenshot(path=str(args.out))
        browser.close()
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
