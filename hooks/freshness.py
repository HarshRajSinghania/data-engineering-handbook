"""MkDocs hook: show when each guide was last reviewed, right under its summary line.

Reads `verified` (date) and the optional `lab_tested` (tool and version) from the
page front matter. A guide not reviewed within STALE_AFTER_DAYS gets a warning
note. Keep STALE_AFTER_DAYS in step with tools/check_freshness.py.
"""
import datetime
import html
import re

STALE_AFTER_DAYS = 180


def on_page_markdown(markdown, page, config, files):
    verified = page.meta.get("verified")
    if not isinstance(verified, datetime.date):
        return markdown

    age = (datetime.date.today() - verified).days
    stale = age > STALE_AFTER_DAYS
    parts = [f'Last reviewed <time datetime="{verified.isoformat()}">{verified:%-d %b %Y}</time>']
    if page.meta.get("lab_tested"):
        parts.append(f'Lab-tested with {html.escape(str(page.meta["lab_tested"]))}')
    css = "page-freshness page-freshness--stale" if stale else "page-freshness"
    line = f'<p class="{css}">{" · ".join(parts)}</p>'
    if stale:
        line += (
            '\n\n!!! warning "Review overdue"\n'
            "    This guide was last reviewed more than six months ago. Check commands and "
            "versions against the vendor documentation before relying on them."
        )

    # Insert after the H1 and the blockquote summary that follows it
    lines = markdown.split("\n")
    h1 = next((i for i, text in enumerate(lines) if re.match(r"# \S", text)), None)
    if h1 is None:
        return markdown
    end = h1 + 1
    while end < len(lines) and lines[end].startswith(">"):
        end += 1
    lines[end:end] = ["", line, ""]
    return "\n".join(lines)
