"""MkDocs hook: give every page its own meta description.

Without this, every page on the site shares the site-wide description, so search results and link
previews look identical for 60 different guides. A guide already has a one-line summary right under
its title (a Markdown blockquote), so that line becomes the page's description unless the page
sets `description:` in its front matter.
"""
import re


def summary(markdown: str) -> str | None:
    """The blockquote right under the first H1, as plain text, or None."""
    lines = markdown.split("\n")
    h1 = next((i for i, line in enumerate(lines) if re.match(r"# \S", line)), None)
    if h1 is None:
        return None
    quote = []
    for line in lines[h1 + 1:]:
        if not line.strip() and not quote:
            continue                                   # blank lines between the title and the quote
        if not line.startswith(">"):
            break
        quote.append(line.lstrip("> ").strip())
    text = " ".join(part for part in quote if part)
    text = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", text)      # [label](url) -> label
    text = re.sub(r"[*_`]", "", text)
    return text or None


def on_page_markdown(markdown, page, config, files):
    if not page.meta.get("description") and not page.is_homepage:
        found = summary(markdown)
        if found:
            page.meta["description"] = found
    return markdown
