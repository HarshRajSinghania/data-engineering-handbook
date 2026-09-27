"""Core pieces of a small RAG pipeline: chunking, a BM25 index, and retrieval.

No third-party packages are needed. BM25 is a keyword-ranking function that is still a
strong baseline, and it is the "sparse" half of the hybrid search used in production RAG.
"""
from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

DOCS_DIR = Path(__file__).resolve().parent.parent.parent / "docs"
SITE_URL = "https://sarangambekar1997.github.io/data-engineering-handbook"

STOPWORDS = frozenset(
    "a an and are as at be but by can do does for from has have how i if in into is it its of on or "
    "so than that the their then there these this to use used uses using what when where which who why will with you your".split()
)


@dataclass
class Chunk:
    chunk_id: int
    path: str          # docs-relative path, e.g. "01-storage/delta-lake.md"
    title: str         # the guide's H1
    heading: str       # the section (H2/H3) the chunk sits under
    text: str

    @property
    def url(self) -> str:
        return f"{SITE_URL}/{self.path.removesuffix('.md')}/"


def tokenize(text: str) -> list[str]:
    """Lowercase word tokens; keeps things like `row_number` and `mergeSchema` searchable."""
    words = re.findall(r"[a-z0-9_]+", text.lower())
    return [w for w in words if w not in STOPWORDS and len(w) > 1]


def chunk_markdown(path: Path, root: Path, max_chars: int = 1800) -> list[Chunk]:
    """Split one guide by headings, then split long sections on blank lines.

    Each chunk carries its guide title and section heading, so a chunk is understandable
    on its own and the answer can cite its source.
    """
    lines = path.read_text(encoding="utf-8").splitlines()
    title = next((ln[2:].strip() for ln in lines if ln.startswith("# ")), path.stem)
    rel = path.relative_to(root).as_posix()

    sections: list[tuple[str, list[str]]] = []
    heading, buf, in_code = title, [], False
    for line in lines:
        if line.startswith("```"):
            in_code = not in_code
        if not in_code and re.match(r"^#{2,3} ", line):
            sections.append((heading, buf))
            heading, buf = line.lstrip("# ").strip(), []
        else:
            buf.append(line)
    sections.append((heading, buf))

    chunks: list[Chunk] = []
    for heading, body in sections:
        text = "\n".join(body).strip()
        if len(text) < 80 or heading.lower() == "table of contents":
            continue
        parts, current = [], ""
        for para in re.split(r"\n\s*\n", text):
            if current and len(current) + len(para) > max_chars:
                parts.append(current)
                current = ""
            current += ("\n\n" if current else "") + para
        parts.append(current)
        chunks += [Chunk(0, rel, title, heading, p) for p in parts if p.strip()]
    return chunks


def load_chunks(docs_dir: Path = DOCS_DIR) -> list[Chunk]:
    chunks: list[Chunk] = []
    for md in sorted(docs_dir.rglob("*.md")):
        if md.name.startswith("_"):
            continue
        chunks += chunk_markdown(md, docs_dir)
    for i, c in enumerate(chunks):
        c.chunk_id = i
    return chunks


class BM25Index:
    """Okapi BM25 over chunk text, with the title and heading repeated to boost them."""

    def __init__(self, chunks: list[Chunk], k1: float = 1.5, b: float = 0.75, heading_boost: int = 3):
        self.chunks, self.k1, self.b = chunks, k1, b
        self.tf: list[Counter] = []
        for c in chunks:
            tokens = tokenize(c.text) + tokenize(f"{c.title} {c.heading}") * heading_boost
            self.tf.append(Counter(tokens))
        self.lengths = [sum(t.values()) for t in self.tf]
        self.avg_len = sum(self.lengths) / len(self.lengths)
        df: Counter = Counter()
        for t in self.tf:
            df.update(t.keys())
        n = len(chunks)
        self.idf = {term: math.log(1 + (n - d + 0.5) / (d + 0.5)) for term, d in df.items()}

    def score(self, query_terms: list[str], i: int) -> float:
        tf, length = self.tf[i], self.lengths[i]
        total = 0.0
        for term in query_terms:
            f = tf.get(term)
            if f:
                total += self.idf[term] * f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * length / self.avg_len))
        return total

    def search(self, query: str, k: int = 5) -> list[tuple[Chunk, float]]:
        terms = tokenize(query)
        scored = [(self.score(terms, i), i) for i in range(len(self.chunks))]
        scored = [s for s in scored if s[0] > 0]
        scored.sort(key=lambda s: (-s[0], s[1]))
        return [(self.chunks[i], score) for score, i in scored[:k]]


def build_index(docs_dir: Path = DOCS_DIR) -> BM25Index:
    return BM25Index(load_chunks(docs_dir))


def unique_sources(results: list[tuple[Chunk, float]]) -> list[str]:
    """Guide paths in rank order, one entry per guide."""
    seen: list[str] = []
    for chunk, _ in results:
        if chunk.path not in seen:
            seen.append(chunk.path)
    return seen
