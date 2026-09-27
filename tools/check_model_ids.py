"""Fail if docs/ or labs/ mention a model ID listed in tools/deprecated_models.txt."""
import re
import sys
from pathlib import Path

patterns = [
    re.compile(line)
    for line in Path("tools/deprecated_models.txt").read_text().splitlines()
    if line.strip() and not line.startswith("#")
]
hits = 0
for root in ("docs", "labs", "README.md"):
    p = Path(root)
    files = [p] if p.is_file() else [f for f in p.rglob("*") if f.is_file() and f.suffix in (".md", ".py", ".json", ".yml") and "target" not in f.parts]
    for f in files:
        for n, line in enumerate(f.read_text(encoding="utf-8", errors="ignore").splitlines(), 1):
            for pat in patterns:
                if pat.search(line):
                    print(f"{f}:{n}: deprecated model ID /{pat.pattern}/ -> {line.strip()[:100]}")
                    hits += 1
print(f"{hits} deprecated model reference(s)")
sys.exit(1 if hits else 0)
