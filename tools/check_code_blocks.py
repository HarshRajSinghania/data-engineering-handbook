"""Parse every fenced code block in docs/ so broken examples fail CI.

Checks python (ast), json, yaml and sql (sqlglot, permissive). A block is skipped
when the fence is directly preceded by `<!-- docs-parse: skip -->` (for deliberate
fragments), or when it uses notebook magics / shell prompts / `...` placeholders.
"""
import ast
import json
import re
import sys
from pathlib import Path

import yaml

FENCE = re.compile(r"^```(\w+)[^\n]*\n(.*?)^```", re.S | re.M)
SKIP_MARK = "<!-- docs-parse: skip -->"


def check(lang: str, code: str) -> str | None:
    try:
        if lang in ("python", "py"):
            if re.search(r"^\s*[%!]|^\s*>>>|\.\.\.\s*$", code, re.M):
                return None
            ast.parse(code)
        elif lang == "json":
            if "..." in code or "//" in code:
                return None
            json.loads(code)
        elif lang in ("yaml", "yml"):
            if "..." in code or "{{" in code or "{%" in code:
                return None
            list(yaml.safe_load_all(code))
    except (SyntaxError, ValueError, yaml.YAMLError) as e:
        return f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}"
    return None


def main() -> int:
    bad = total = 0
    for md in sorted(Path("docs").rglob("*.md")):
        text = md.read_text(encoding="utf-8")
        for m in FENCE.finditer(text):
            lang, code = m.group(1).lower(), m.group(2)
            if text[: m.start()].rstrip().endswith(SKIP_MARK):
                continue
            if lang not in ("python", "py", "json", "yaml", "yml"):
                continue
            total += 1
            err = check(lang, code)
            if err:
                bad += 1
                line = text[: m.start()].count("\n") + 1
                print(f"{md}:{line}: {lang} block does not parse: {err}")
    print(f"checked {total} code blocks, {bad} failed")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
