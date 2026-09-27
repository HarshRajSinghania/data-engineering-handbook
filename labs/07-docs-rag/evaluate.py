"""Measure retrieval quality against a labelled question set.

    python evaluate.py                       # report only
    python evaluate.py --min-hit-at-5 0.85   # exit non-zero below the threshold (for CI)

A question is a *hit at k* when at least one of its expected guides appears among the
guides of the top-k retrieved chunks. MRR (mean reciprocal rank) rewards ranking the
right guide first. This is how you notice when a change to chunking or ranking helps
or hurts, instead of judging by a few hand-picked queries.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from rag import build_index, unique_sources

EVAL_SET = Path(__file__).with_name("eval_set.jsonl")
KS = (1, 3, 5)


def evaluate(index, questions: list[dict]) -> dict:
    hits = {k: 0 for k in KS}
    reciprocal = 0.0
    misses = []
    for q in questions:
        ranked = unique_sources(index.search(q["question"], k=20))
        rank = next((i + 1 for i, p in enumerate(ranked) if p in q["expected"]), None)
        for k in KS:
            hits[k] += rank is not None and rank <= k
        reciprocal += 1 / rank if rank else 0.0
        if rank is None or rank > 3:
            misses.append({"question": q["question"], "expected": q["expected"], "got": ranked[:3], "rank": rank})
    n = len(questions)
    return {"n": n, **{f"hit@{k}": hits[k] / n for k in KS}, "mrr": reciprocal / n, "misses": misses}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--min-hit-at-5", type=float, default=0.0, help="fail if hit@5 is below this")
    args = parser.parse_args()

    questions = [json.loads(line) for line in EVAL_SET.read_text().splitlines() if line.strip()]
    index = build_index()
    result = evaluate(index, questions)

    print(f"{len(index.chunks)} chunks indexed, {result['n']} questions\n")
    print(f"  hit@1  {result['hit@1']:.0%}\n  hit@3  {result['hit@3']:.0%}\n  hit@5  {result['hit@5']:.0%}\n  MRR    {result['mrr']:.2f}\n")
    if result["misses"]:
        print("Not in the top 3:")
        for m in result["misses"]:
            print(f"  - {m['question']}\n      expected {m['expected']}\n      got      {m['got']}  (rank of first expected: {m['rank']})")
    if result["hit@5"] < args.min_hit_at_5:
        print(f"\nFAIL: hit@5 {result['hit@5']:.0%} is below the required {args.min_hit_at_5:.0%}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
