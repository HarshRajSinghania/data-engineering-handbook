#!/usr/bin/env bash
# Runs once when the dev container is created.
set -euo pipefail

# Tools for building and checking the handbook (see CONTRIBUTING.md)
pip install --quiet -r requirements-docs.txt pyyaml

cat <<'MSG'

Ready. Each lab keeps its own dependencies, so create a virtual environment per lab:

  cd labs/01-sql-analytics
  python -m venv .venv && source .venv/bin/activate
  pip install -r requirements.txt

Labs 04 and 05 use Docker Compose (already available in this container).
Preview the handbook with:  mkdocs serve
MSG
