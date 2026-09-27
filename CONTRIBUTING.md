# Contributing

Corrections and improvements are welcome: a wrong command, an outdated model ID or price, a broken link, or a missing topic.

## Reporting a problem

Open an [issue](https://github.com/sarangambekar1997/data-engineering-handbook/issues) with the guide name, the section, and what is wrong. A link to the vendor documentation that shows the correct behaviour helps a lot.

## Changing a guide

1. Fork the repo and create a branch (`fix/<short-name>` or `docs/<topic>`).
2. Edit the file under `docs/`. New guides start from [`docs/_template.md`](docs/_template.md) and go in the folder that matches where the topic sits in a pipeline. Add the guide to `nav:` in `mkdocs.yml` and to the index in [`docs/README.md`](docs/README.md).
3. Check your changes locally:

   ```bash
   pip install -r requirements-docs.txt pyyaml
   mkdocs build --strict              # fails on broken internal links and anchors
   python tools/check_code_blocks.py  # every python/json/yaml block must parse
   python tools/check_model_ids.py    # no retired model IDs
   mkdocs serve                       # preview at http://127.0.0.1:8000
   ```

4. Open a pull request. CI runs the same checks.

## Writing rules

- Every guide follows **Basic → Intermediate → Advanced**, then **Common Pitfalls**, **Cheat Sheet**, **Interview Questions** and **Further Reading**.
- Code blocks state their language and run as written, or say what they need (credentials, a cluster, sample data). A block that is a deliberate fragment goes after `<!-- docs-parse: skip -->`.
- Do not hardcode prices or "latest" model names. Link to the vendor page, or load values from config as the AI guides do. When a model is retired, add its ID pattern to [`tools/deprecated_models.txt`](tools/deprecated_models.txt) so CI finds every remaining mention.
- Use relative links between guides, and pick the anchor from the rendered heading.

## Labs

Each lab runs on a laptop and has a `README.md`, exercises and solutions. See [`labs/README.md`](labs/README.md) for the conventions. CI runs Labs 01–03 end to end and validates the Docker Compose files of Labs 04–05.

## License

By contributing you agree your contribution is released under the [MIT License](LICENSE).
