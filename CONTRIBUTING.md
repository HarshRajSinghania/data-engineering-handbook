# Contributing

Corrections and improvements are welcome: a wrong command, an outdated model ID or price, a broken link, or a missing topic.

## Your first contribution

Most useful contributions are small, and you do not need to install anything for the first two.

| Task | Effort | How |
|------|--------|-----|
| Fix a typo, wrong command or dead link | 5 minutes | Click the pencil icon (**Edit this page**) on any page of the site. GitHub forks the repo and opens the pull request for you. |
| Review a guide against the vendor documentation | 30-60 minutes | Take a guide from the [good first issue](https://github.com/sarangambekar1997/data-engineering-handbook/issues?q=is%3Aopen+label%3A%22good+first+issue%22) list, run its commands, correct what changed, update `verified:` and remove `review_status: baseline`. See [Reviewing a guide](docs/maintenance.md#reviewing-a-guide). |
| Add an interview question or a glossary term | 15-30 minutes | Follow the format of the existing entries in the guide or in the [glossary](docs/99-reference/glossary.md). |
| Write a new guide or lab | Several hours | Pick an item marked *help wanted* on the [roadmap](docs/roadmap.md), and comment on its issue first so the work is not duplicated. |

The steps are the same for all of them:

1. Find or open an issue, and comment that you are working on it.
2. Fork the repo, create a branch and make the change.
3. Open a pull request. CI runs the checks and reports what to fix, so you can open one without any local tooling. The PR template lists what reviewers look for.

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
   python tools/check_freshness.py    # review dates and lab versions are valid
   mkdocs serve                       # preview at http://127.0.0.1:8000
   ```

4. Open a pull request. CI runs the same checks.

## Writing rules

- Every guide follows **Basic → Intermediate → Advanced**, then **Common Pitfalls**, **Cheat Sheet**, **Interview Questions** and **Further Reading**.
- Code blocks state their language and run as written, or say what they need (credentials, a cluster, sample data). A block that is a deliberate fragment goes after `<!-- docs-parse: skip -->`.
- Do not hardcode prices or "latest" model names. Link to the vendor page, or load values from config as the AI guides do. When a model is retired, add its ID pattern to [`tools/deprecated_models.txt`](tools/deprecated_models.txt) so CI finds every remaining mention.
- Use relative links between guides, and pick the anchor from the rendered heading.
- Every guide has `verified: YYYY-MM-DD` in its front matter. Set it only after you have actually checked the guide's commands, versions and links against the vendor documentation, and say what you checked in the pull request. A typo fix does not change it. Guides marked `review_status: baseline` have never been individually reviewed; when you review one, remove that line as well. See [How the handbook is maintained](docs/maintenance.md).

## Growing into a reviewer

Regular contributors can become reviewers and, later, maintainers. [GOVERNANCE.md](GOVERNANCE.md) describes the roles and what each involves.

## Labs

Each lab runs on a laptop and has a `README.md`, exercises and solutions. See [`labs/README.md`](labs/README.md) for the conventions. CI runs every lab end to end. Labs 04 and 05 start Kafka and Airflow with Docker Compose and run `ci_smoke.py`, which replays the README exercises and checks the results the README describes. You can run the same script locally, or open the repository in a [dev container](.devcontainer/devcontainer.json) that has Python, Java and Docker ready.

## Recognition

Contributors are listed on the repository's [contributors page](https://github.com/sarangambekar1997/data-engineering-handbook/graphs/contributors), and each entry in the [changelog](CHANGELOG.md) credits the GitHub handle of the person whose change it records. A contributor who reviews or maintains an area over time can be added to [`.github/CODEOWNERS`](.github/CODEOWNERS) for it.

## License

By contributing you agree your contribution is released under the [MIT License](LICENSE).
