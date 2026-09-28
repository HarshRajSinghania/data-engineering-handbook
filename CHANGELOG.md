# Changelog

All notable changes to the handbook are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Versions describe the handbook as a whole:

- **Minor** (`0.x.0`): new guides, labs or site features.
- **Patch** (`0.x.y`): corrections, review-date refreshes and fixes to existing content.
- **1.0.0** is planned for when the [roadmap](docs/roadmap.md) coverage rounds are complete, every lab runs in CI and every guide has been reviewed within the last six months.

Contributors are credited by GitHub handle on the entry for their change.

## [Unreleased]

### Added

- Coverage round A, five new guides, bringing the total to 53: [Azure and Microsoft Fabric](docs/01-storage/azure-fabric.md), [Real-Time Analytics Databases](docs/01-storage/realtime-olap.md) (ClickHouse, Druid, Pinot), [Trino and Query Federation](docs/02-processing/trino-federation.md), [Kubernetes for Data Workloads](docs/06-infrastructure/kubernetes-for-de.md) and [Testing and CI/CD for Data Pipelines](docs/06-infrastructure/testing-cicd.md).
- Coverage round B, four new guides, bringing the total to 57: [Apache Beam and Dataflow](docs/04-streaming/beam-dataflow.md), [Streaming SQL](docs/04-streaming/streaming-sql.md) (RisingWave, Materialize), [BI Tools](docs/02-processing/bi-tools.md) (Superset, Metabase) and [NoSQL and Operational Stores](docs/01-storage/nosql-operational-stores.md).
- Coverage round C, four new guides, bringing the total to 61: [MCP and Text-to-SQL](docs/07-ai/mcp-text-to-sql.md), [Data Catalogs in Practice](docs/05-quality-governance/data-catalogs.md) (DataHub, OpenMetadata), [DataOps](docs/05-quality-governance/dataops-operations.md) (on-call and incident response) and [Choosing a Stack](docs/08-architecture/choosing-a-stack.md).
- Labs 04 and 05 now run end to end in CI: `ci_smoke.py` in each lab replays the README exercises against a live Kafka broker and a live Airflow, and asserts the results the README describes. Both also run weekly.
- Lab 05: the README now creates `warehouse/` before `docker compose up`. Docker otherwise creates it owned by root on Linux and Airflow fails with `Permission denied` (found by the new CI job).
- Lab 08, data quality gates with Great Expectations: suites, thresholds and severity, and a gate that blocks bad, stale and schema-changed data. The Data Quality guide gains a section on thresholds and severity and a lab-tested version.
- Lab 09, the Lab 03 pipeline on Apache Iceberg: snapshots, `MERGE`, schema and partition evolution, maintenance and branches. The Iceberg guide now shows the tested Spark 4.1 and Iceberg 1.11 setup, replaces the `snapshot-id` and `as-of-timestamp` reader options that no longer work with `versionAsOf` and `timestampAsOf`, and has a lab-tested version.
- Lab 10, change data capture with Debezium: Postgres to Kafka, applying inserts, updates and deletes idempotently, connector pause and restart, a new snapshot, and a schema change. The Ingestion & CDC guide is corrected (an update's `before` is null on Postgres unless `REPLICA IDENTITY FULL`), gains a note on tombstones and hard deletes, and has a lab-tested version. Two glossary terms and two abbreviations added.
- Role-based [learning paths](docs/paths/index.md) for analytics engineers, data platform engineers and AI data engineers, each pairing guides with labs and checkpoints.
- Per-page search and sharing metadata: every page has its own description (from its summary line) and structured data, and a CI check (`tools/check_seo.py`) keeps it that way.
- Print stylesheet and a *Print this cheat sheet* button on every guide, and PDF export: one PDF per guide and one bookmarked PDF of the whole handbook, built in CI (closes #32).
- A [dev container](.devcontainer/devcontainer.json) with Python 3.12, Java 17 and Docker, and a Codespaces link in the README.
- Thirty-three glossary terms introduced by the new guides.
- Community files: `CODEOWNERS`, `CITATION.cff`, this changelog, release-note categories and a public [roadmap](docs/roadmap.md).
- A "first contribution" path and contributor recognition policy in [CONTRIBUTING.md](CONTRIBUTING.md).

## [0.5.0] - 2026-09-28

### Added

- A review date (`verified`) on every guide, shown as "Last reviewed", with a warning when a guide is more than six months overdue (#11).
- "Lab-tested with" versions on the guides whose tools a lab runs in CI, and a check that fails when a lab's pinned version drifts from the guide (#11).
- [How the handbook is maintained](docs/maintenance.md), and a weekly job that keeps one tracking issue of overdue guides (#11).

### Fixed

- The Pages deployment job no longer runs on manual runs from branches other than `main` (#11).

## [0.4.0] - 2026-09-27

### Added

- Card-grid homepage, term tooltips, breadcrumbs and typography changes (#8).
- `CODE_OF_CONDUCT.md`, `SECURITY.md` and registered issue templates (#10).

### Changed

- The right-hand table of contents lists only top-level sections instead of 40 or more entries (#9).

### Fixed

- Dead internal and external links (#10).

## [0.3.0] - 2026-09-27

### Added

- New guides, bringing the total from 38 to 48, and a Mermaid diagram in every guide (#7).
- Two capstone projects: a Dagster pipeline (Lab 06) and a docs RAG system with retrieval evals (Lab 07) (#7).
- MIT license, an about page, a social card, issue and pull request templates (#7).
- CI checks for code blocks, Mermaid diagrams and retired model IDs (#7).

### Changed

- Renamed the project to "Sarang's Data Engineering Handbook" (#7).
- Refreshed AI model IDs and links, and standardized the Common Pitfalls sections (#6).

### Fixed

- The Capstone 07 evaluation set was excluded by a `.gitignore` rule and is now committed (#7).

## [0.2.0] - 2026-09-24

### Added

- Five hands-on labs on one shared e-commerce dataset: SQL with DuckDB, dbt, a Spark and Delta Lake lakehouse, Kafka streaming and Airflow orchestration, with a labs index and CI that runs Labs 01 to 03 end to end (#5).

## [0.1.0] - 2026-09-24

### Added

- The first 30 guides, organized into topic folders with a shared template, prerequisite and related links, and Next/Back navigation (#1, #2).
- The MkDocs Material site with a strict build in CI and deployment to GitHub Pages (#3).
- Eight more guides, for 38 in total: ingestion and CDC, system design, BigQuery, Redshift, DuckDB and Polars, Flink, governance and cost optimization (#4).

[Unreleased]: https://github.com/sarangambekar1997/data-engineering-handbook/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/sarangambekar1997/data-engineering-handbook/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/sarangambekar1997/data-engineering-handbook/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/sarangambekar1997/data-engineering-handbook/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/sarangambekar1997/data-engineering-handbook/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/sarangambekar1997/data-engineering-handbook/releases/tag/v0.1.0
