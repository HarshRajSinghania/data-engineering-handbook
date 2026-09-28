# Governance

How decisions are made in Sarang's Data Engineering Handbook, who can do what, and how someone becomes a reviewer or maintainer. It is short on purpose. The project is small, and this page should change as it grows.

## Roles

| Role | What they do | How to become one |
|------|--------------|-------------------|
| **Contributor** | Opens issues and pull requests, reviews others' changes, answers questions | Nothing to do. Anyone can contribute, and the [contribution guide](CONTRIBUTING.md) starts with tasks that take 30 to 60 minutes |
| **Reviewer** | Is listed in [`.github/CODEOWNERS`](.github/CODEOWNERS) for an area (for example streaming, AI, or the labs), is asked to review pull requests that touch it, and can approve them | Have several substantive pull requests or reviews merged in that area, follow the conventions, and ask in an issue or be invited by a maintainer |
| **Maintainer** | Merges pull requests, publishes releases, triages issues, runs the [monthly routine](docs/maintenance.md#monthly-routine), and can change repository settings | Be a reviewer for at least three months with reliable, kind reviews, and be invited by the existing maintainers |
| **Owner** | Has administrator access to the repository, its Pages site and its secrets | The founder, currently [@sarangambekar1997](https://github.com/sarangambekar1997) |

## Current maintainers

| Name | Role | Areas |
|------|------|-------|
| [@sarangambekar1997](https://github.com/sarangambekar1997) | Owner and maintainer | Everything |

**Reviewers wanted** for any topic area, and especially for the labs (which need someone who can run Docker) and for the AI guides, whose tools change fastest. Comment on a [help wanted](https://github.com/sarangambekar1997/data-engineering-handbook/issues?q=is%3Aopen+label%3A%22help+wanted%22) issue, or open an issue titled *Reviewer request* saying which area you know.

## How decisions are made

- **Most changes need no discussion.** A pull request that fixes an error, or follows an accepted [topic request](https://github.com/sarangambekar1997/data-engineering-handbook/issues/new?template=topic-request.yml), is merged when CI passes and a reviewer approves it.
- **Larger changes are discussed first**, in an issue: a new guide or lab, a change to the template or conventions, a new tool in the build. The [roadmap](docs/roadmap.md) records the outcome.
- **Disagreements** are settled by the maintainers, aiming for agreement. If they cannot agree, the owner decides and writes down the reason in the issue.
- **This page changes by pull request**, like everything else.

## What every merge must satisfy

- The CI checks pass. They cover strict site build, code blocks, Mermaid diagrams, review dates and lab versions, search metadata, and, for lab changes, the labs themselves.
- Claims are checked. A guide states the version it was checked against, and a change that alters a `verified:` date lists in the pull request what was actually checked.
- While there is one maintainer, that maintainer merges their own pull requests once CI passes. Pull requests from anyone else get a review comment first.

## Continuity

A project with one maintainer stops when that person is unavailable. To limit that:

- **A second maintainer with administrator access** is the goal. Until then, the owner keeps the access below documented, so it can be handed over.
- **What a maintainer needs access to:** repository administration and settings; GitHub Pages and the `github-pages` environment; repository secrets (`TRAFFIC_TOKEN`, described in [How the handbook is maintained](docs/maintenance.md#metrics)); the release process; Discussions moderation.
- **Inactivity.** A reviewer or maintainer with no activity for six months is moved to emeritus status, with thanks, and can return by asking. This keeps the lists honest.

## Conduct and security

Everyone follows the [Code of Conduct](CODE_OF_CONDUCT.md). Report security problems as described in [SECURITY.md](SECURITY.md).
