# Security Policy

This repository is a documentation site and a set of local, educational labs. It
has no server, no user data, and no production deployment beyond the static
GitHub Pages site built by `mkdocs`. That said, a few things are worth
reporting responsibly if you find them.

## What to report privately

Use GitHub's [private vulnerability reporting](https://github.com/sarangambekar1997/data-engineering-handbook/security/advisories/new) (Security → Advisories → Report a vulnerability) rather than a public issue for:

- A dependency in `requirements-docs.txt` or any `labs/*/requirements.txt` with
  a known, exploitable CVE.
- Anything in the GitHub Actions workflows (`.github/workflows/`) that could
  leak a secret, run untrusted code on `pull_request` with write permissions,
  or otherwise be abused via a malicious PR.
- A guide's example code that demonstrates an insecure pattern as if it were
  safe (for example, string-built SQL presented without a warning, or a secret
  hardcoded in a "real" example rather than an obvious placeholder).

## What's fine as a public issue

- A broken link, a wrong command, an outdated model ID or price, or unclear
  writing — see [CONTRIBUTING.md](CONTRIBUTING.md).
- A lab that fails to run as documented.

## Response

This is a solo-maintained project. I aim to acknowledge a security report
within a week and to fix or provide a mitigation promptly once confirmed.
There is no bug bounty.
