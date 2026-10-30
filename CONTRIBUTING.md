# Contributing

Contributions are welcome: fixes to an example, a new language for an existing example, or a new example.

## Before you start

- **Bugs:** [open an issue](https://github.com/IBM/docling-ibm-examples/issues) with the example, the command you ran and the output, so it can be tracked.
- **New examples or new languages:** [open an issue](https://github.com/IBM/docling-ibm-examples/issues) first, so we can agree on the scope before you write the code.
- **Problems with the Docling for IBM watsonx service itself** (not with an example): contact [IBM Support](https://www.ibm.com/mysupport/).

## How the repository is organised

Each example lives in a numbered folder with a language-neutral `README.md`, and each implementation sits in a subfolder named after its language or tool (`python/`, `curl/`, `java/`, ...). See [Repository layout](README.md#repository-layout).

When you add or change an example:

- Keep it self-contained and runnable from its own folder.
- Read credentials from the environment or a `.env` file, using the variable names in [`.env.example`](.env.example). Never commit credentials or real API keys.
- Use public or fictional sample documents only.
- For Python, declare dependencies in the folder's `pyproject.toml` so `uv run` works without extra steps.
- Update the example `README.md`, and the tables in the top-level [README.md](README.md) if you add a folder.
- Run the example end to end against a Docling for IBM watsonx instance before opening the pull request.

## Pull requests

1. Fork the repository and create a branch for your change.
2. Commit with a sign-off (see [Legal](#legal)).
3. Open a pull request that says what the change does and how you tested it.

A maintainer reviews every pull request. For the list of maintainers, see [MAINTAINERS.md](MAINTAINERS.md).

## Legal

This project is licensed under the [Apache License 2.0](LICENSE). New source files should start with an SPDX license header, for example:

```python
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
```

We use the [Developer's Certificate of Origin 1.1 (DCO)](https://developercertificate.org/), the same approach as the Linux kernel community. Every commit must carry a sign-off line that certifies you wrote the change or have the right to submit it:

```
Signed-off-by: John Doe <john.doe@example.com>
```

`git commit -s` adds it for you. A check on each pull request verifies the sign-off.
