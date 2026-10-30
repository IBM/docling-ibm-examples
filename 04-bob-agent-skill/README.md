# 04 · Docling in IBM Bob: fill a compliance form from three documents

A compliance analyst receives a risk disclosure (PDF), an internal audit summary (Word)
and regulatory guidelines (EPUB), and has to fill the same eight-field assessment form
every quarter. In this example **IBM Bob** does it: it converts the documents with
Docling for IBM watsonx (`docling convert-remote`), navigates the DocLang output with
`dclq`, fills the form and writes a short report. No code to write: Bob only needs the
skill in `.bob/skills/`.

| Path | What it is |
|---|---|
| [`.bob/skills/docling-service/SKILL.md`](.bob/skills/docling-service/SKILL.md) | The skill that tells Bob how to convert with Docling for IBM watsonx and navigate with `dclq` |
| [`docs/`](docs) | Three sample documents (PDF, DOCX, EPUB) and the empty form (`compliance_assessment_form.json`). All companies, people and figures are fictional; mentions of real regulators are for realism only |
| [`form.md`](form.md) | The eight fields of the form and which document contains each |
| [`prompts.md`](prompts.md) | The prompts to send to Bob, in order |

This folder accompanies the [Docling in IBM Bob](https://developer.dcls.saas.ibm.com/tutorials/bob-agent-skill) tutorial of the
[Docling for IBM watsonx documentation](https://developer.dcls.saas.ibm.com/).

## Prerequisites

- A Docling for IBM watsonx instance: the **Service URL** and an **API key**
  (Workbench, **API examples**).
- [IBM Bob](https://www.ibm.com/products/bob), signed in.
- Python 3.10 or later and [`uv`](https://docs.astral.sh/uv/) (`pip` works too).

## Setup

```bash
git clone https://github.com/IBM/docling-ibm-examples
cd docling-ibm-examples/04-bob-agent-skill

uv venv
source .venv/bin/activate
uv pip install docling-client dclq      # or: uv sync

cp ../.env.example .env                 # fill in DOCLING_SERVICE_URL and DOCLING_SERVICE_API_KEY
```

The `.env` file must be in this folder, the workspace root that Bob uses. It is ignored
by git; never commit it.

Test the connection from the terminal:

```bash
docling convert-remote --to md --output ./converted docs/regulatory_guidelines_2026.epub
head -20 converted/regulatory_guidelines_2026.md
```

| You see | Cause |
|---|---|
| Exit code 2, `No service URL` | `.env` is missing, not in the current folder, or a variable name is misspelt |
| `401` or `403` | The API key is wrong or has been regenerated |
| `Cannot reach service` | The Service URL is wrong, or outbound HTTPS is blocked |

## Run

1. Open **this folder** (`bob-agent-skill/`) in IBM Bob as the workspace. Bob loads the
   skill from `.bob/skills/` when it opens the workspace; reload the window if you add or
   change the skill later.
2. Send the prompts from [`prompts.md`](prompts.md) one at a time.

## Expected output

In `converted/` (ignored by git):

- a `.md` and a `.dclx` file for each of the three documents,
- `compliance_assessment_form_filled.json` with all eight fields set,
- `compliance_report.md` with a short executive summary and the filled form as a table,
  with the source document and section of every field.

Spot-check two values yourself:

```bash
dclq grep -i 'reference' converted/vantara_risk_disclosure.dclx
dclq grep -i 'sign' converted/q3_audit_summary.dclx
```

You can also open any `.dclx` file in the [DocLang Viewer](https://doclang.ai/viewer/).

## Clean up

Delete `converted/` and `.env`, and revoke the API key if you created it only for this
example.
