---
name: docling-service
description: >
  Use when the user wants to convert, read or query a document (PDF, DOCX, PPTX,
  XLSX, HTML, EPUB, images, URLs) with the Docling for IBM watsonx service.
  Trigger phrases: "convert", "read this PDF", "what does this document say",
  "find the table", "extract fields from".
---

# Docling for IBM watsonx

Convert documents on the managed service with `docling convert-remote`, then
navigate the DocLang output with `dclq`. No local models are needed.

Credentials come from the `.env` file in the workspace root
(`DOCLING_SERVICE_URL`, `DOCLING_SERVICE_API_KEY`). Never print or export them.
Activate `.venv` and run commands from the workspace root.

## Convert

Always request Markdown and DocLang together:

    docling convert-remote --to md --to dclx --output ./converted <file-or-url-or-folder>

- Several sources: list them, or pass a folder.
- Only some formats from a folder: `--from pdf --from docx`.
- Born-digital PDF: add `--no-ocr`. Scanned PDF with bad text: `--force-ocr`.
- Classify pictures (chart, diagram, photo, ...): `--enrich-picture-classes`.
- Page subset: `--page-range 1-3`.

Output: `./converted/<name>.md` and `./converted/<name>.dclx`.
Exit code 1 means a connection or conversion failure, 2 means missing credentials.

## Navigate before answering

Do not read a whole document if you only need part of it. Use `dclq` on the
`.dclx` file:

    dclq inspect ./converted/<name>.dclx                          # inventory
    dclq outline ./converted/<name>.dclx                          # headings + XPaths
    dclq grep -i 'pattern' ./converted/<name>.dclx                # find units
    dclq show ./converted/<name>.dclx '/heading[N]' --section     # one section
    dclq list ./converted/<name>.dclx --type table_cell --page 1   # table cells

When you answer, cite the section heading or XPath you used.
Read the Markdown file when the user wants the whole document.
