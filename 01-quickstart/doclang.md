# Explore the DocLang output with `dclq`

Markdown is easy to read, but it flattens the document. The `.dclx` file that
`convert.py` (or `convert.sh`) writes is a **DocLang** archive: it keeps headings,
sections, tables and their cells, figures, reading order and page positions.

## Look at it

Drag the `.dclx` file onto the [DocLang Viewer](https://doclang.ai/viewer/). It runs
in your browser and shows the page, the DocLang tree and a reading view side by side.

## Query it with `dclq`

`dclq` is a small command-line tool for DocLang files, a bit like `jq` for documents.

```bash
uv tool install dclq      # or: pip install dclq, or: uvx dclq ...
```

The examples below use the output of `uv run convert.py` (the Docling technical report):

```bash
dclq inspect output/2408.09869.dclx                            # inventory: pages, sections, tables, pictures
dclq outline output/2408.09869.dclx                            # heading tree, with the XPath of each section
dclq grep -i 'table' output/2408.09869.dclx                    # find matching elements
dclq show    output/2408.09869.dclx '/heading[2]' --section    # print one whole section
dclq list    output/2408.09869.dclx --type table_cell --page 5 # list the table cells on one page
```

Every element has an address such as `/heading[2]`. Use the addresses printed by
`outline` to cite a section, or to give only that section to an LLM instead of the
whole document.

> `dclq` is experimental: its commands and output may change between releases.
> Run `dclq --help` to see the options of your version.
