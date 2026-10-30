"""Convert a document with Docling for IBM watsonx and save Markdown and DocLang.

    uv run convert.py                        # the Docling technical report
    uv run convert.py path/to/report.pdf     # a local file, or any URL
"""

import os
import sys
from pathlib import Path

from docling.service_client import DoclingServiceClient
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv(usecwd=True))  # .env here or in a parent folder

source = sys.argv[1] if len(sys.argv) > 1 else "https://arxiv.org/pdf/2408.09869"
is_url = source.startswith(("http://", "https://"))
name = source.rstrip("/").rsplit("/", 1)[-1] if is_url else Path(source).stem

with DoclingServiceClient(
    url=os.environ["DOCLING_SERVICE_URL"],
    api_key=os.environ["DOCLING_SERVICE_API_KEY"],
) as client:
    document = client.convert(source=source if is_url else Path(source)).document

output = Path("output")
output.mkdir(exist_ok=True)
(output / f"{name}.md").write_text(document.export_to_markdown(), encoding="utf-8")
document.save_as_doclang_archive(output / f"{name}.dclx")

print(f"Saved {output / name}.md and {output / name}.dclx ({document.num_pages()} pages)")
