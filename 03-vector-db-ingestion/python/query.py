"""Search the Docling chunks in AstraDB. AstraDB embeds the question ($vectorize).

    uv run query.py "How many pages does DocLayNet contain?"
"""

import os
import sys
from pathlib import Path

from astrapy import DataAPIClient
from dotenv import load_dotenv

for folder in (Path.cwd(), *Path.cwd().parents):
    load_dotenv(folder / ".env", override=False)

question = " ".join(sys.argv[1:]) or "How many pages does DocLayNet contain?"

database = DataAPIClient().get_database(
    os.environ["ASTRA_DB_API_ENDPOINT"], token=os.environ["ASTRA_DB_TOKEN"]
)
collection = database.get_collection(os.environ.get("ASTRA_DB_COLLECTION", "docling_chunks"))

hits = collection.find(
    sort={"$vectorize": question},
    limit=5,
    projection={"doc_id": True, "headings": True, "page_numbers": True, "text": True},
)

print(f"Q: {question}\n")
for i, hit in enumerate(hits, start=1):
    print(f"[{i}] {hit.get('doc_id')}  {hit.get('headings')}  pages {hit.get('page_numbers')}")
    print(f"    {hit.get('text', '')[:200]}\n")
