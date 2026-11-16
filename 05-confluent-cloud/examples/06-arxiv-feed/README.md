# 06 — arXiv feed

Coming soon:

**Every new paper on arXiv, extracted into the fields you care about, in the
minute it is announced.**

A Kafka Connect source connector polls the arXiv RSS feeds and writes one
record per paper: title, authors, date, categories, abstract, and the PDF URL.
One Flink statement then calls Docling for IBM watsonx on that PDF and extracts
the terms you chose — method, datasets, results, limitations, institutions —
and writes the key/value answers to a compacted topic a dashboard reads.
