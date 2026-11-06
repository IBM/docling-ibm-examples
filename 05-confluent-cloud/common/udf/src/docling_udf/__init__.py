"""Docling chunk enrichment as Confluent Cloud Flink Python UDFs.

``docling_udf.functions`` holds the registered functions. The modules beside it
— :mod:`chunk_record`, :mod:`enrichment`, :mod:`pii` — hold the decisions as
pure functions, with no Flink in them, so they can be tested without a cluster.
"""

__version__ = "0.1.0"
