-- 3. Optional: try the pipeline without Docling.
--
-- Five hand-written chunks in the shape Docling produces: a normal one, a
-- duplicate of it, one with personal data, one too short, and a second normal one.
INSERT INTO `docling_chunks` VALUES
  (ENCODE('sample-doc', 'UTF-8'), ENCODE('{"chunk_id": "c1", "doc_id": "sample-doc", "chunk_index": 0, "headings": ["Introduction"], "text": "Docling converts PDF, Office and image documents into structured data for retrieval."}', 'UTF-8')),
  (ENCODE('sample-doc', 'UTF-8'), ENCODE('{"chunk_id": "c2", "doc_id": "sample-doc", "chunk_index": 1, "headings": ["Introduction"], "text": "Docling converts PDF,   Office and image documents into structured data for retrieval."}', 'UTF-8')),
  (ENCODE('sample-doc', 'UTF-8'), ENCODE('{"chunk_id": "c3", "doc_id": "sample-doc", "chunk_index": 2, "headings": ["Contact"], "text": "For questions about this report, write to jane.doe@example.com or call +1 555 010 2030."}', 'UTF-8')),
  (ENCODE('sample-doc', 'UTF-8'), ENCODE('{"chunk_id": "c4", "doc_id": "sample-doc", "chunk_index": 3, "headings": ["Contact"], "text": "Page 4"}', 'UTF-8')),
  (ENCODE('sample-doc', 'UTF-8'), ENCODE('{"chunk_id": "c5", "doc_id": "sample-doc", "chunk_index": 4, "headings": ["Results"], "text": "Tables keep their rows, columns and merged cells, so numbers stay next to their labels."}', 'UTF-8'));
