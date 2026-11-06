-- The host Docling's answers live on — the second connection, as in example 02.
--
-- Docling for IBM watsonx does not return the extraction. It returns a
-- *presigned URL* to it on Amazon S3 (an `inbody` target is a 422), so
-- DoclingExtract's last call goes to a host that is not the service's, and a
-- UDF may only reach hosts it is bound to.
--
-- No credential: a presigned URL is its own. What this object provides is
-- reachability, nothing else. A self-hosted docling-serve that answers in the
-- body does not need it; point DOWNLOAD_ENDPOINT at that host then.
--
-- The PDFs need no connection at all: Docling fetches them, not the UDF.
CREATE CONNECTION `${DOWNLOAD_CONNECTION}`
WITH (
  'type' = 'REST',
  'endpoint' = '${DOWNLOAD_ENDPOINT}',
  'token' = ''
);
