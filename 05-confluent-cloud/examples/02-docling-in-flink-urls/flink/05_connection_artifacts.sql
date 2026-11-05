-- The second connection, and the one that surprises people.
--
-- Docling for IBM watsonx does not return the converted document. It returns a
-- *presigned URL* on Amazon S3 — there is no `inbody` target to ask for
-- otherwise; the service answers 422 for one — so the result lives on a host
-- that is not the service's. A UDF may only reach hosts it is bound to, so
-- fetching it needs a connection of its own.
--
-- It carries no credential. A presigned URL is its own credential, and the
-- empty token says so. What this object provides is reachability, nothing else.
--
-- A self-hosted docling-serve that answers in the body makes this connection
-- unnecessary; the UDF handles both shapes, and you can point DOWNLOAD_ENDPOINT
-- at the same host in that case.
CREATE CONNECTION `${DOWNLOAD_CONNECTION}`
WITH (
  'type' = 'REST',
  'endpoint' = '${DOWNLOAD_ENDPOINT}',
  'token' = ''
);
