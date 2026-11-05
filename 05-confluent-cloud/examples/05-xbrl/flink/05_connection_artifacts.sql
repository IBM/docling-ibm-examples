-- Reachability connection for external artifact downloads (e.g. S3 presigned URLs).
CREATE CONNECTION `${DOWNLOAD_CONNECTION}`
WITH (
  'type' = 'REST',
  'endpoint' = '${DOWNLOAD_ENDPOINT}',
  'token' = ''
);
