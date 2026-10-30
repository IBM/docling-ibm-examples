#!/usr/bin/env bash
# Convert a document with the Docling for IBM watsonx REST API, using only curl and jq.
#
#   ./convert.sh                                  # converts the Docling technical report
#   ./convert.sh https://arxiv.org/pdf/2206.01062  # any public URL
#   ./convert.sh path/to/report.pdf               # a local file (uploaded)
#
# Reads DOCLING_SERVICE_URL and DOCLING_SERVICE_API_KEY from the environment,
# or from a .env file in this folder or a parent folder.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Load .env files from the repository root down to this folder (the nearest one wins).
for env_file in "${HERE}/../../.env" "${HERE}/../.env" "${HERE}/.env"; do
  if [ -f "${env_file}" ]; then set -a; . "${env_file}"; set +a; fi
done

: "${DOCLING_SERVICE_URL:?Set DOCLING_SERVICE_URL (environment or .env)}"
: "${DOCLING_SERVICE_API_KEY:?Set DOCLING_SERVICE_API_KEY (environment or .env)}"
command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

SOURCE="${1:-https://arxiv.org/pdf/2408.09869}"
OUTPUT="${OUTPUT:-output}"
mkdir -p "${OUTPUT}"

# 1. Submit
if [ -f "${SOURCE}" ]; then
  echo "Uploading ${SOURCE} ..."
  TASK_ID=$(curl -sS --fail-with-body -X POST "${DOCLING_SERVICE_URL}/v1/convert/file/async" \
    -H "X-Api-Key: ${DOCLING_SERVICE_API_KEY}" \
    -F "files=@${SOURCE}" \
    -F "to_formats=md" -F "to_formats=dclx" | jq -r '.task_id')
else
  echo "Submitting ${SOURCE} ..."
  TASK_ID=$(curl -sS --fail-with-body -X POST "${DOCLING_SERVICE_URL}/v1/convert/source/async" \
    -H "X-Api-Key: ${DOCLING_SERVICE_API_KEY}" \
    -H "Content-Type: application/json" \
    -d "$(jq -n --arg url "${SOURCE}" \
      '{sources: [{kind: "http", url: $url}], options: {to_formats: ["md", "dclx"]}}')" \
    | jq -r '.task_id')
fi
echo "Task: ${TASK_ID}"

# 2. Poll until the task is done
while true; do
  STATUS=$(curl -sS --fail-with-body "${DOCLING_SERVICE_URL}/v1/status/poll/${TASK_ID}" \
    -H "X-Api-Key: ${DOCLING_SERVICE_API_KEY}" | jq -r '.task_status')
  echo "Status: ${STATUS}"
  case "${STATUS}" in
    success) break ;;
    failure) echo "Conversion failed" >&2; exit 1 ;;
  esac
  sleep 3
done

# 3. Fetch the result and download every artifact (the links expire, so do it now)
RESULT=$(curl -sS --fail-with-body "${DOCLING_SERVICE_URL}/v1/result/${TASK_ID}" \
  -H "X-Api-Key: ${DOCLING_SERVICE_API_KEY}")
echo "${RESULT}" | jq -r '.documents[] | .filename as $f | .artifacts[] | [$f, .artifact_type, .uri] | @tsv' |
while IFS=$'\t' read -r FILENAME TYPE URI; do
  case "${TYPE}" in
    markdown) EXT="md" ;;
    *) EXT="${TYPE}" ;;
  esac
  BASE="${FILENAME}"
  case "${BASE}" in
    *.pdf|*.docx|*.pptx|*.xlsx|*.html|*.htm|*.md|*.epub|*.png|*.jpg|*.jpeg|*.tiff) BASE="${BASE%.*}" ;;
  esac
  DEST="${OUTPUT}/${BASE}.${EXT}"
  curl -sS --fail -o "${DEST}" "${URI}"
  echo "Saved ${DEST}"
done
