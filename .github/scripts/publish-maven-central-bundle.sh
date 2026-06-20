#!/bin/bash
set -euo pipefail

BUNDLE_FILE="${CENTRAL_BUNDLE_FILE:?CENTRAL_BUNDLE_FILE is required}"
DEPLOYMENT_NAME="${CENTRAL_DEPLOYMENT_NAME:?CENTRAL_DEPLOYMENT_NAME is required}"
USERNAME="${CENTRAL_PORTAL_USERNAME:?CENTRAL_PORTAL_USERNAME is required}"
PASSWORD="${CENTRAL_PORTAL_PASSWORD:?CENTRAL_PORTAL_PASSWORD is required}"
PUBLISHING_TYPE="${CENTRAL_PUBLISHING_TYPE:-AUTOMATIC}"
API_BASE="${CENTRAL_PORTAL_API_BASE:-https://central.sonatype.com/api/v1/publisher}"
MAX_ATTEMPTS="${CENTRAL_STATUS_MAX_ATTEMPTS:-360}"
SLEEP_SECONDS="${CENTRAL_STATUS_SLEEP_SECONDS:-10}"

if [ ! -f "$BUNDLE_FILE" ]; then
  echo "Central bundle not found: $BUNDLE_FILE" >&2
  exit 1
fi

TOKEN="$(printf '%s:%s' "$USERNAME" "$PASSWORD" | base64 | tr -d '\n')"
UPLOAD_RESPONSE="$(mktemp)"

HTTP_CODE="$(
  curl -sS \
    --request POST \
    --header "Authorization: Bearer $TOKEN" \
    --form "bundle=@${BUNDLE_FILE};type=application/octet-stream" \
    --output "$UPLOAD_RESPONSE" \
    --write-out '%{http_code}' \
    "${API_BASE}/upload?publishingType=${PUBLISHING_TYPE}&name=${DEPLOYMENT_NAME}"
)"

if [ "$HTTP_CODE" != "201" ]; then
  echo "Central Portal upload failed with HTTP $HTTP_CODE" >&2
  cat "$UPLOAD_RESPONSE" >&2
  exit 1
fi

DEPLOYMENT_ID="$(tr -d '\r\n' < "$UPLOAD_RESPONSE")"
if [ -z "$DEPLOYMENT_ID" ]; then
  echo "Central Portal upload returned an empty deployment id" >&2
  exit 1
fi

echo "Central Portal deployment id: $DEPLOYMENT_ID"

for attempt in $(seq 1 "$MAX_ATTEMPTS"); do
  STATUS_JSON="$(
    curl -fsS \
      --request POST \
      --header "Authorization: Bearer $TOKEN" \
      "${API_BASE}/status?id=${DEPLOYMENT_ID}"
  )"
  STATE="$(echo "$STATUS_JSON" | jq -r '.deploymentState // empty')"
  echo "Central Portal status attempt $attempt/$MAX_ATTEMPTS: $STATE"

  case "$STATE" in
    PUBLISHED)
      echo "$STATUS_JSON" | jq
      exit 0
      ;;
    FAILED)
      echo "$STATUS_JSON" | jq >&2
      exit 1
      ;;
    PENDING|VALIDATING|VALIDATED|PUBLISHING)
      sleep "$SLEEP_SECONDS"
      ;;
    *)
      echo "Unknown Central Portal deployment state: $STATE" >&2
      echo "$STATUS_JSON" | jq >&2
      exit 1
      ;;
  esac
done

echo "Timed out waiting for Central Portal deployment $DEPLOYMENT_ID to publish" >&2
exit 1
