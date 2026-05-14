#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")/.."

VERSION="${FEDDI_API_USAGE_VERSION:?FEDDI_API_USAGE_VERSION is required}"
REQUIRE_SIGNING="${FEDDI_API_USAGE_REQUIRE_SIGNING:-false}"

if [ "$REQUIRE_SIGNING" = "true" ]; then
  : "${ORG_GRADLE_PROJECT_signingInMemoryKey:?ORG_GRADLE_PROJECT_signingInMemoryKey is required when signing is required}"
  : "${ORG_GRADLE_PROJECT_signingInMemoryKeyPassword:?ORG_GRADLE_PROJECT_signingInMemoryKeyPassword is required when signing is required}"
fi

./gradlew clean build centralPortalBundle \
  -PfeddiApiUsageVersion="$VERSION" \
  -PfeddiApiUsageRequireSigning="$REQUIRE_SIGNING" \
  "$@"

if [ "$REQUIRE_SIGNING" = "true" ]; then
  missing_signatures=0
  while IFS= read -r artifact; do
    if [ ! -f "$artifact.asc" ]; then
      echo "Missing signature for $artifact" >&2
      missing_signatures=1
    fi
  done < <(find build/maven-repository -type f \( -name '*.jar' -o -name '*.pom' -o -name '*.module' \) ! -name '*.asc' | sort)
  if [ "$missing_signatures" -ne 0 ]; then
    exit 1
  fi
fi

mkdir -p dist/api-usage

client_jars=()
while IFS= read -r jar; do
  client_jars+=("$jar")
done < <(find usage-client/build/libs -maxdepth 1 -type f -name 'feddi-api-usage-client-*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' ! -name '*-jcstress.jar' | sort)

proto_jars=()
while IFS= read -r jar; do
  proto_jars+=("$jar")
done < <(find usage-proto/build/libs -maxdepth 1 -type f -name 'feddi-api-usage-proto-*.jar' ! -name '*-sources.jar' ! -name '*-javadoc.jar' | sort)

if [ "${#client_jars[@]}" -ne 1 ]; then
  echo "Expected exactly one usage client jar, found ${#client_jars[@]}" >&2
  printf '%s\n' "${client_jars[@]}" >&2
  exit 1
fi

if [ "${#proto_jars[@]}" -ne 1 ]; then
  echo "Expected exactly one usage proto jar, found ${#proto_jars[@]}" >&2
  printf '%s\n' "${proto_jars[@]}" >&2
  exit 1
fi

cp "${client_jars[0]}" dist/api-usage/feddi-api-usage-client.jar
cp "${proto_jars[0]}" dist/api-usage/feddi-api-usage-proto.jar
