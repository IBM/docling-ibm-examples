#!/usr/bin/env bash
# Build the uploadable Java artifact — one shaded JAR.
#
# Needs a JDK and nothing else: if `mvn` is not on your PATH this downloads a
# pinned Apache Maven into ../../.tools and uses that. A repo you clone should
# build, and asking someone to `brew install maven` before they can read an
# example is a step too many.
#
#   ./build.sh          -> target/docling-flink-udf.jar
#   ./build.sh test     -> the unit tests, without a cluster
set -euo pipefail
cd "$(dirname "$0")"

MAVEN_VERSION="3.9.11"
MAVEN_SHA512="bcfe4fe305c962ace56ac7b5fc7a08b87d5abd8b7e89027ab251069faebee516b0ded8961445d6d91ec1985dfe30f8153268843c89aa392733d1a3ec956c9978"
TOOLS="$(cd ../.. && pwd)/.tools"

mvn_bin() {
  if command -v mvn >/dev/null 2>&1; then
    command -v mvn
    return
  fi
  local home="$TOOLS/apache-maven-$MAVEN_VERSION"
  if [ ! -x "$home/bin/mvn" ]; then
    # archive.apache.org rather than dlcdn, because dlcdn keeps only the
    # current release and this pin has to keep working.
    local url="https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    local tarball="$TOOLS/apache-maven-$MAVEN_VERSION-bin.tar.gz"
    mkdir -p "$TOOLS"
    echo "  no mvn on PATH; fetching Apache Maven $MAVEN_VERSION into .tools/" >&2
    curl -fsSL -o "$tarball" "$url"
    # Checked, because this is a build tool downloaded over the network and the
    # published checksum is the only thing that makes that defensible.
    local got
    got=$(shasum -a 512 "$tarball" | cut -d' ' -f1)
    [ "$got" = "$MAVEN_SHA512" ] || {
      rm -f "$tarball"
      echo "  the download does not match the published sha512 — refusing to use it" >&2
      exit 1
    }
    tar -xzf "$tarball" -C "$TOOLS"
    rm -f "$tarball"
  fi
  printf '%s' "$home/bin/mvn"
}

MVN=$(mvn_bin)

if [ "${1:-}" = "test" ]; then
  "$MVN" -q -B test
  exit 0
fi

# -q keeps the build quiet; everything Maven does say goes to stderr below, so
# that stdout stays the one line setup.sh reads.
"$MVN" -B -q -DskipTests package >&2

JAR="target/docling-flink-udf.jar"
[ -f "$JAR" ] || { echo "expected $JAR; got: $(ls target 2>/dev/null)" >&2; exit 1; }

# Exactly one line on stdout — the path — because setup.sh reads it. Anything
# for a human goes to stderr.
ls -lh "$JAR" >&2
echo "$JAR"
