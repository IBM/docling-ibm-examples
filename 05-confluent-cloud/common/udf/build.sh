#!/usr/bin/env bash
# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
# Build the uploadable artifact.
#
# Confluent Cloud takes a Python UDF as a **zip containing the sdist tarball** —
# not a wheel, and not a zip of the source tree. `uv build --sdist` makes the
# tarball; `zip -j` puts it in a zip with no directory prefix, which is the
# shape the runtime unpacks.
set -euo pipefail
cd "$(dirname "$0")"

VERSION=$(grep -m1 '^version = ' pyproject.toml | cut -d'"' -f2)
SDIST="dist/docling_udf-${VERSION}.tar.gz"
ZIP="dist/docling_udf-${VERSION}.zip"

rm -f "$SDIST" "$ZIP"
uv build --sdist
[ -f "$SDIST" ] || { echo "expected $SDIST; got: $(ls dist)" >&2; exit 1; }
zip -FS -j "$ZIP" "$SDIST" >/dev/null

# Exactly one line on stdout — the path — because setup.sh reads it. Anything
# for a human goes to stderr: a second stdout line meant the caller reached for
# `head -1`, which closed this pipe mid-write and killed the build with SIGPIPE
# before anything was ever uploaded.
ls -lh "$ZIP" >&2
echo "$ZIP"
