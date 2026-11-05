#!/usr/bin/env bash
# Build the uploadable artifact — the same shape as common/udf/build.sh (a zip
# holding the sdist tarball), with one step in front: compile the manifest into
# the package, because that is what the fake extraction answers from.
set -euo pipefail
cd "$(dirname "$0")"

python3 compile_truth.py

VERSION=$(grep -m1 '^version = ' pyproject.toml | cut -d'"' -f2)
SDIST="dist/invoice_udf-${VERSION}.tar.gz"
ZIP="dist/invoice_udf-${VERSION}.zip"

rm -f "$SDIST" "$ZIP"
uv build --sdist >&2
[ -f "$SDIST" ] || { echo "expected $SDIST; got: $(ls dist)" >&2; exit 1; }
tar -tzf "$SDIST" | grep -q '_truth.py$' || { echo "_truth.py is not in $SDIST" >&2; exit 1; }
zip -FS -j "$ZIP" "$SDIST" >/dev/null

# One line on stdout, the path, because setup.sh reads it. See common/udf/build.sh.
ls -lh "$ZIP" >&2
echo "$ZIP"
