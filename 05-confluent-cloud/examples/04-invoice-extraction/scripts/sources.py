# Copyright IBM Corp. 2026
# SPDX-License-Identifier: Apache-2.0
"""Where an invoice's PDF is fetched from: the URL a submission carries.

A real extraction is a URL handed to Docling, and Docling fetches it — so the
URL has to be reachable from wherever Docling runs. Docling for IBM watsonx
takes only ``http`` sources (no upload: ``kind: file`` is a 422), so the
control app on localhost will not do. Three ways to name the PDF:

    local    the manifest's URL — this control app's /invoices. What every
             *synthetic* invoice carries: that extraction reads only the name.
    base     ``--base-url https://host/path`` — any public host you put the
             PDFs on.
    bucket   ``INVOICE_BUCKET=owner/name`` — a *private* Hugging Face Storage
             Bucket. The PDFs stay private; each URL handed out is a signed CDN
             URL, valid for an hour, that needs no credential to fetch.

The bucket one is how *real* invoices are released. Asking the Hub for a bucket
file with a token (``GET /buckets/<owner>/<name>/resolve/<path>``) answers 302
to its CDN with a URL signed for one hour — a presigned URL in all but name,
minted with nothing but the Hub token you already have. So the URL is minted
when the invoice is *released* (the clock, ``scripts/extract.py``), never
earlier: an hour is plenty for an extraction that starts within seconds, and
not enough to put a month of submissions on the topic ahead of time and still
have them fetchable. The token stays on the laptop; the topic only ever sees
the signed URL.

Upload once with ``scripts/extract.py upload``, which is ``hf buckets sync``.
"""

from __future__ import annotations

import os
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request

HUB = "https://huggingface.co"
#: The Hub signs for an hour; re-mint a little before that.
SIGNED_FOR = 55 * 60
PREFIX = "invoices"


def hf_token() -> str | None:
    """``HF_TOKEN``, else the token ``hf auth login`` saved."""
    token = os.environ.get("HF_TOKEN")
    if token:
        return token.strip()
    home = pathlib.Path(os.environ.get("HF_HOME", pathlib.Path.home() / ".cache" / "huggingface"))
    path = home / "token"
    return path.read_text(encoding="utf-8").strip() if path.is_file() else None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Sources:
    """``doc_id`` and mode -> the URL the submission carries.

    A real invoice gets a URL Docling can fetch — ``--base-url`` if given, else
    a signed URL into the bucket. A synthetic one keeps the manifest's.
    ``real_unavailable`` says why real mode cannot be offered, or is None.
    """

    def __init__(self, bucket: str | None = None, base_url: str | None = None):
        self.bucket = (bucket or "").strip().strip("/") or None
        self.base_url = (base_url or "").strip().rstrip("/") or None
        self._signed: dict[str, tuple[float, str]] = {}
        self._token = hf_token() if self.bucket and not self.base_url else None
        self._opener = urllib.request.build_opener(_NoRedirect)

    @classmethod
    def from_env(cls, env: dict[str, str], base_url: str | None = None) -> Sources:
        return cls(bucket=env.get("INVOICE_BUCKET"), base_url=base_url)

    @property
    def real_unavailable(self) -> str | None:
        if self.base_url:
            return None
        if not self.bucket:
            return "no INVOICE_BUCKET: Docling has nowhere to fetch the PDFs from"
        if not self._token:
            return f"no Hugging Face token to sign URLs into {self.bucket}: set HF_TOKEN, or `hf auth login`"
        return None

    def describe(self) -> str:
        why = self.real_unavailable
        if why:
            return f"real mode unavailable — {why}"
        if self.base_url:
            return f"real invoices from {self.base_url}"
        return f"real invoices from hf://buckets/{self.bucket}/{PREFIX}/ (signed URLs, 1 h)"

    def url(self, doc_id: str, manifest_url: str, real: bool = True) -> str:
        if not real:
            return manifest_url
        why = self.real_unavailable
        if why:
            raise RuntimeError(f"cannot release {doc_id} for a real extraction: {why}")
        if self.base_url:
            return f"{self.base_url}/{doc_id}.pdf"
        return self.signed(doc_id)

    def resolve_url(self, doc_id: str) -> str:
        """The Hub's own URL for the file — needs the token, so never on a topic."""
        return f"{HUB}/buckets/{self.bucket}/resolve/{PREFIX}/{urllib.parse.quote(doc_id)}.pdf"

    def signed(self, doc_id: str) -> str:
        cached = self._signed.get(doc_id)
        if cached and cached[0] > time.time():
            return cached[1]
        request = urllib.request.Request(self.resolve_url(doc_id), headers={"Authorization": f"Bearer {self._token}"})
        try:
            with self._opener.open(request, timeout=30) as response:
                raise RuntimeError(f"{doc_id}: the Hub answered {response.status}, not a redirect")
        except urllib.error.HTTPError as exc:  # a 302 surfaces here, because redirects are not followed
            location = exc.headers.get("Location") if exc.code in (301, 302, 303, 307, 308) else None
            if not location:
                raise RuntimeError(f"{doc_id}: the Hub answered {exc.code} for {self.resolve_url(doc_id)}") from exc
        self._signed[doc_id] = (time.time() + SIGNED_FOR, location)
        return location


def filename_of(url: str) -> str:
    """The PDF's file name, from a plain URL or from a signed CDN one.

    A signed bucket URL's path is a content hash; the name is in the
    ``response-content-disposition`` it asks the CDN to send back
    (``inline; filename*=UTF-8''lumen-inv-0042.pdf``).
    """
    parsed = urllib.parse.urlparse(url or "")
    disposition = urllib.parse.parse_qs(parsed.query).get("response-content-disposition", [""])[0]
    for part in disposition.split(";"):
        key, _, value = part.strip().partition("=")
        if key in ("filename*", "filename") and value:
            return urllib.parse.unquote(value.split("''", 1)[-1].strip('"'))
    return urllib.parse.unquote(parsed.path.rsplit("/", 1)[-1]) or (url or "")
