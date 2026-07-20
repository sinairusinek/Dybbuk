"""Legacy Transkribus TrpServer REST client.

Auth: password grant (`POST /auth/login`) returns a JSESSIONID cookie that the
session keeps for subsequent calls. This is the *legacy* API (collections,
docs, pages, transcripts) — separate from Metagrapho HTR.

Endpoints used:
  POST /auth/login                              -> session cookie
  GET  /collections/list                        -> all collections the user can see
  GET  /collections/{cid}/list                  -> docs in a collection
  GET  /collections/{cid}/{did}/fulldoc         -> doc with pages + transcripts
  GET  <transcript.url>                         -> PAGE-XML
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional

import requests

DEFAULT_BASE = "https://transkribus.eu/TrpServer/rest"


class SpanLossError(RuntimeError):
    """A push would destroy most of the live annotation. See _guard_span_loss."""


@dataclass
class TrpClient:
    user: str
    password: str
    base: str = DEFAULT_BASE
    session: requests.Session = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    @classmethod
    def from_env(cls, base: Optional[str] = None) -> "TrpClient":
        try:
            user = os.environ["TRANSKRIBUS_USER"]
            pw = os.environ["TRANSKRIBUS_PASS"]
        except KeyError as e:
            raise SystemExit(f"Missing env var {e}. Set TRANSKRIBUS_USER and TRANSKRIBUS_PASS.")
        c = cls(user=user, password=pw, base=base or DEFAULT_BASE)
        c.login()
        return c

    def login(self) -> None:
        resp = self.session.post(
            f"{self.base}/auth/login",
            data={"user": self.user, "pw": self.password},
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Login failed ({resp.status_code}): {resp.text[:200]}")

    def list_collections(self) -> list[dict]:
        r = self.session.get(f"{self.base}/collections/list", timeout=30)
        r.raise_for_status()
        return r.json()

    def list_docs(self, col_id: int) -> list[dict]:
        r = self.session.get(f"{self.base}/collections/{col_id}/list", timeout=60)
        r.raise_for_status()
        return r.json()

    def fulldoc(self, col_id: int, doc_id: int) -> dict:
        r = self.session.get(
            f"{self.base}/collections/{col_id}/{doc_id}/fulldoc", timeout=120
        )
        r.raise_for_status()
        return r.json()

    def fetch_transcript(self, url: str) -> str:
        r = self.session.get(url, timeout=60)
        r.raise_for_status()
        return r.text

    def fetch_image(self, url: str) -> bytes:
        """Download a page image. `url` is the per-page `url` field from fulldoc
        (e.g. https://files.transkribus.eu/Get?fileType=view&id=...)."""
        r = self.session.get(url, timeout=120)
        r.raise_for_status()
        return r.content

    def page_image_map(self, col_id: int, doc_id: int) -> dict[int, dict]:
        """Map pageNr -> {url, imgFileName, width, height} for a doc."""
        fd = self.fulldoc(col_id, doc_id)
        pages = fd.get("pageList", {}).get("pages", [])
        out: dict[int, dict] = {}
        for p in pages:
            out[int(p["pageNr"])] = {
                "url": p.get("url"),
                "imgFileName": p.get("imgFileName"),
                "width": p.get("width"),
                "height": p.get("height"),
            }
        return out

    _SPAN_RX = re.compile(r"(\w+)\s*\{")
    MIN_SPANS_TO_GUARD = 5      # below this, a page has too few spans to judge
    LOSS_RATIO = 0.5            # blocked when more than half the spans vanish

    @staticmethod
    def _count_spans(page_xml: str) -> int:
        """Non-readingOrder spans across all TextLine @custom in a PAGE-XML."""
        n = 0
        for m in re.finditer(r'custom="([^"]*)"', page_xml):
            for tag in TrpClient._SPAN_RX.findall(m.group(1)):
                if tag != "readingOrder":
                    n += 1
        return n

    def _guard_span_loss(self, col_id: int, doc_id: int, page_nr: int,
                         page_xml: str) -> None:
        """Raise SpanLossError if this push would wipe most of the live spans."""
        try:
            doc = self.fulldoc(col_id, doc_id)
            page = next(p for p in doc["pageList"]["pages"]
                        if p["pageNr"] == page_nr)
            live = self.fetch_transcript(page["tsList"]["transcripts"][0]["url"])
        except Exception:
            return          # never block a push because the check itself failed
        before, after = self._count_spans(live), self._count_spans(page_xml)
        if before >= self.MIN_SPANS_TO_GUARD and after < before * self.LOSS_RATIO:
            raise SpanLossError(
                f"refusing to push doc {doc_id} p{page_nr}: it would drop "
                f"{before - after} of {before} annotation spans (live={before}, "
                f"payload={after}). This is the signature of a payload built "
                f"from a stale local copy rather than the live transcript. "
                f"Refresh from live first (transkribus.refresh_page_annotated), "
                f"or pass allow_span_loss=True if the removal is intended."
            )

    def push_transcript(
        self,
        col_id: int,
        doc_id: int,
        page_nr: int,
        page_xml: str,
        *,
        parent_tsid: Optional[int] = None,
        status: str = "IN_PROGRESS",
        note: Optional[str] = None,
        tool_name: str = "YiDraCor-annotation-pipeline",
        allow_span_loss: bool = False,
    ) -> dict:
        """Upload a new PAGE-XML transcript layer to an existing page.

        Posts to `POST /collections/{col}/{doc}/{pageNr}/text` (legacy API).
        Returns the response JSON (typically the new tsId + url).

        Refuses a WHOLESALE span loss unless `allow_span_loss=True`. Sinai
        2026-07-20: BasSheva p8 lost all 32 of Noa's `l` spans to a push built
        from a stale `page_annotated/` mirror, and nothing noticed for nine days
        — lint validates that spans are well-formed, never that spans which used
        to exist still do. The exact tool behind that push is still
        unidentified, which is precisely why the guard sits here, at the choke
        point every push goes through, rather than in any one tool.

        The check is deliberately wholesale-only, because legitimate passes DO
        remove spans — retag_musical_directions §2b strips `l` from whole-line
        stage directions, the pageNum sweep replaces `l` with `fw`. Losing a few
        spans is normal; losing nearly all of them means the payload was built
        from something other than the current transcript.
        """
        if not allow_span_loss and parent_tsid is not None:
            self._guard_span_loss(col_id, doc_id, page_nr, page_xml)
        params: dict = {"toolName": tool_name, "status": status}
        if parent_tsid is not None:
            params["parent"] = parent_tsid
        if note is not None:
            params["note"] = note
        r = self.session.post(
            f"{self.base}/collections/{col_id}/{doc_id}/{page_nr}/text",
            params=params,
            data=page_xml.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=120,
        )
        if r.status_code >= 300:
            raise RuntimeError(f"Push failed ({r.status_code}): {r.text[:400]}")
        try:
            return r.json()
        except ValueError:
            return {"status_code": r.status_code, "text": r.text[:400]}

    def delete_doc(self, col_id: int, doc_id: int) -> dict:
        """Remove a document from a collection (DELETE /collections/{cid}/{did}).

        If the doc exists in other collections too, this is a per-collection
        removal. If the collection is the doc's only home, the doc is fully
        deleted from Transkribus. Caller is responsible for confirming intent.
        """
        # Try standard endpoints in order until one works.
        endpoints = [
            ("DELETE", f"{self.base}/collections/{col_id}/{doc_id}", {}),
            ("POST", f"{self.base}/collections/{col_id}/removeDocFromCol",
                {"params": {"id": doc_id}}),
            ("DELETE", f"{self.base}/collections/{col_id}/list",
                {"params": {"id": doc_id}}),
        ]
        last_err = None
        for method, url, kwargs in endpoints:
            r = self.session.request(method, url, timeout=60, **kwargs)
            if r.status_code < 300:
                return {"endpoint": url, "status_code": r.status_code,
                        "text": r.text[:200]}
            last_err = f"{method} {url} → {r.status_code}: {r.text[:200]}"
        raise RuntimeError(f"All delete attempts failed. Last: {last_err}")
