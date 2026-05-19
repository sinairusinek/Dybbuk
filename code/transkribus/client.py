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
from dataclasses import dataclass
from typing import Optional

import requests

DEFAULT_BASE = "https://transkribus.eu/TrpServer/rest"


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
    ) -> dict:
        """Upload a new PAGE-XML transcript layer to an existing page.

        Posts to `POST /collections/{col}/{doc}/{pageNr}/text` (legacy API).
        Returns the response JSON (typically the new tsId + url).
        """
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
