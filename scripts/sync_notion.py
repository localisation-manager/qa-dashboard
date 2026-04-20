#!/usr/bin/env python3
"""
Sync L10N QA testing scenario statuses from Notion to data/data.json.

Reads:
  - NOTION_TOKEN env var (Notion internal integration token, 'ntn_...')
  - Hardcoded database IDs for Fresha Partner and Marketplace

Writes:
  - data/data.json  (aggregated counts per language per platform)

Run locally:
  export NOTION_TOKEN=ntn_...
  python scripts/sync_notion.py

In CI (GitHub Actions):
  NOTION_TOKEN is read from a repo secret and passed in via env:.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

# --- Config: the two databases we sync ---------------------------------------
# NOTE: Use the database container ID (from the Notion URL), NOT the collection/
# data-source ID. Notion's public API attaches integration permissions to the
# container, and `/v1/databases/{id}/query` expects this container ID.
PARTNER_DB_ID = "a4ba8c62-6fbe-4311-949f-4f89ce4a8cd6"
MARKETPLACE_DB_ID = "031f5fda-b918-41ec-8ba2-9d31e185f22e"

# Language columns (these are `status` properties in Notion)
PARTNER_LANGS = [
    "AR", "BG", "DA", "DE", "EL", "EN-US", "ES", "FI", "FR", "HR", "HU",
    "ID ",  # trailing space matches the real Notion property name
    "IT", "JA", "KO", "MS", "NB", "NL", "PL", "PT", "PT-BR", "RO", "RU",
    "SV", "TH", "TR", "VI", "es-MX", "fr-CA", "zh-CN", "zh-HK",
]
MARKETPLACE_LANGS = [
    "AR", "BG", "DA", "DE", "EL", "ES", "FI", "FR", "HR", "HU",
    "ID ",
    "IT", "JA", "KO", "MS", "NB", "NL", "PL", "RO", "RU", "SV",
    "TH", "TR", "VI", "en-GB", "es-MX", "fr-CA", "pt-BR", "pt-PT",
    "zh-CN", "zh-HK",
]

# --- Status normalization ----------------------------------------------------
# Maps Notion's raw option names to our canonical buckets. Anything not matched
# (including empty / None) falls through to "Not started".
STATUS_MAP = {
    "Done": "Done",
    "In progress": "In progress",
    "Blocked": "Blocked",
    "Post-launch": "Post-launch",
    "Post launch": "Post-launch",
    "Repeat 🔁": "Repeat",
    "🔁 Repeat": "Repeat",
    "Not started": "Not started",
}
CANONICAL_STATUSES = ["Done", "In progress", "Blocked", "Post-launch", "Repeat", "Not started"]


def empty_counts() -> dict[str, int]:
    return {s: 0 for s in CANONICAL_STATUSES}


def normalize_status(raw: str | None) -> str:
    if not raw:
        return "Not started"
    return STATUS_MAP.get(raw, "Not started")


def display_code(col: str) -> str:
    """Clean up column names for display (strip trailing spaces like 'ID ')."""
    return col.strip()


# --- Notion client -----------------------------------------------------------

def notion_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def query_all_pages(token: str, database_id: str) -> list[dict[str, Any]]:
    """Page through the Notion database query endpoint and return every page."""
    url = f"{NOTION_API}/databases/{database_id}/query"
    results: list[dict[str, Any]] = []
    start_cursor: str | None = None
    with httpx.Client(timeout=30.0) as client:
        while True:
            body: dict[str, Any] = {"page_size": 100}
            if start_cursor:
                body["start_cursor"] = start_cursor
            resp = client.post(url, headers=notion_headers(token), json=body)
            if resp.status_code != 200:
                raise RuntimeError(
                    f"Notion query failed ({resp.status_code}) for db {database_id}: {resp.text[:500]}"
                )
            data = resp.json()
            results.extend(data.get("results", []))
            if data.get("has_more"):
                start_cursor = data.get("next_cursor")
            else:
                break
    return results


# --- Aggregation -------------------------------------------------------------

def extract_status(page: dict[str, Any], prop: str) -> str | None:
    """Return the option name of a status property on a page, or None."""
    props = page.get("properties", {})
    val = props.get(prop)
    if not val or val.get("type") != "status":
        return None
    status_obj = val.get("status")
    return status_obj.get("name") if status_obj else None


def aggregate(pages: list[dict[str, Any]], lang_cols: list[str]) -> list[dict[str, Any]]:
    """For each language column, tally canonical-status counts overall AND
    broken down by Feature/Area. Emits:
      {code, counts, by_feature: [{name, counts}, ...]}
    Pages without a Feature/Area are bucketed as "(No feature)".
    """
    out = []
    for col in lang_cols:
        overall = empty_counts()
        per_feature: dict[str, dict[str, int]] = {}
        for page in pages:
            status = normalize_status(extract_status(page, col))
            overall[status] += 1
            feat = extract_feature(page) or "(No feature)"
            bucket = per_feature.setdefault(feat, empty_counts())
            bucket[status] += 1
        by_feature = [
            {"name": name, "counts": counts}
            for name, counts in sorted(per_feature.items())
        ]
        out.append({
            "code": display_code(col),
            "counts": overall,
            "by_feature": by_feature,
        })
    return out


def extract_feature(page: dict[str, Any]) -> str | None:
    """Return the Feature/Area value of a page, or None if unset."""
    props = page.get("properties", {})
    val = props.get("Feature / Area")
    if not val or val.get("type") != "select":
        return None
    sel = val.get("select")
    return sel.get("name") if sel else None


def aggregate_features(pages: list[dict[str, Any]], lang_cols: list[str]) -> list[dict[str, Any]]:
    """For each Feature/Area, count scenarios and how many are 'fully done'
    (every language column marked Done).

    Scenarios without a Feature/Area are skipped.
    """
    total_langs = len(lang_cols)
    features: dict[str, dict[str, int]] = {}
    for page in pages:
        name = extract_feature(page)
        if not name:
            continue
        done_langs = sum(
            1 for col in lang_cols if normalize_status(extract_status(page, col)) == "Done"
        )
        fully_done = 1 if done_langs == total_langs and total_langs > 0 else 0
        bucket = features.setdefault(name, {"scenario_count": 0, "fully_done": 0})
        bucket["scenario_count"] += 1
        bucket["fully_done"] += fully_done
    return [
        {"name": name, "scenario_count": b["scenario_count"], "fully_done": b["fully_done"]}
        for name, b in sorted(features.items())
    ]


# --- Main --------------------------------------------------------------------

def main() -> int:
    token = os.environ.get("NOTION_TOKEN")
    if not token:
        print("ERROR: NOTION_TOKEN env var is required.", file=sys.stderr)
        return 2

    print("Querying Fresha Partner database…")
    partner_pages = query_all_pages(token, PARTNER_DB_ID)
    print(f"  → {len(partner_pages)} scenarios")

    print("Querying Marketplace database…")
    marketplace_pages = query_all_pages(token, MARKETPLACE_DB_ID)
    print(f"  → {len(marketplace_pages)} scenarios")

    data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": "notion",
        "partner": {
            "total_scenarios": len(partner_pages),
            "languages": aggregate(partner_pages, PARTNER_LANGS),
            "features": aggregate_features(partner_pages, PARTNER_LANGS),
        },
        "marketplace": {
            "total_scenarios": len(marketplace_pages),
            "languages": aggregate(marketplace_pages, MARKETPLACE_LANGS),
            "features": aggregate_features(marketplace_pages, MARKETPLACE_LANGS),
        },
    }

    out_path = Path(__file__).resolve().parent.parent / "data" / "data.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
