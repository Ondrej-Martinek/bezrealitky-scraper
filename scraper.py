#!/usr/bin/env python3
"""Bezrealitky scraper with sitemap discovery, Postgres persistence and ntfy alerts."""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

import psycopg
from psycopg.rows import dict_row

SITEMAP_INDEX_URL = "https://www.bezrealitky.cz/sitemap/sitemap.xml"
DEFAULT_TIMEOUT = 20
NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.DOTALL,
)

TRACKED_FIELDS = [
    "price_czk",
    "charges_czk",
    "total_monthly_czk",
    "deposit_czk",
    "surface_m2",
    "available_from_utc",
    "description",
    "is_active",
]


@dataclass
class SitemapEntry:
    loc: str
    lastmod: Optional[str]
    caption: Optional[str]
    geo_location: Optional[str]


@dataclass
class PersistResult:
    new_listings: list[dict]
    price_drops: list[dict]
    removed_listings: list[dict]
    changed_listings: list[dict]


class NtfyClient:
    def __init__(self, topic: str, base_url: str = "https://ntfy.sh", token: Optional[str] = None) -> None:
        self.topic = topic
        self.base_url = base_url.rstrip("/")
        self.token = token

    def send(self, message: str, title: str, tags: str = "house", priority: str = "3") -> None:
        url = f"{self.base_url}/{self.topic}"
        req = Request(url, data=message.encode("utf-8"), method="POST")
        req.add_header("Title", title)
        req.add_header("Tags", tags)
        req.add_header("Priority", priority)
        if self.token:
            req.add_header("Authorization", f"Bearer {self.token}")
        with urlopen(req, timeout=DEFAULT_TIMEOUT):
            return


class Database:
    def __init__(self, dsn: str) -> None:
        self.conn = psycopg.connect(dsn)
        self.conn.autocommit = False

    def close(self) -> None:
        self.conn.close()

    def migrate(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS scrape_runs (
                    id BIGSERIAL PRIMARY KEY,
                    city TEXT NOT NULL,
                    offer_type TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    estate_types JSONB NOT NULL,
                    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    finished_at TIMESTAMPTZ,
                    status TEXT NOT NULL DEFAULT 'RUNNING',
                    found_urls INTEGER NOT NULL DEFAULT 0,
                    scraped_count INTEGER NOT NULL DEFAULT 0,
                    error_count INTEGER NOT NULL DEFAULT 0,
                    output_dir TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS listings (
                    listing_id TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    uri TEXT,
                    city TEXT,
                    offer_type TEXT,
                    estate_type TEXT,
                    disposition TEXT,
                    surface_m2 NUMERIC,
                    price_czk NUMERIC,
                    charges_czk NUMERIC,
                    deposit_czk NUMERIC,
                    total_monthly_czk NUMERIC,
                    currency TEXT,
                    available_from_utc TIMESTAMPTZ,
                    address TEXT,
                    description TEXT,
                    details_json JSONB NOT NULL,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    last_run_id BIGINT REFERENCES scrape_runs(id)
                );
                """
            )
            cur.execute("ALTER TABLE listings ADD COLUMN IF NOT EXISTS description TEXT")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS listing_scope_state (
                    scope_key TEXT NOT NULL,
                    listing_id TEXT NOT NULL REFERENCES listings(listing_id) ON DELETE CASCADE,
                    first_seen_at TIMESTAMPTZ NOT NULL,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    PRIMARY KEY (scope_key, listing_id)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS listing_history (
                    id BIGSERIAL PRIMARY KEY,
                    listing_id TEXT NOT NULL REFERENCES listings(listing_id) ON DELETE CASCADE,
                    scope_key TEXT NOT NULL,
                    run_id BIGINT NOT NULL REFERENCES scrape_runs(id),
                    event_type TEXT NOT NULL,
                    old_values JSONB,
                    new_values JSONB,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS notifications (
                    id BIGSERIAL PRIMARY KEY,
                    dedupe_key TEXT UNIQUE NOT NULL,
                    channel TEXT NOT NULL,
                    target TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
        self.conn.commit()

    def create_run(self, city: str, offer_type: str, scope_key: str, estate_types: list[str], output_dir: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scrape_runs(city, offer_type, scope_key, estate_types, output_dir)
                VALUES (%s, %s, %s, %s::jsonb, %s)
                RETURNING id
                """,
                (city, offer_type, scope_key, json.dumps(estate_types), output_dir),
            )
            run_id = cur.fetchone()[0]
        self.conn.commit()
        return run_id

    def finish_run(
        self,
        run_id: int,
        status: str,
        found_urls: int,
        scraped_count: int,
        error_count: int,
        metadata: dict,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE scrape_runs
                SET finished_at = NOW(),
                    status = %s,
                    found_urls = %s,
                    scraped_count = %s,
                    error_count = %s,
                    metadata = %s::jsonb
                WHERE id = %s
                """,
                (status, found_urls, scraped_count, error_count, json.dumps(metadata), run_id),
            )
        self.conn.commit()

    def notification_sent(self, dedupe_key: str) -> bool:
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM notifications WHERE dedupe_key = %s", (dedupe_key,))
            return cur.fetchone() is not None

    def record_notification(self, dedupe_key: str, channel: str, target: str, payload: dict) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO notifications(dedupe_key, channel, target, payload)
                VALUES (%s, %s, %s, %s::jsonb)
                ON CONFLICT (dedupe_key) DO NOTHING
                """,
                (dedupe_key, channel, target, json.dumps(payload)),
            )
        self.conn.commit()

    def persist_listings(self, run_id: int, scope_key: str, listings: list[dict]) -> PersistResult:
        now = datetime.now(timezone.utc)
        new_listings: list[dict] = []
        price_drops: list[dict] = []
        removed_listings: list[dict] = []
        changed_listings: list[dict] = []

        with self.conn.cursor(row_factory=dict_row) as cur:
            for listing in listings:
                listing_id = str(listing["id"])
                listing["id"] = listing_id

                cur.execute("SELECT * FROM listings WHERE listing_id = %s", (listing_id,))
                existing = cur.fetchone()

                details_json = json.dumps(listing, ensure_ascii=False)
                available_from = listing.get("available_from_utc")
                if existing is None:
                    cur.execute(
                        """
                        INSERT INTO listings (
                            listing_id, url, uri, city, offer_type, estate_type, disposition,
                            surface_m2, price_czk, charges_czk, deposit_czk, total_monthly_czk,
                            currency, available_from_utc, address, description, details_json,
                            first_seen_at, last_seen_at, is_active, last_run_id
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s::jsonb,
                            %s, %s, TRUE, %s
                        )
                        """,
                        (
                            listing_id,
                            listing.get("url"),
                            listing.get("uri"),
                            listing.get("city"),
                            listing.get("offer_type"),
                            listing.get("estate_type"),
                            listing.get("disposition"),
                            listing.get("surface_m2"),
                            listing.get("price_czk"),
                            listing.get("charges_czk"),
                            listing.get("deposit_czk"),
                            listing.get("total_monthly_czk"),
                            listing.get("currency"),
                            available_from,
                            listing.get("address"),
                            listing.get("description"),
                            details_json,
                            now,
                            now,
                            run_id,
                        ),
                    )
                    cur.execute(
                        """
                        INSERT INTO listing_history(listing_id, scope_key, run_id, event_type, old_values, new_values)
                        VALUES (%s, %s, %s, 'NEW', NULL, %s::jsonb)
                        """,
                        (listing_id, scope_key, run_id, details_json),
                    )
                    new_listings.append(listing)
                else:
                    changed = {}
                    for key in TRACKED_FIELDS:
                        old_value = existing.get(key)
                        new_value = listing.get(key)
                        if comparable_value(old_value) != comparable_value(new_value):
                            changed[key] = {"old": old_value, "new": new_value}

                    cur.execute(
                        """
                        UPDATE listings
                        SET url = %s,
                            uri = %s,
                            city = %s,
                            offer_type = %s,
                            estate_type = %s,
                            disposition = %s,
                            surface_m2 = %s,
                            price_czk = %s,
                            charges_czk = %s,
                            deposit_czk = %s,
                            total_monthly_czk = %s,
                            currency = %s,
                            available_from_utc = %s,
                            address = %s,
                            description = %s,
                            details_json = %s::jsonb,
                            last_seen_at = %s,
                            is_active = TRUE,
                            last_run_id = %s
                        WHERE listing_id = %s
                        """,
                        (
                            listing.get("url"),
                            listing.get("uri"),
                            listing.get("city"),
                            listing.get("offer_type"),
                            listing.get("estate_type"),
                            listing.get("disposition"),
                            listing.get("surface_m2"),
                            listing.get("price_czk"),
                            listing.get("charges_czk"),
                            listing.get("deposit_czk"),
                            listing.get("total_monthly_czk"),
                            listing.get("currency"),
                            available_from,
                            listing.get("address"),
                            listing.get("description"),
                            details_json,
                            now,
                            run_id,
                            listing_id,
                        ),
                    )

                    if changed:
                        cur.execute(
                            """
                            INSERT INTO listing_history(listing_id, scope_key, run_id, event_type, old_values, new_values)
                            VALUES (%s, %s, %s, 'UPDATED', %s::jsonb, %s::jsonb)
                            """,
                            (
                                listing_id,
                                scope_key,
                                run_id,
                                json.dumps({k: v["old"] for k, v in changed.items()}),
                                json.dumps({k: v["new"] for k, v in changed.items()}),
                            ),
                        )
                        changed_listings.append({"listing": listing, "changes": changed})

                    old_price = as_number(existing.get("price_czk"))
                    new_price = as_number(listing.get("price_czk"))
                    if old_price is not None and new_price is not None and new_price < old_price:
                        price_drops.append(
                            {
                                "listing": listing,
                                "old_price": old_price,
                                "new_price": new_price,
                                "delta": old_price - new_price,
                            }
                        )

                cur.execute(
                    """
                    INSERT INTO listing_scope_state(scope_key, listing_id, first_seen_at, last_seen_at, is_active)
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT (scope_key, listing_id)
                    DO UPDATE SET last_seen_at = EXCLUDED.last_seen_at, is_active = TRUE
                    """,
                    (scope_key, listing_id, now, now),
                )

            current_ids = [str(item["id"]) for item in listings]
            if current_ids:
                cur.execute(
                    """
                    SELECT lss.listing_id, l.url, l.price_czk, l.address
                    FROM listing_scope_state lss
                    JOIN listings l ON l.listing_id = lss.listing_id
                    WHERE lss.scope_key = %s
                      AND lss.is_active = TRUE
                      AND lss.listing_id <> ALL(%s)
                    """,
                    (scope_key, current_ids),
                )
            else:
                cur.execute(
                    """
                    SELECT lss.listing_id, l.url, l.price_czk, l.address
                    FROM listing_scope_state lss
                    JOIN listings l ON l.listing_id = lss.listing_id
                    WHERE lss.scope_key = %s AND lss.is_active = TRUE
                    """,
                    (scope_key,),
                )
            removed = cur.fetchall()

            for row in removed:
                listing_id = row["listing_id"]
                cur.execute(
                    "UPDATE listing_scope_state SET is_active = FALSE WHERE scope_key = %s AND listing_id = %s",
                    (scope_key, listing_id),
                )
                cur.execute(
                    """
                    UPDATE listings l
                    SET is_active = EXISTS (
                        SELECT 1 FROM listing_scope_state s
                        WHERE s.listing_id = l.listing_id AND s.is_active = TRUE
                    )
                    WHERE l.listing_id = %s
                    """,
                    (listing_id,),
                )
                cur.execute(
                    """
                    INSERT INTO listing_history(listing_id, scope_key, run_id, event_type, old_values, new_values)
                    VALUES (%s, %s, %s, 'REMOVED', NULL, NULL)
                    """,
                    (listing_id, scope_key, run_id),
                )
                removed_listings.append(
                    {
                        "id": listing_id,
                        "url": row["url"],
                        "price_czk": row["price_czk"],
                        "address": row["address"],
                    }
                )

        self.conn.commit()
        return PersistResult(
            new_listings=new_listings,
            price_drops=price_drops,
            removed_listings=removed_listings,
            changed_listings=changed_listings,
        )


def comparable_value(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    return value


def as_number(value: object) -> Optional[float]:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def normalize_text(text: str) -> str:
    no_diacritics = "".join(
        char for char in unicodedata.normalize("NFKD", text) if not unicodedata.combining(char)
    )
    return no_diacritics.casefold().strip()


def slugify_city(city: str) -> str:
    slug = normalize_text(city)
    slug = re.sub(r"[^a-z0-9]+", "-", slug).strip("-")
    return slug


def fetch_text(url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; BezrealitkyScraper/1.0; +https://example.com)",
            "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
        },
    )
    with urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_sitemap_index(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    return [
        node.text.strip()
        for node in root.findall("sm:sitemap/sm:loc", namespace)
        if node.text and node.text.strip()
    ]


def parse_urlset(xml_text: str) -> list[SitemapEntry]:
    entries: list[SitemapEntry] = []
    for url_block in re.findall(r"<url>(.*?)</url>", xml_text, flags=re.DOTALL):
        loc_match = re.search(r"<loc>(.*?)</loc>", url_block, flags=re.DOTALL)
        if not loc_match:
            continue
        lastmod_match = re.search(r"<lastmod>(.*?)</lastmod>", url_block, flags=re.DOTALL)
        caption_match = re.search(r"<image:caption>(.*?)</image:caption>", url_block, flags=re.DOTALL)
        geo_match = re.search(r"<image:geo_location>(.*?)</image:geo_location>", url_block, flags=re.DOTALL)
        entries.append(
            SitemapEntry(
                loc=html.unescape(loc_match.group(1)).strip(),
                lastmod=html.unescape(lastmod_match.group(1)).strip() if lastmod_match else None,
                caption=html.unescape(caption_match.group(1)).strip() if caption_match else None,
                geo_location=html.unescape(geo_match.group(1)).strip() if geo_match else None,
            )
        )
    return entries


def is_target_listing(
    entry: SitemapEntry,
    city: str,
    city_slug: str,
    offer_type: str,
    estate_types: set[str],
) -> bool:
    url_lower = entry.loc.casefold()
    if f"nabidka-{offer_type}-" not in url_lower:
        return False

    estate_type_match = any(f"-{estate}-" in url_lower for estate in estate_types)
    if not estate_type_match:
        return False

    city_norm = normalize_text(city)
    in_url = city_slug in url_lower
    in_caption = bool(entry.caption and city_norm in normalize_text(entry.caption))
    in_geo = bool(entry.geo_location and city_norm in normalize_text(entry.geo_location))
    return in_url or in_caption or in_geo


def extract_next_data(html_doc: str) -> dict:
    match = NEXT_DATA_RE.search(html_doc)
    if not match:
        raise ValueError("__NEXT_DATA__ script not found")
    return json.loads(match.group(1))


def ts_to_iso(value: Optional[int]) -> Optional[str]:
    if not value:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def extract_listing_details(url: str, html_doc: str) -> dict:
    data = extract_next_data(html_doc)
    page_props = data.get("props", {}).get("pageProps", {})
    advert = page_props.get("origAdvert")
    if not isinstance(advert, dict):
        raise ValueError("origAdvert not found in __NEXT_DATA__")

    price = advert.get("price")
    charges = advert.get("charges")
    deposit = advert.get("deposit")

    return {
        "url": url,
        "id": str(advert.get("id")),
        "uri": advert.get("uri"),
        "offer_type": advert.get("offerType"),
        "estate_type": advert.get("estateType"),
        "disposition": advert.get("disposition"),
        "surface_m2": advert.get("surface"),
        "price_czk": price,
        "charges_czk": charges,
        "deposit_czk": deposit,
        "total_monthly_czk": (price + charges) if isinstance(price, (int, float)) and isinstance(charges, (int, float)) else None,
        "currency": advert.get("currency"),
        "available_from_utc": ts_to_iso(advert.get("availableFrom")),
        "address": advert.get("address"),
        "city": advert.get("city"),
        "street": advert.get("street"),
        "zip": advert.get("zip"),
        "region_tree": [item.get("name") for item in advert.get("regionTree", []) if isinstance(item, dict)],
        "roommate": advert.get("roommate"),
        "equipped": advert.get("equipped"),
        "condition": advert.get("condition"),
        "construction": advert.get("construction"),
        "floor": advert.get("floor"),
        "etage": advert.get("etage"),
        "total_floors": advert.get("totalFloors"),
        "balcony": advert.get("balcony"),
        "terrace": advert.get("terrace"),
        "loggia": advert.get("loggia"),
        "parking": advert.get("parking"),
        "cellar": advert.get("cellar"),
        "garage": advert.get("garage"),
        "gps": advert.get("gps"),
        "description": advert.get("description"),
        "is_active": True,
    }


def scrape_listing(url: str, delay_s: float) -> dict:
    html_doc = fetch_text(url)
    if delay_s > 0:
        time.sleep(delay_s)
    return extract_listing_details(url, html_doc)


def write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(path: Path, rows: Iterable[dict]) -> None:
    rows = list(rows)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def should_notify(db: Optional[Database], dedupe_key: str) -> bool:
    if db is None:
        return True
    return not db.notification_sent(dedupe_key)


def record_notify(db: Optional[Database], dedupe_key: str, topic: str, payload: dict) -> None:
    if db is None:
        return
    db.record_notification(dedupe_key=dedupe_key, channel="ntfy", target=topic, payload=payload)


def send_ntfy_alerts(
    client: Optional[NtfyClient],
    db: Optional[Database],
    scope_key: str,
    run_id: int,
    persist: Optional[PersistResult],
    scraped_count: int,
    error_count: int,
) -> None:
    if client is None:
        return

    if persist is None:
        title = f"Bezrealitky run {scope_key}"
        message = f"Run #{run_id}: scraped={scraped_count}, errors={error_count}"
        client.send(message=message, title=title, tags="house,information")
        return

    event_key_payload = {
        "scope_key": scope_key,
        "new": sorted(item["id"] for item in persist.new_listings),
        "drops": sorted(item["listing"]["id"] for item in persist.price_drops),
        "removed": sorted(item["id"] for item in persist.removed_listings),
    }
    event_hash = hashlib.sha1(json.dumps(event_key_payload, sort_keys=True).encode("utf-8")).hexdigest()
    dedupe_key = f"ntfy:{scope_key}:{event_hash}"

    if event_key_payload["new"] or event_key_payload["drops"] or event_key_payload["removed"]:
        if should_notify(db, dedupe_key):
            lines = [
                f"Run #{run_id} for {scope_key}",
                f"New listings: {len(persist.new_listings)}",
                f"Price drops: {len(persist.price_drops)}",
                f"Removed: {len(persist.removed_listings)}",
                f"Scraped: {scraped_count}, Errors: {error_count}",
            ]
            for item in persist.new_listings[:5]:
                lines.append(f"NEW {item.get('price_czk')} CZK | {item.get('address')} | {item.get('url')}")
            for item in persist.price_drops[:5]:
                listing = item["listing"]
                lines.append(
                    f"DROP {item['old_price']} -> {item['new_price']} CZK | {listing.get('address')} | {listing.get('url')}"
                )
            for item in persist.removed_listings[:5]:
                lines.append(f"REMOVED | {item.get('address')} | {item.get('url')}")

            client.send(
                message="\n".join(lines),
                title=f"Bezrealitky changes: {scope_key}",
                tags="house,warning",
                priority="4",
            )
            record_notify(db, dedupe_key, client.topic, event_key_payload)

    summary_key = f"ntfy:{scope_key}:run:{run_id}"
    if should_notify(db, summary_key):
        summary = (
            f"Run #{run_id} completed\n"
            f"Scope: {scope_key}\n"
            f"Scraped: {scraped_count}\n"
            f"Errors: {error_count}\n"
            f"New: {len(persist.new_listings)} | Drops: {len(persist.price_drops)} | Removed: {len(persist.removed_listings)}"
        )
        client.send(message=summary, title=f"Bezrealitky summary: {scope_key}", tags="house,information", priority="3")
        record_notify(db, summary_key, client.topic, {"run_id": run_id, "scope_key": scope_key})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape bezrealitky listings, persist to Postgres, and send ntfy alerts.")
    parser.add_argument("--city", default=os.getenv("BZR_CITY", "Hostivice"), help="City/area to target")
    parser.add_argument("--offer", default=os.getenv("BZR_OFFER", "pronajem"), choices=["pronajem", "prodej"], help="Offer type in URL slug")
    parser.add_argument(
        "--estate-types",
        nargs="+",
        default=os.getenv("BZR_ESTATE_TYPES", "bytu domu").split(),
        help="Estate URL tokens to include",
    )
    parser.add_argument("--max-listings", type=int, default=int(os.getenv("BZR_MAX_LISTINGS", "0")), help="Limit number of listings")
    parser.add_argument("--workers", type=int, default=int(os.getenv("BZR_WORKERS", "4")), help="Parallel workers")
    parser.add_argument("--delay", type=float, default=float(os.getenv("BZR_DELAY", "0")), help="Delay in seconds")
    parser.add_argument("--out-dir", default=os.getenv("BZR_OUT_DIR", "output"), help="Output directory")
    parser.add_argument("--db-dsn", default=os.getenv("DB_DSN"), help="PostgreSQL DSN")
    parser.add_argument("--ntfy-topic", default=os.getenv("NTFY_TOPIC"), help="ntfy topic name")
    parser.add_argument("--ntfy-base-url", default=os.getenv("NTFY_BASE_URL", "https://ntfy.sh"), help="ntfy base URL")
    parser.add_argument("--ntfy-token", default=os.getenv("NTFY_TOKEN"), help="Optional ntfy bearer token")
    parser.add_argument("--no-notify", action="store_true", help="Disable ntfy notifications")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    city = args.city
    city_slug = slugify_city(city)
    estate_types = {e.casefold() for e in args.estate_types}
    scope_key = f"{city_slug}:{args.offer}:{','.join(sorted(estate_types))}"

    db: Optional[Database] = None
    run_id: int = 0
    persist_result: Optional[PersistResult] = None

    try:
        if args.db_dsn:
            db = Database(args.db_dsn)
            db.migrate()
            run_id = db.create_run(
                city=city,
                offer_type=args.offer,
                scope_key=scope_key,
                estate_types=sorted(estate_types),
                output_dir=str(out_dir),
            )
            print(f"Database connected. run_id={run_id}")
        else:
            print("DB_DSN not set, skipping database persistence")

        print(f"[1/4] Fetching sitemap index: {SITEMAP_INDEX_URL}")
        sitemap_index_xml = fetch_text(SITEMAP_INDEX_URL)
        sitemap_urls = parse_sitemap_index(sitemap_index_xml)

        detail_sitemaps = [url for url in sitemap_urls if "sitemap_detail_" in url]
        if not detail_sitemaps:
            raise RuntimeError("No detail sitemaps found in sitemap index")
        print(f"Found {len(detail_sitemaps)} detail sitemap files")

        print(f"[2/4] Filtering city='{city}', offer='{args.offer}', estate={sorted(estate_types)}")
        matched_entries: list[SitemapEntry] = []
        for sitemap_url in detail_sitemaps:
            xml_text = fetch_text(sitemap_url)
            entries = parse_urlset(xml_text)
            matched_entries.extend(
                entry
                for entry in entries
                if is_target_listing(entry, city=city, city_slug=city_slug, offer_type=args.offer, estate_types=estate_types)
            )

        unique_by_url: dict[str, SitemapEntry] = {entry.loc: entry for entry in matched_entries}
        filtered_entries = sorted(unique_by_url.values(), key=lambda item: item.loc)
        if args.max_listings > 0:
            filtered_entries = filtered_entries[: args.max_listings]

        urls_path = out_dir / f"{city_slug}_{args.offer}_urls.txt"
        urls_path.write_text("\n".join(entry.loc for entry in filtered_entries), encoding="utf-8")
        write_json(out_dir / f"{city_slug}_{args.offer}_sitemap_matches.json", [entry.__dict__ for entry in filtered_entries])

        print(f"Matched {len(filtered_entries)} listing URLs -> {urls_path}")

        print("[3/4] Scraping listing details")
        details: list[dict] = []
        errors: list[dict] = []

        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            future_map = {executor.submit(scrape_listing, entry.loc, args.delay): entry for entry in filtered_entries}
            for future in as_completed(future_map):
                entry = future_map[future]
                try:
                    details.append(future.result())
                except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
                    errors.append({"url": entry.loc, "error": str(exc)})

        details.sort(key=lambda item: str(item.get("url", "")))

        details_json = out_dir / f"{city_slug}_{args.offer}_details.json"
        details_csv = out_dir / f"{city_slug}_{args.offer}_details.csv"
        errors_json = out_dir / f"{city_slug}_{args.offer}_errors.json"

        write_json(details_json, details)
        write_csv(details_csv, details)
        write_json(errors_json, errors)

        if db is not None:
            persist_result = db.persist_listings(run_id=run_id, scope_key=scope_key, listings=details)
            db.finish_run(
                run_id=run_id,
                status="SUCCESS",
                found_urls=len(filtered_entries),
                scraped_count=len(details),
                error_count=len(errors),
                metadata={
                    "new_listings": len(persist_result.new_listings),
                    "price_drops": len(persist_result.price_drops),
                    "removed": len(persist_result.removed_listings),
                    "changed": len(persist_result.changed_listings),
                },
            )

        ntfy_client = None
        if not args.no_notify and args.ntfy_topic:
            ntfy_client = NtfyClient(topic=args.ntfy_topic, base_url=args.ntfy_base_url, token=args.ntfy_token)

        send_ntfy_alerts(
            client=ntfy_client,
            db=db,
            scope_key=scope_key,
            run_id=run_id,
            persist=persist_result,
            scraped_count=len(details),
            error_count=len(errors),
        )

        print("[4/4] Done")
        print(f"Details: {details_json}")
        print(f"CSV:     {details_csv}")
        print(f"Errors:  {errors_json} ({len(errors)} errors)")
        if persist_result is not None:
            print(
                "DB changes: "
                f"new={len(persist_result.new_listings)}, "
                f"drops={len(persist_result.price_drops)}, "
                f"removed={len(persist_result.removed_listings)}, "
                f"changed={len(persist_result.changed_listings)}"
            )
        return 0

    except Exception:
        if db is not None and run_id:
            db.finish_run(
                run_id=run_id,
                status="FAILED",
                found_urls=0,
                scraped_count=0,
                error_count=1,
                metadata={"exception": True},
            )
        raise
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())
