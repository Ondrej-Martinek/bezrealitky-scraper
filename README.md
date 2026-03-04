<<<<<<< HEAD
# bezrealitky-web-scraper
Web scraper of listings on Bezrealitky portal
=======
# Bezrealitky scraper

Sitemap-based scraper for `bezrealitky.cz` with:
- listing discovery
- detail extraction
- PostgreSQL persistence + change history
- ntfy alerts
- scheduled GitHub Actions deployment

## Features

1. Fetches sitemap index from `https://www.bezrealitky.cz/sitemap/sitemap.xml`
2. Parses `sitemap_detail_*.xml` and filters by city/offer/estate type
3. Scrapes listing detail pages and extracts data from `__NEXT_DATA__`
4. Stores current snapshot + history in PostgreSQL
5. Detects and tracks:
- new listings
- updated listings
- price drops
- removed listings (from selected scope)
6. Sends notifications to `ntfy`

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Local run

```bash
export DB_DSN='postgresql://user:pass@host:5432/dbname?sslmode=require'
export NTFY_TOPIC='your-topic'
# optional
export NTFY_TOKEN='your-ntfy-token'
export NTFY_BASE_URL='https://ntfy.sh'

python scraper.py \
  --city Hostivice \
  --offer pronajem \
  --estate-types bytu domu \
  --out-dir output
```

If `DB_DSN` is missing, scraper still runs and writes output files, but DB persistence is skipped.

If `NTFY_TOPIC` is missing (or `--no-notify` is used), notifications are skipped.

## CLI options

```bash
python scraper.py \
  --city Hostivice \
  --offer pronajem \
  --estate-types bytu domu \
  --max-listings 50 \
  --workers 4 \
  --delay 0.2 \
  --db-dsn "$DB_DSN" \
  --ntfy-topic "$NTFY_TOPIC" \
  --ntfy-base-url "https://ntfy.sh" \
  --out-dir output
```

## Database schema

Tables are created automatically by the scraper:
- `scrape_runs`: run metadata/status
- `listings`: latest listing snapshot
- `listing_scope_state`: active/inactive state per query scope
- `listing_history`: NEW/UPDATED/REMOVED events
- `notifications`: sent notification dedupe log

## SQL query pack

Ready-to-use monitoring queries are in:
- `sql/query_pack.sql`

Set `:scope_key` to your scope, for example:
- `hostivice:pronajem:bytu,domu`

## Notifications (ntfy)

Scraper sends:
- change alert when new/price-drop/removed events appear
- run summary after each run

Dedupe keys are stored in DB (`notifications`) to avoid repeated alerts.

## Deployment (GitHub Actions)

Workflow file: `.github/workflows/scraper.yml`

Schedule: every 30 minutes + manual trigger.

### Required GitHub Secrets

- `DB_DSN`
- `NTFY_TOPIC`
- `NTFY_TOKEN` (optional for private ntfy)

### Optional GitHub Variables

- `NTFY_BASE_URL` (default: `https://ntfy.sh`)
- `BZR_CITY` (default: `Hostivice`)
- `BZR_OFFER` (default: `pronajem`)
- `BZR_ESTATE_TYPES` (default: `bytu domu`)
- `BZR_WORKERS` (default: `4`)
- `BZR_DELAY` (default: `0.0`)

## Output files

Written to `output/`:
- `<city>_<offer>_urls.txt`
- `<city>_<offer>_sitemap_matches.json`
- `<city>_<offer>_details.json`
- `<city>_<offer>_details.csv`
- `<city>_<offer>_errors.json`
>>>>>>> 7ff06cd (first)
