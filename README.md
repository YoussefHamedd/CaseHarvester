# Case Harvester

A scraping pipeline for the [Maryland Judiciary Case Search](https://casesearch.courts.state.md.us/casesearch/inquiry-index.jsp) (MJCS). Builds a near-complete database of Maryland court cases.

---

## Pipeline Overview

```
Spider → finds case numbers
Scraper → downloads full HTML for each case → stores in MinIO (S3)
Parser → parses HTML → stores structured data in PostgreSQL
```

---

## Components

### Spider
Discovers case numbers by submitting search queries to MJCS. Handles the 500-result limit by recursively narrowing queries.

### FC Enumeration Spider (`fc_enum_spider.py`)
A targeted spider for Foreclosure (FC) and Right of Redemption (ROR) cases. Instead of keyword search, it enumerates every possible case number:

```
C-{county_code}-CV-{year}-{sequence:06d}
```

- 2 instances (split counties between them)
- 10 workers per instance via ThreadPoolExecutor
- Webshare rotating proxies + curl_cffi for DataDome bypass
- Inserts found cases immediately to `cases` table + Redis scraper queue
- Retries blocked sequences forever — never drops a case
- Progress saved to JSON files for live Admin UI display

**Run:**
```bash
python3 fc_enum_spider.py --instance 1   # Counties first half
python3 fc_enum_spider.py --instance 2   # Counties second half
```

### Scraper
Downloads case HTML from MJCS for each case in the Redis queue.

```bash
python3 harvester.py --environment production scraper --from-queue --concurrency 3
```

### Parser
Parses downloaded HTML and inserts structured data into PostgreSQL.

```bash
python3 harvester.py --environment production parser --queue --ignore-errors
```

---

## Case Types Supported

| Code | Description |
|------|-------------|
| MJCS2 | Circuit Court Civil (FC, ROR, CV) |
| ODYCIVIL | MDEC Civil Cases |
| DSCIVIL | District Court Civil |
| ODYCRIM | MDEC Criminal |
| DSCR | District Court Criminal |
| ... | (and more) |

---

## Database

PostgreSQL database `mjcs`. Key tables:

| Table | Description |
|-------|-------------|
| `cases` | All discovered cases (skeleton + scraped) |
| `mjcs2` | Fully parsed Circuit Court Civil cases |
| `scrape_versions` | Scrape history per case |

---

## Environment

```env
SQLALCHEMY_DATABASE_URI_PRODUCTION=postgresql://user:pass@host:5432/mjcs
REDIS_URL=redis://localhost:6379
MINIO_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

---

## Import from Local Browser Spider

The browser-based spider runs on a residential Mac IP to bypass DataDome. Cases found are POSTed to a local receiver and imported via:

```bash
python3 fc_import.py <batch_file.json>
```

Format:
```json
[{"case_number": "C-02-CV-26-000123", "case_type": "Foreclosure - Residential", "filing_date": "2026-01-15", "caption": "...", "court": "..."}]
```

