```markdown
# 8x8 → BigQuery User Sync (GAS)

A zero-infrastructure Google Apps Script pipeline that pulls **all user accounts** from the 8x8 SCIM v2 API and loads them into **BigQuery** with a **staging + merge** pattern (insert new, update changed, keep history).

---

## What it does

1. Reads your 8x8 `customer_id` from **GAS Script Properties** (no hard-coding).
2. Authenticates to the 8x8 SCIM endpoint with a **static Bearer token**.
3. Paginates through `/Users` (page size = 100) until every record is fetched.
4. Flattens the nested SCIM JSON into a BigQuery-friendly row.
5. Streams the rows into a **staging table** (`*_staging`) with `WRITE_TRUNCATE`.
6. Runs an **id-based MERGE** to upsert into the final table.
7. Logs every step to **Apps Script Logger** (visible in Executions).

---

## BigQuery objects created

| Object | Purpose |
|--------|---------|
| `{project}.{dataset}.8x8Users` | Production table (latest snapshot) |
| `{dataset}.8x8Users_staging` | Temporary landing zone (truncated each run) |

---

## Quick start

1. **Create / reuse a GCP project**  
   - Enable BigQuery API  
   - Give the Apps Script default service account  
     `PROJECT_ID@appspot.gserviceaccount.com`  
     the role **BigQuery Data Editor** on the dataset.

2. **Open Apps Script** (script.google.com) → **New Project**  
   - Delete default `Code.gs`  
   - Paste the entire contents of `8x8UsersSync.gs` (this repo)

3. **Project Settings ⚙️ → Script Properties**  
   Add key/value pairs:

   | Key | Value |
   |-----|-------|
   | `customer_id` | `abc123` (your 8x8 tenant id) |

4. **Edit the constants at the top of the file**  
   Replace every `<YOUR>-*` placeholder with real values:

   ```javascript
   var SCIM_API_TOKEN            = 'eyJ0eXAiOiJKV1...';   // 8x8 portal → API tokens
   var BIGQUERY_PROJECT_ID       = 'my-gcp-project';
   var BIGQUERY_DATASET_ID       = '8x8_dataset';
   var BIGQUERY_TABLE_ID         = '8x8Users';
   var BIGQUERY_STAGING_TABLE_ID = '8x8Users_staging';
   ```

5. **Run `sync8x8UsersToBigQuery` once manually**  
   - Authorize scopes (BigQuery + URL Fetch)  
   - Check **Executions** → last log line should be  
     `sync8x8UsersToBigQuery FINISHED successfully.`

6. **Schedule it**  
   Triggers ⏰ → Add Trigger →  
   Function: `sync8x8UsersToBigQuery`  
   Event source: **Time-driven** (e.g. every 4 hours)

---

## Schema (auto-created on first run)

| Column | Type | Source |
|--------|------|--------|
| `id` | STRING | `Resources[i].id` |
| `userName` | STRING | `userName` |
| `givenName` | STRING | `name.givenName` |
| `familyName` | STRING | `name.familyName` |
| `email` | STRING | `emails[0].value` |
| `active` | BOOLEAN | `active` |
| `created` | TIMESTAMP | `meta.created` |
| `lastModified` | TIMESTAMP | `meta.lastModified` |

---

## Monitoring

* **Apps Script → Executions** – last status & error messages  
* **BigQuery → Jobs** – bytes processed, slot time  
* **Cloud Logging** – if you link the script to a GCP standard project

---

## Idempotency & safety

* Staging table is **truncated** before every load → no duplicates from retries  
* MERGE uses **id** as the business key → only one row per 8x8 user  
* Script stops after **100 API pages** (~10 k users) to prevent runaway costs (raise `MAX_USER_PAGES_TO_FETCH` if you have more)

---

## Local testing / one-off export

Run the function `manuallyFetchAndLogSampleUsers()` – it executes the full flow but keeps everything in Logger (no side effects beyond BigQuery load).

---

## Troubleshooting cheatsheet

| Symptom | Fix |
|---------|-----|
| `Missing customer_id` | Add Script Property `customer_id` |
| `403 BigQuery` | Grant **BigQuery Data Editor** to the Apps Script SA |
| `401 / 403 from 8x8` | Regenerate token in 8x8 portal; paste new value into `SCIM_API_TOKEN` |
| `MERGE failed` | Ensure both tables exist and have **identical schema** (run once with staging to auto-create) |

---

## License

MIT – feel free to fork and embed in your own GCP tenant.
```
