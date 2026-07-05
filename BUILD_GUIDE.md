# Real-Time E-Commerce Clickstream Pipeline — Build Guide

**Architecture:** Python Producer → Pub/Sub → Dataflow (Google-provided template) → BigQuery → Looker Studio

This guide uses the **GCP Console (UI)** for every step except one unavoidable CLI command
(`gcloud auth application-default login`, needed so your local producer script can
authenticate to Pub/Sub).

---

## 0. Prerequisites

- A GCP project with billing enabled
- Python 3.9+
- `pip install google-cloud-pubsub`

---

## 1. Enable required APIs

Console → **APIs & Services → Library** → enable:
- Cloud Pub/Sub API
- Dataflow API
- BigQuery API

---

## 2. Create the Pub/Sub topic

1. Console → **Pub/Sub → Topics → Create Topic**
2. Topic ID: `clickstream-events`
3. Leave "Add a default subscription" checked (creates `clickstream-events-sub`, not strictly needed since the Dataflow template reads from the topic directly, but useful for testing)
4. Click **Create**

---

## 3. Create the BigQuery dataset and table

1. Console → **BigQuery → your project → Create Dataset**
   - Dataset ID: `clickstream_dw`
   - Location: choose one close to you (e.g. `asia-south1` for Hyderabad)
2. Inside the dataset → **Create Table**
   - Source: Empty table
   - Table name: `raw_events`
   - Schema: click **Edit as text** and paste the contents of `schema.json`
   - Leave partitioning/clustering default for now (optional: partition by `event_timestamp` for cost control on larger volumes)
3. Click **Create Table**

---

## 4. Set up authentication for the producer

This is the one CLI step:

```
gcloud auth application-default login
```

This lets the local Python script publish to Pub/Sub using your user credentials
without managing a service account key file.

---

## 5. Run the producer

```
pip install google-cloud-pubsub
python producer.py --project YOUR_PROJECT_ID --topic clickstream-events --rate 3
```

Leave this running in a terminal — it will keep publishing simulated events
(page views, add-to-cart, purchases, etc.) until you stop it with Ctrl+C.

---

## 6. Stream data into BigQuery

### Option A — Python subscriber (used in this build)

On a fresh GCP Free Trial project, Compute Engine CPU quota defaults to **0**,
which blocks Dataflow from launching worker VMs. Rather than upgrade billing
just to get workers, this build uses a lightweight Python subscriber that
pulls from the Pub/Sub subscription and writes straight into BigQuery — no
Compute Engine VM required.

```
pip install google-cloud-bigquery
python subscriber.py --project YOUR_PROJECT_ID \
    --subscription clickstream-events-sub \
    --dataset clickstream_dw --table raw_events
```

Leave this running alongside the producer. It batches ~10 messages (or every
5 seconds, whichever comes first) and streams them into BigQuery via
`insert_rows_json`, printing `Inserted N rows into BigQuery` as it goes.

### Option B — Dataflow managed template (if you have CPU quota / paid billing)

1. Console → **Dataflow → Jobs → Create Job from Template**
2. Job name: `clickstream-to-bq`
3. Regional endpoint: same region as your BigQuery dataset
4. Dataflow template: **Pub/Sub Topic to BigQuery**
5. Fill in the required parameters:
   - Input Pub/Sub topic: `projects/YOUR_PROJECT_ID/topics/clickstream-events`
   - BigQuery output table: `YOUR_PROJECT_ID:clickstream_dw.raw_events`
   - Temporary location: a GCS bucket path for staging (e.g. `gs://your-bucket/temp`)
6. Click **Run Job**

The job will show as "Running" and start pulling messages from Pub/Sub within a
minute or two. Both options expect each Pub/Sub message to be a JSON object
whose fields match the BigQuery table schema — this is why the producer's
field names mirror `schema.json` field-for-field.

---

## 7. Verify data is flowing

Console → **BigQuery → clickstream_dw.raw_events → Preview**, or run:

```sql
SELECT event_type, COUNT(*) AS cnt
FROM `YOUR_PROJECT_ID.clickstream_dw.raw_events`
GROUP BY event_type
ORDER BY cnt DESC
```

You should see rows accumulating every few seconds while the producer runs.

---

## 8. Build the Looker Studio dashboard

1. Go to **lookerstudio.google.com → Create → Report**
2. Add data source → **BigQuery** → select `clickstream_dw.raw_events`
3. Suggested visuals:
   - Time series: event count over time, broken down by `event_type`
   - Bar chart: events by `category`
   - Scorecard: total purchases, total revenue (`SUM(price * quantity)` filtered to `event_type = 'purchase'`)
   - Pie chart: traffic by `device`
4. Set the report to auto-refresh (Looker Studio refreshes on view / on a schedule depending on your connector settings)

---

## 9. Clean up (avoid ongoing charges)

- Stop the producer script (Ctrl+C)
- Stop the subscriber script (Ctrl+C) — it flushes any remaining buffered rows before exiting
- If you ran the Dataflow variant instead: Console → **Dataflow → Jobs → clickstream-to-bq → Stop** (choose "Cancel", not "Drain")
- Optionally delete the BigQuery dataset, Pub/Sub topic, and GCS staging bucket if you're done demoing

---

## Troubleshooting notes (from real debugging)

- **Field mismatch errors in Dataflow logs**: the Pub/Sub-to-BigQuery template does strict
  schema matching — a field present in the JSON but not in the BigQuery schema (or vice versa
  as REQUIRED) will cause the message to fail and land in the job's error output. Double-check
  `schema.json` matches the producer's JSON keys exactly.
- **Timestamp format**: BigQuery expects ISO 8601 with a `Z` suffix for the `TIMESTAMP` type,
  which is why the producer formats it as `%Y-%m-%dT%H:%M:%S.%fZ`.
- **No data appearing**: confirm the Dataflow job's region matches where you expect, and that
  the producer is pointed at the correct topic name (typos here fail silently — Pub/Sub just
  won't have a subscriber pulling from the wrong topic).
