# Real-Time E-Commerce Clickstream Pipeline

A streaming data pipeline that ingests simulated e-commerce user activity
(page views, cart actions, purchases) in real time and makes it queryable
and visualizable within seconds of the event happening.

## Architecture

```
Python Producer  -->  Pub/Sub Topic  -->  Python Subscriber  -->  BigQuery  -->  Looker Studio
                                          (streaming insert)
```

- **Producer**: a Python script generates realistic clickstream events and publishes
  them as JSON to a Pub/Sub topic.
- **Pub/Sub**: decouples event generation from processing; buffers messages.
- **Subscriber**: a Python script pulls messages from the Pub/Sub subscription and
  streams them into BigQuery via `insert_rows_json`, batching small groups of
  messages for efficiency.
- **BigQuery**: stores raw events, immediately queryable.
- **Looker Studio**: real-time dashboard on top of BigQuery — event volume by type,
  revenue, device breakdown.

> **Design note:** the original design used Cloud Dataflow's managed
> **Pub/Sub Topic to BigQuery** template for the ingestion layer. During the build,
> the GCP Free Trial project had 0 Compute Engine CPU quota (a default restriction
> on trial accounts), which blocks Dataflow from launching worker VMs. Rather than
> block the project on a billing upgrade, I replaced the managed Dataflow stage with
> a lightweight Python subscriber doing the same job — a good example of adapting
> a streaming architecture around real infrastructure constraints. The Dataflow
> template version remains a documented alternative below.

## Why this project

Most fresher portfolios are batch-only. This project demonstrates the streaming
side of data engineering: event-driven ingestion, schema-first design for
streaming targets, and handling a live pipeline end to end — skills that come up
directly in GCP data engineer interviews.

## Tech stack

`Python` · `Cloud Pub/Sub` · `BigQuery (streaming inserts)` · `Looker Studio`
· *(Cloud Dataflow — documented alternative ingestion path)*

## Repo contents

| File | Purpose |
|---|---|
| `producer.py` | Simulates and publishes clickstream events to Pub/Sub |
| `subscriber.py` | Pulls messages from Pub/Sub and streams them into BigQuery (replaces the Dataflow template stage) |
| `schema.json` | BigQuery table schema (also used to align producer output) |
| `BUILD_GUIDE.md` | Full step-by-step console build instructions |

## Sample event

```json
{
  "event_id": "a1b2c3d4-...",
  "user_id": "U9f3a21",
  "session_id": "9f3a21e8-...",
  "event_type": "purchase",
  "product_id": "P4821",
  "category": "Electronics",
  "price": 129.99,
  "quantity": 1,
  "device": "mobile",
  "browser": "Chrome",
  "page_url": "/checkout",
  "event_timestamp": "2026-07-04T10:15:32.512Z"
}
```

## How to run it

See [BUILD_GUIDE.md](./BUILD_GUIDE.md) for the full walkthrough — GCP resource
setup via console, running the producer, launching the Dataflow job, and
building the dashboard.

## Possible extensions

- Add a dead-letter topic for malformed messages
- Partition the BigQuery table by `event_timestamp` for cost efficiency at scale
- Add a second Dataflow stage (custom Beam pipeline) for sessionization or
  windowed aggregates before landing in BigQuery
