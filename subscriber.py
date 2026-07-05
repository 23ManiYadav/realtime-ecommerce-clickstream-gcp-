"""
Clickstream Event Subscriber (No-Dataflow Variant)
----------------------------------------------------
Pulls messages from a Pub/Sub subscription and streams them straight into
BigQuery using the BigQuery client library's insert_rows_json (streaming
insert). This replaces the Dataflow template step entirely, so it needs
zero Compute Engine quota -- it just runs as a normal Python process.

Architecture:
    Producer --> Pub/Sub Topic --> [this script] --> BigQuery

Prerequisites:
    pip install google-cloud-pubsub google-cloud-bigquery
    gcloud auth application-default login

Usage:
    python subscriber.py --project YOUR_PROJECT_ID \
        --subscription clickstream-events-sub \
        --dataset clickstream_dw --table raw_events
"""

import argparse
import json
import time

from google.cloud import bigquery
from google.cloud import pubsub_v1


def make_callback(bq_client, table_ref, batch, batch_size, ack_deadline_batch):
    """Returns a Pub/Sub callback that buffers messages and flushes them to
    BigQuery in small batches for efficiency."""

    def callback(message):
        try:
            row = json.loads(message.data.decode("utf-8"))
            batch.append(row)
            ack_deadline_batch.append(message)

            if len(batch) >= batch_size:
                flush(bq_client, table_ref, batch, ack_deadline_batch)

        except Exception as e:
            print(f"Failed to process message: {e}")
            message.nack()

    return callback


def flush(bq_client, table_ref, batch, ack_deadline_batch):
    if not batch:
        return

    errors = bq_client.insert_rows_json(table_ref, batch)

    if errors:
        print(f"BigQuery insert errors: {errors}")
        # Nack so Pub/Sub redelivers these messages
        for msg in ack_deadline_batch:
            msg.nack()
    else:
        print(f"Inserted {len(batch)} rows into BigQuery.")
        for msg in ack_deadline_batch:
            msg.ack()

    batch.clear()
    ack_deadline_batch.clear()


def main():
    parser = argparse.ArgumentParser(description="Stream Pub/Sub messages into BigQuery")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--subscription", default="clickstream-events-sub", help="Pub/Sub subscription name")
    parser.add_argument("--dataset", default="clickstream_dw", help="BigQuery dataset")
    parser.add_argument("--table", default="raw_events", help="BigQuery table")
    parser.add_argument("--batch-size", type=int, default=10, help="Rows to buffer before writing to BigQuery")
    parser.add_argument("--flush-interval", type=float, default=5.0, help="Max seconds to wait before flushing a partial batch")
    args = parser.parse_args()

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(args.project, args.subscription)

    bq_client = bigquery.Client(project=args.project)
    table_ref = f"{args.project}.{args.dataset}.{args.table}"

    batch = []
    ack_deadline_batch = []

    callback = make_callback(bq_client, table_ref, batch, args.batch_size, ack_deadline_batch)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening on {subscription_path}, writing to {table_ref}...")
    print("Ctrl+C to stop.")

    try:
        while True:
            time.sleep(args.flush_interval)
            # Flush any partial batch that hasn't hit batch_size yet,
            # so rows don't sit unwritten for too long during slow periods.
            if batch:
                flush(bq_client, table_ref, batch, ack_deadline_batch)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
        # Final flush of anything left in the buffer
        if batch:
            flush(bq_client, table_ref, batch, ack_deadline_batch)
        print("\nStopped.")


if __name__ == "__main__":
    main()
