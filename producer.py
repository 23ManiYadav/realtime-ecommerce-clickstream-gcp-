"""
Clickstream Event Producer
---------------------------
Simulates real-time e-commerce user activity (page views, add-to-cart,
purchases, etc.) and publishes each event as a JSON message to a
Pub/Sub topic.

Prerequisites:
    pip install google-cloud-pubsub
    gcloud auth application-default login   (the one CLI step you need)

Usage:
    python producer.py --project YOUR_PROJECT_ID --topic clickstream-events
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1

EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "purchase", "search"]
CATEGORIES = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Sports", "Beauty"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]
PAGE_URLS = [
    "/home", "/product/{}", "/cart", "/checkout", "/search", "/category/{}"
]


def generate_event(session_pool):
    """Build one realistic clickstream event as a dict."""
    session_id = random.choice(session_pool)
    event_type = random.choice(EVENT_TYPES)
    product_id = f"P{random.randint(1000, 9999)}"
    category = random.choice(CATEGORIES)

    page_url_template = random.choice(PAGE_URLS)
    page_url = page_url_template.format(product_id) if "{}" in page_url_template else page_url_template

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"U{session_id[-6:]}",
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product_id if event_type in ("add_to_cart", "remove_from_cart", "purchase") else None,
        "category": category,
        "price": round(random.uniform(5.0, 500.0), 2) if event_type in ("add_to_cart", "purchase") else None,
        "quantity": random.randint(1, 3) if event_type in ("add_to_cart", "purchase") else None,
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "page_url": page_url,
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
    return event


def main():
    parser = argparse.ArgumentParser(description="Publish simulated clickstream events to Pub/Sub")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--topic", default="clickstream-events", help="Pub/Sub topic name")
    parser.add_argument("--rate", type=float, default=2.0, help="Events per second")
    parser.add_argument("--duration", type=int, default=0, help="Seconds to run (0 = run forever)")
    parser.add_argument("--sessions", type=int, default=20, help="Number of concurrent simulated sessions")
    args = parser.parse_args()

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(args.project, args.topic)

    session_pool = [str(uuid.uuid4()) for _ in range(args.sessions)]

    print(f"Publishing to {topic_path} at ~{args.rate} events/sec. Ctrl+C to stop.")
    start_time = time.time()
    sent = 0

    try:
        while True:
            event = generate_event(session_pool)
            data = json.dumps(event).encode("utf-8")
            future = publisher.publish(topic_path, data)
            future.result()  # confirm publish
            sent += 1

            if sent % 20 == 0:
                print(f"Published {sent} events... last: {event['event_type']} ({event['session_id'][:8]})")

            if args.duration and (time.time() - start_time) > args.duration:
                break

            time.sleep(1.0 / args.rate)

    except KeyboardInterrupt:
        pass
    finally:
        print(f"\nDone. Total events published: {sent}")


if __name__ == "__main__":
    main()
