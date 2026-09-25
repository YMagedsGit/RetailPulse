#!/usr/bin/env python3
"""Generate RetailPulse application events in the existing Flume lab mount."""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone


DEFAULT_OUTPUT_DIR = "./base/flume/lab_data/retailpulse"


def event_payload(sequence, occurred_at):
    event_type = random.choices(
        ["product_viewed", "cart_updated", "checkout_started", "order_confirmed", "fulfillment_update"],
        weights=[50, 20, 12, 10, 8],
        k=1,
    )[0]
    payload = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": occurred_at.isoformat(),
        "customer_id": random.randint(1, 40_000),
        "session_id": f"session-{random.randint(1, 80_000)}",
        "order_id": random.randint(1, 100_000) if event_type in {"order_confirmed", "fulfillment_update"} else None,
        "product_id": random.randint(1, 10_000),
        "store_id": random.randint(1, 50),
        "channel": random.choice(["web", "mobile"]),
        "sequence": sequence,
    }
    return payload


def write_batch(output_dir, records, batch_number):
    os.makedirs(output_dir, exist_ok=True)
    final_path = os.path.join(output_dir, f"retailpulse_events_{int(time.time())}_{batch_number:03d}.json")
    temporary_path = f"{final_path}.tmp"
    with open(temporary_path, "w", encoding="utf-8") as output:
        for record in records:
            if isinstance(record, str):
                output.write(f"{record}\n")
            else:
                output.write(json.dumps(record, separators=(",", ":")) + "\n")
    os.replace(temporary_path, final_path)
    return final_path


def main():
    parser = argparse.ArgumentParser(description="Generate RetailPulse events for Flume and Kafka.")
    parser.add_argument("--output-dir", default=os.getenv("FLUME_LAB_HOST_DIR", DEFAULT_OUTPUT_DIR),
                        help="Path under the Docker-BigData-Tools repository's existing Flume lab mount.")
    parser.add_argument("--records", type=int, default=10_000)
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    if args.records < 100 or args.batch_size < 1:
        parser.error("--records must be at least 100 and --batch-size must be positive")

    random.seed(args.seed)
    now = datetime.now(timezone.utc)
    records, recent_events, created = [], [], 0
    malformed_count = duplicate_count = late_count = 0
    for sequence in range(1, args.records + 1):
        roll = random.random()
        if roll < 0.01:
            records.append('{"event_id":"malformed","event_type":')
            malformed_count += 1
        else:
            occurred_at = now - timedelta(seconds=random.randint(0, 7_200))
            if roll < 0.04:
                occurred_at -= timedelta(days=random.randint(1, 3))
                late_count += 1
            record = event_payload(sequence, occurred_at)
            if roll < 0.06 and recent_events:
                record["event_id"] = random.choice(recent_events)["event_id"]
                duplicate_count += 1
            records.append(record)
            recent_events.append(record)
            recent_events = recent_events[-1_000:]
        if len(records) == args.batch_size:
            created += 1
            path = write_batch(args.output_dir, records, created)
            print(f"Created {path} ({len(records)} records)")
            records = []
    if records:
        created += 1
        path = write_batch(args.output_dir, records, created)
        print(f"Created {path} ({len(records)} records)")

    print(f"Completed {args.records:,} events in {created} files.")
    print(f"Injected: malformed={malformed_count} (~1%), duplicate={duplicate_count} (~2%), late={late_count} (~3%).")


if __name__ == "__main__":
    main()