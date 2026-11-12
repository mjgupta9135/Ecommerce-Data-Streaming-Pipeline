
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from google.api_core.exceptions import DeadlineExceeded
from google.cloud import pubsub_v1, storage

import config


def drain_entity(entity, subscriber, bucket, ingestion_date):
    sub_path = subscriber.subscription_path(config.PROJECT_ID, config.SUBSCRIPTIONS[entity])
    empty_pulls = 0
    total_messages = 0
    total_files = 0

    # Pub/Sub can return an empty response even when messages remain,
    # so stop only after several empty pulls in a row.
    while empty_pulls < config.MAX_EMPTY_PULLS:
        try:
            response = subscriber.pull(
                request={"subscription": sub_path, "max_messages": config.PULL_BATCH_SIZE},
                timeout=30,
            )
            received = list(response.received_messages)
        except DeadlineExceeded:
            received = []

        if not received:
            empty_pulls += 1
            continue
        empty_pulls = 0

        lines = [m.message.data.decode("utf-8") for m in received]
        ack_ids = [m.ack_id for m in received]

        now = datetime.now(timezone.utc)
        blob_name = (
            f"{config.RAW_PREFIX}/{entity}/dt={ingestion_date}/"
            f"{entity}_{now:%H%M%S}_{uuid.uuid4().hex[:8]}.json"
        )
        bucket.blob(blob_name).upload_from_string(
            "\n".join(lines) + "\n", content_type="application/json"
        )

        # Ack only after the upload above succeeded.
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids": ack_ids})

        total_messages += len(lines)
        total_files += 1
        print(f"  [{entity}] wrote {len(lines):>5} messages -> gs://{config.BUCKET_NAME}/{blob_name}")

    return total_messages, total_files


def main():
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bucket = storage.Client(project=config.PROJECT_ID).bucket(config.BUCKET_NAME)
    subscriber = pubsub_v1.SubscriberClient()

    print(f"Ingestion date (UTC): {ingestion_date}")
    # One thread per entity, so the idle waits overlap instead of adding up.
    # The Pub/Sub and Storage clients are thread-safe.
    with subscriber, ThreadPoolExecutor(max_workers=len(config.ENTITIES)) as pool:
        futures = {
            entity: pool.submit(drain_entity, entity, subscriber, bucket, ingestion_date)
            for entity in config.ENTITIES
        }
        # .result() re-raises any error from a thread, so a failure is never silent
        summary = {entity: future.result() for entity, future in futures.items()}

    print("\nSummary")
    for entity, (messages, files) in summary.items():
        print(f"  {entity:<13} {messages:>6} messages in {files} file(s)")


if __name__ == "__main__":
    main()