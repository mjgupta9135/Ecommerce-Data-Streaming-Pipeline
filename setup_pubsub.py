"""Create the 4 Pub/Sub topics and their subscriptions. Safe to run more than once."""

from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

import config


def main():
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()

    with subscriber:
        for entity in config.ENTITIES:
            topic_path = publisher.topic_path(config.PROJECT_ID, config.TOPICS[entity])
            sub_path = subscriber.subscription_path(
                config.PROJECT_ID, config.SUBSCRIPTIONS[entity]
            )

            try:
                publisher.create_topic(request={"name": topic_path})
                print(f"Created topic:        {topic_path}")
            except AlreadyExists:
                print(f"Topic exists:         {topic_path}")

            try:
                subscriber.create_subscription(
                    request={
                        "name": sub_path,
                        "topic": topic_path,
                        "ack_deadline_seconds": 60,
                    }
                )
                print(f"Created subscription: {sub_path}")
            except AlreadyExists:
                print(f"Subscription exists:  {sub_path}")


if __name__ == "__main__":
    main()