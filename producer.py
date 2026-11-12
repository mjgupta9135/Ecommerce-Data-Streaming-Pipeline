"""Publish e-commerce events to Pub/Sub (4 topics), with deliberately messy data.

Messy data injected (so the silver layer has real work to do):
  - duplicates : the same message is published twice
  - nulls      : some non-key fields are set to null
  - null keys  : a few records have a null business key (silver must drop them)
  - late events: event time is 1-3 days older than the publish time

Usage:
  python producer.py --dry-run                      # no GCP needed, just prints stats
  python producer.py --customers 50 --products 30 --orders 500
"""

import argparse
import json
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone

import config

DUP_PROB = 0.05       # chance a message is published twice
NULL_PROB = 0.03      # chance a nullable field is set to null
NULL_KEY_PROB = 0.01  # chance a record's business key is null
LATE_PROB = 0.05      # chance an event time is 1-3 days in the past

LOCATIONS = [
    ("Mumbai", "Maharashtra", "India"),
    ("Bengaluru", "Karnataka", "India"),
    ("Austin", "Texas", "USA"),
    ("Seattle", "Washington", "USA"),
    ("London", "England", "UK"),
    ("Manchester", "England", "UK"),
]
CATEGORIES = {
    "Electronics": ["Aura", "Voltix", "Nexo"],
    "Clothing": ["Threadly", "UrbanFit", "Loomis"],
    "Home": ["Nestle&Co", "Casaro", "Hearthly"],
    "Books": ["PageTurn", "Inkwell"],
    "Sports": ["Stridex", "ProPlay"],
    "Beauty": ["Glowa", "Pureline"],
}
PAYMENT_METHODS = ["card", "upi", "netbanking", "wallet", "cod"]


# ---------------------------------------------------------------- helpers
def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def maybe_late(now):
    """Return (event_time, is_late)."""
    if random.random() < LATE_PROB:
        return now - timedelta(days=random.randint(1, 3), hours=random.randint(0, 23)), True
    return now, False


def with_nulls(record, fields):
    """Return a copy of record where each listed field may be set to None."""
    out = dict(record)
    for f in fields:
        if random.random() < NULL_PROB:
            out[f] = None
    return out


# ---------------------------------------------------------------- sink
class Sink:
    """Publishes to Pub/Sub, or only counts messages when dry_run=True."""

    def __init__(self, dry_run):
        self.dry_run = dry_run
        self.stats = Counter()
        self.futures = []
        self.client = None
        if not dry_run:
            from google.cloud import pubsub_v1

            self.client = pubsub_v1.PublisherClient()

    def send(self, entity, record):
        key = config.KEY_FIELD[entity]
        if random.random() < NULL_KEY_PROB:
            record = dict(record)
            record[key] = None
            self.stats["null_keys"] += 1

        copies = 2 if random.random() < DUP_PROB else 1
        if copies == 2:
            self.stats["duplicates"] += 1

        for _ in range(copies):
            self.stats[entity] += 1
            if self.dry_run:
                continue
            topic_path = self.client.topic_path(config.PROJECT_ID, config.TOPICS[entity])
            future = self.client.publish(
                topic_path, json.dumps(record).encode("utf-8"), entity=entity
            )
            self.futures.append(future)

        if len(self.futures) >= 1000:
            self.flush()

    def flush(self):
        for f in self.futures:
            f.result()  # raises if a publish failed
        self.futures = []


# ---------------------------------------------------------------- generators
def new_customer(customer_id, updated_at):
    city, state, country = random.choice(LOCATIONS)
    return {
        "customer_id": customer_id,
        "name": f"Customer {customer_id}",
        "email": f"{customer_id.lower()}@example.com",
        "city": city,
        "state": state,
        "country": country,
        "updated_at": iso(updated_at),
    }


def new_product(product_id, updated_at):
    category = random.choice(list(CATEGORIES))
    return {
        "product_id": product_id,
        "name": f"{category} item {product_id}",
        "category": category,
        "brand": random.choice(CATEGORIES[category]),
        "price": round(random.uniform(5, 500), 2),
        "updated_at": iso(updated_at),
    }


def build_transactions(order_id, customer_id, status, total, order_time, now):
    """Transactions consistent with the final order status."""

    def txn(ttype, tstatus, amount, when):
        return {
            "transaction_id": f"T{uuid.uuid4().hex[:12]}",
            "order_id": order_id,
            "customer_id": customer_id,
            "type": ttype,
            "status": tstatus,
            "amount": round(amount, 2),
            "payment_method": random.choice(PAYMENT_METHODS),
            "transaction_time": iso(min(now, when)),
        }

    t0 = order_time + timedelta(minutes=random.randint(0, 5))
    txns = []
    if status == "cancelled":
        txns.append(txn("payment", "failed", total, t0))
        return txns

    if random.random() < 0.10:  # a failed attempt, then a retry that succeeds
        txns.append(txn("payment", "failed", total, t0))
        t0 += timedelta(minutes=random.randint(1, 10))
    txns.append(txn("payment", "success", total, t0))

    if status == "returned":
        refund_time = order_time + timedelta(hours=random.randint(6, 72))
        txns.append(txn("refund", "success", total, max(refund_time, t0)))
    return txns


# ---------------------------------------------------------------- main
def run(args):
    sink = Sink(args.dry_run)
    now = datetime.now(timezone.utc)

    # 1) initial dimensions: versions older than any order, so SCD2 joins work
    customers, products = {}, {}
    for i in range(args.customers):
        cid = f"C{i + 1:05d}"
        customers[cid] = new_customer(cid, now - timedelta(days=random.randint(5, 30)))
        sink.send("customers", with_nulls(customers[cid], ["email", "city"]))
    for i in range(args.products):
        pid = f"P{i + 1:04d}"
        products[pid] = new_product(pid, now - timedelta(days=random.randint(5, 30)))
        sink.send("products", with_nulls(products[pid], ["category", "brand", "price"]))

    customer_ids, product_ids = list(customers), list(products)

    # 2) orders + transactions, with occasional customer/product changes (SCD2)
    for _ in range(args.orders):
        order_time, _late = maybe_late(now)
        order_id = f"O{uuid.uuid4().hex[:12]}"
        customer_id = random.choice(customer_ids)

        items = []
        chosen = random.sample(product_ids, random.randint(1, min(4, len(product_ids))))
        for line_no, pid in enumerate(chosen, start=1):
            items.append(
                {
                    "line_number": line_no,
                    "product_id": pid,
                    "quantity": random.randint(1, 3),
                    "unit_price": products[pid]["price"],
                }
            )
        total = sum(i["quantity"] * i["unit_price"] for i in items)

        roll = random.random()
        status = "cancelled" if roll < 0.05 else "returned" if roll < 0.10 else "completed"

        order = {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": status,
            "order_time": iso(order_time),
            "items": [with_nulls(i, ["unit_price"]) for i in items],
        }
        sink.send("orders", with_nulls(order, ["order_status"]))

        for t in build_transactions(order_id, customer_id, status, total, order_time, now):
            sink.send("transactions", with_nulls(t, ["payment_method"]))

        # occasional change to a customer or product -> new version (SCD2)
        if random.random() < args.update_prob:
            when, _ = maybe_late(now)
            if random.random() < 0.5:
                cid = random.choice(customer_ids)
                city, state, country = random.choice(LOCATIONS)
                customers[cid].update(
                    city=city, state=state, country=country, updated_at=iso(when)
                )
                sink.send("customers", with_nulls(customers[cid], ["email", "city"]))
            else:
                pid = random.choice(product_ids)
                products[pid].update(
                    price=round(products[pid]["price"] * random.uniform(0.8, 1.2), 2),
                    updated_at=iso(when),
                )
                sink.send("products", with_nulls(products[pid], ["category", "brand", "price"]))

        if args.sleep:
            time.sleep(args.sleep)

    sink.flush()
    label = "DRY RUN (nothing published)" if args.dry_run else "Published"
    print(f"\n{label}")
    for k in [*config.ENTITIES, "duplicates", "null_keys"]:
        print(f"  {k:<13} {sink.stats[k]}")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--customers", type=int, default=50)
    p.add_argument("--products", type=int, default=30)
    p.add_argument("--orders", type=int, default=500)
    p.add_argument("--update-prob", type=float, default=0.10,
                   help="chance after each order that a customer/product gets a new version")
    p.add_argument("--sleep", type=float, default=0.0, help="seconds to wait between orders")
    p.add_argument("--dry-run", action="store_true", help="generate and count only, no GCP calls")
    run(p.parse_args())