"""Kafka producer: simulates the website/app sending clickstream + order events.

KAFKA IN 5 LINES
  topic      = a named, append-only log of events ("retail.events")
  partition  = a topic is split into N ordered logs so N consumers can work in parallel;
               events with the same KEY always land in the same partition (order kept per key)
  offset     = position of an event inside a partition (0, 1, 2, ...); consumers track it
  consumer group = consumers sharing the work; each partition is read by ONE member,
               and the group's committed offsets say where to resume after a restart
  retention  = events stay for days, so a new consumer can replay history.
"""
import argparse
import json
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import CONFIG  # noqa: E402

EVENT_TYPES = ["page_view", "page_view", "page_view", "add_to_cart", "add_to_cart", "order_placed"]


def make_event(rnd):
    now = datetime.now(timezone.utc)
    # ~5% of events arrive late (phone was offline) -> the watermark decides if they still count
    if rnd.random() < 0.05:
        now -= timedelta(minutes=rnd.randint(1, 15))
    etype = rnd.choice(EVENT_TYPES)
    return {
        "event_id": f"e{rnd.getrandbits(48):x}",
        "event_type": etype,
        "customer_id": rnd.randint(1, 2000),
        "product_id": rnd.randint(1, 200),
        "store_id": rnd.randint(1, 20),
        "amount": round(rnd.uniform(200, 20000), 2) if etype == "order_placed" else None,
        "event_time": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=50)
    ap.add_argument("--rate", type=int, default=40, help="events per second")
    args = ap.parse_args()
    rnd = random.Random(1)
    producer = KafkaProducer(
        bootstrap_servers=CONFIG["kafka"]["bootstrap_servers"],
        key_serializer=lambda k: str(k).encode(),          # key = customer_id -> same partition per customer
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all")                                         # wait until the broker has stored it
    topic, sent, end = CONFIG["kafka"]["topic"], 0, time.time() + args.seconds
    print(f"[producer] sending ~{args.rate} events/s to topic '{topic}' for {args.seconds}s", flush=True)
    while time.time() < end:
        for _ in range(args.rate):
            e = make_event(rnd)
            producer.send(topic, key=e["customer_id"], value=e)
            sent += 1
        time.sleep(1)
    producer.flush()
    print(f"[producer] done, {sent:,} events sent", flush=True)
