import os
import sys
import time

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic


bootstrap = os.environ.get("CASCADE_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
topic = sys.argv[1] if len(sys.argv) > 1 else f"python-compat-{time.time_ns()}"
expected = [f"python-{index}" for index in range(25)]

admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10_000})
creation = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[topic]
creation.result(15)

delivery_errors = []


def delivered(error, _message):
    if error is not None:
        delivery_errors.append(error)


producer = Producer(
    {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 15_000,
    }
)
for value in expected:
    producer.produce(topic, value=value.encode("utf-8"), on_delivery=delivered)
remaining = producer.flush(20)
if remaining or delivery_errors:
    raise RuntimeError(f"Python producer failed: remaining={remaining}, errors={delivery_errors}")

consumer = Consumer(
    {
        "bootstrap.servers": bootstrap,
        "group.id": f"{topic}-reader",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
)
actual = []
try:
    consumer.assign([TopicPartition(topic, 0, 0)])
    deadline = time.monotonic() + 20
    while len(actual) < len(expected) and time.monotonic() < deadline:
        message = consumer.poll(0.25)
        if message is None:
            continue
        if message.error():
            raise RuntimeError(message.error())
        actual.append(message.value().decode("utf-8"))
finally:
    consumer.close()

if actual != expected:
    raise RuntimeError(f"Python client mismatch: expected={expected}, actual={actual}")
print(f"Python confluent-kafka verified {len(actual)}/{len(expected)} records through {bootstrap}")
