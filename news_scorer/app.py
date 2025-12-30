import argparse
import json
import logging
import os
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
from confluent_kafka import Consumer, Producer

from news_scorer.loader import load_artifacts
from news_scorer.preprocessing import preprocess_message


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def get_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=os.getenv("KAFKA_RAW_TOPIC", "raw_news"))
    ap.add_argument("--destination", default=os.getenv("KAFKA_SCORED_TOPIC", "scored_news"))

    ap.add_argument("--weights_dir", default=os.getenv("WEIGHTS_DIR", "/app/model"))
    ap.add_argument("--bootstrap_servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    ap.add_argument("--threshold", type=float, default=float(os.getenv("THRESHOLD", "0.5")))
    return ap.parse_args()


def parse_message(msg_value):
    data = json.loads(msg_value.decode("utf-8"))
    if "news_id" not in data or "text" not in data:
        raise ValueError("Message must contain news_id and text fields")
    return data


class KafkaFakeNewsService:
    def __init__(self, args):
        self.args = args

        weights_dir = Path(str(args.weights_dir).rstrip("/").rstrip("\\"))
        if not (weights_dir / "model.pkl").exists() or not (weights_dir / "vectorizer.pkl").exists():
            raise FileNotFoundError(f"Model or Vectorizer not found in {weights_dir}")

        self.model, self.vectorizer = load_artifacts(str(weights_dir))

        self.consumer = Consumer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "group.id": "ml-fake-news-scorer",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        self.consumer.subscribe([args.source])

        self.producer = Producer({"bootstrap.servers": args.bootstrap_servers})
        logger.info(f"Kafka is initialized. Consumer={args.source} -> Producer={args.destination}")

    def score(self, clean_text):
        X = self.vectorizer.transform([clean_text])
        proba = float(self.model.predict_proba(X)[0, 1])
        return proba, int(proba >= self.args.threshold)

    def send_result(self, data):
        self.producer.produce(self.args.destination, value=json.dumps(data).encode("utf-8"))
        self.producer.flush()

    def run(self):
        logger.info("Kafka scoring service started")
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Kafka error: {msg.error()}")
                continue

            try:
                payload = parse_message(msg.value())
                clean = preprocess_message(payload)
                score, is_fake = self.score(clean)

                out = {
                    "news_id": payload["news_id"],
                    "score": score,
                    "is_fake": is_fake,
                    "source": payload.get("source", "unknown"),
                    "processed_at": datetime.utcnow().isoformat(),
                }

                self.send_result(out)
                logger.info(f"Scored news_id={payload['news_id']} score={score:.6f} is_fake={is_fake}")

            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)


def main():
    args = get_args()
    service = KafkaFakeNewsService(args)
    service.run()


if __name__ == "__main__":
    main()
