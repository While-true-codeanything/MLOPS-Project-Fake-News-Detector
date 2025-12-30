import argparse
import json
import logging
import os
import sys

from confluent_kafka import Consumer
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def get_args():
    ap = argparse.ArgumentParser()

    ap.add_argument("--scoring_topic", default=os.getenv("KAFKA_SCORED_TOPIC", "scored_news"))
    ap.add_argument("--bootstrap_servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))

    ap.add_argument("--user", default=os.getenv("POSTGRES_USER", "user"))
    ap.add_argument("--password", default=os.getenv("POSTGRES_PASSWORD", "password"))

    return ap.parse_args()


class KafkaScoresToPostgresService:
    def __init__(self, args):
        self.args = args

        logger.info("Initializing service for DB logging...")

        self.consumer = Consumer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "group.id": "ml-scores-postgres-logger",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )
        self.consumer.subscribe([args.scoring_topic])
        logger.info(f"Consumer successfully initialized to: {args.scoring_topic}")

        self.conn = psycopg2.connect(
            host = "postgres",
            port = 5432,
            dbname = "fake_news",
            user = args.user,
            password = args.password,
        )
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()
        logger.info("Connected to Postgres")

    def parse_message(self, msg_value):
        data = json.loads(msg_value.decode("utf-8"))
        news_id = str(data["news_id"])
        score = float(data["score"])
        is_fake = int(data["is_fake"])
        return news_id, score, is_fake

    def write_to_db(self, row):
        q = f"""
            INSERT INTO news_predictions (news_id, score, is_fake)
            VALUES (%s, %s, %s)
            ON CONFLICT (news_id) DO NOTHING
        """
        self.cursor.execute(q, row)

    def run(self):
        logger.info("Postgres results database logger service started")
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    news_id, score, is_fake = self.parse_message(msg.value())
                    self.write_to_db((news_id, score, is_fake))
                    logger.info(f"Saved news_id={news_id} score={score:.4f} is_fake={is_fake}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error starting database logger Kafka run service: {e}", exc_info=True)


def main():
    args = get_args()
    service = KafkaScoresToPostgresService(args)
    service.run()


if __name__ == "__main__":
    main()
