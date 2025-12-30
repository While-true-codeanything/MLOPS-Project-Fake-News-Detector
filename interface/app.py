import json
import os
import time
import uuid
import tempfile
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
import streamlit as st
from confluent_kafka import Producer
import matplotlib.pyplot as plt

DEFAULT_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "raw_news")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "fake_news")
PG_USER = os.getenv("PG_USER", "user")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")


def plot_score_hist(scores, outpath):
    plt.figure(figsize=(10, 5))
    plt.hist(scores, bins=60, density=True, alpha=0.85)
    plt.grid(alpha=0.4)
    plt.xlabel("Score")
    plt.ylabel("Density")
    plt.title("Score distribution")
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()


def _to_int_or_none(s: str):
    s = (s or "").strip()
    if s == "":
        return None
    try:
        v = int(s)
        return v if v > 0 else None
    except Exception:
        return None


def send_single_to_kafka(title, text, topic, bootstrap_servers, news_id=None, source="ui_single", timestamp=None):
    producer = Producer({"bootstrap.servers": bootstrap_servers})
    payload = {
        "news_id": news_id or str(uuid.uuid4()),
        "title": title or "",
        "text": text or "",
        "source": source,
        "timestamp": timestamp or datetime.utcnow().isoformat(),
    }
    producer.produce(topic, value=json.dumps(payload).encode("utf-8"))
    producer.flush()
    return payload["news_id"]



def send_rows_to_kafka(
        df: pd.DataFrame,
        topic,
        bootstrap_servers,
        sleep_sec,
        limit_rows,
):
    df = df.copy()
    if limit_rows is not None:
        df = df.head(limit_rows)

    if "news_id" not in df.columns:
        df.insert(0, "news_id", [str(uuid.uuid4()) for _ in range(len(df))])

    progress = st.progress(0.0)
    total = len(df)

    has_title = "title" in df.columns

    for i, row in df.iterrows():
        nid = str(row["news_id"])
        title = "" if not has_title else ("" if pd.isna(row.get("title")) else str(row.get("title")))
        text = "" if pd.isna(row.get("text")) else str(row.get("text"))

        send_single_to_kafka(
            title=title,
            text=text,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            news_id=nid,
            source="ui_csv",
            timestamp=datetime.utcnow().isoformat(),
        )

        progress.progress((i + 1) / max(total, 1))
        if sleep_sec and sleep_sec > 0:
            time.sleep(sleep_sec)


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=str(PG_PORT),
        dbname="fake_news",
        user=PG_USER,
        password=PG_PASSWORD,
    )


def load_last_fakes(limit = 10) -> pd.DataFrame:
    sql = f"""
        SELECT news_id, score, is_fake, source, created_at
        FROM news_predictions
        WHERE is_fake = 1
        ORDER BY created_at DESC
        LIMIT {int(limit)};
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


def load_last_scores(limit = 200) -> pd.DataFrame:
    sql = f"""
        SELECT score
        FROM news_predictions
        ORDER BY created_at DESC
        LIMIT {int(limit)};
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


def load_last_predictions(limit=20) -> pd.DataFrame:
    sql = f"""
        SELECT news_id, score, is_fake, source, created_at
        FROM news_predictions
        ORDER BY created_at DESC
        LIMIT {int(limit)};
    """
    with get_conn() as conn:
        return pd.read_sql(sql, conn)


st.set_page_config(page_title="Fake News UI", layout="wide")

st.title("Fake News Detection UI")

with st.sidebar:
    st.subheader("Connections")
    brokers = st.text_input("Kafka brokers", value=DEFAULT_BROKERS)
    topic = st.text_input("Kafka topic", value=DEFAULT_TOPIC)

    st.divider()
    st.subheader("Sending")
    sleep_sec = st.slider("Pause between messages (sec)", min_value=0.0, max_value=1.0, value=0.02, step=0.01)
    limit_rows = _to_int_or_none(st.text_input("Row limit (optional)", value=""))

    st.divider()
    st.subheader("Results")
    scores_n = st.selectbox("Scores to plot", options=[50, 100, 200, 500], index=2)
    last_n = st.selectbox("Rows in history table", options=[10, 20, 50], index=1)

mode = st.radio("Mode", ["Single news", "Upload CSV"], horizontal=True)

col_left, col_right = st.columns([1.2, 1.0], gap="large")

with col_left:
    st.subheader("1) Submit data")

    if mode == "Single news":
        title = st.text_input("Title", value="")
        text = st.text_area("Text", height=220, value="")
        c1, c2 = st.columns([1, 2])
        with c1:
            if st.button("Send to Kafka", type="primary", use_container_width=True):
                if not text.strip():
                    st.warning("Text is required")
                else:
                    with st.spinner("Sending..."):
                        news_id = send_single_to_kafka(title, text, topic=topic, bootstrap_servers=brokers)
                    st.success(f"Sent. news_id={news_id}")

        with c2:
            st.caption("This sends one JSON message to Kafka. Scorer will process it asynchronously.")

    else:
        uploaded = st.file_uploader("Upload CSV (must contain column `text`, optional `title`)", type=["csv"])
        df = None
        if uploaded is not None:
            df = pd.read_csv(uploaded)

        if df is None:
            st.info("Upload a CSV file to continue.")
        else:
            if "text" not in df.columns:
                st.error("CSV must contain column `text`")
            else:
                m1, m2, m3 = st.columns(3)
                m1.metric("Rows", str(len(df)))
                m2.metric("Columns", str(df.shape[1]))
                m3.metric("Will send", str(len(df) if limit_rows is None else min(len(df), limit_rows)))

                st.write("Preview")
                st.dataframe(df.head(7), use_container_width=True, hide_index=True)

                if st.button("Send file rows to Kafka", type="primary", use_container_width=True):
                    with st.spinner("Sending messages..."):
                        send_rows_to_kafka(
                            df=df,
                            topic=topic,
                            bootstrap_servers=brokers,
                            sleep_sec=float(sleep_sec),
                            limit_rows=limit_rows,
                        )
                    st.success("Done. Messages sent to Kafka.")

with col_right:
    st.subheader("2) Results & Monitoring")

    top_actions = st.columns([1, 1, 2])
    with top_actions[0]:
        if st.button("Refresh", use_container_width=True):
            st.rerun()
    with top_actions[1]:
        show_only_fakes = st.toggle("Only FAKE", value=False)
    with top_actions[2]:
        st.caption(f"Postgres: `{PG_HOST}:{PG_PORT}/fake_news` table `news_predictions`")

    try:
        hist_df = load_last_predictions(limit=int(last_n))
        if show_only_fakes:
            hist_df = hist_df[hist_df["is_fake"] == 1]

        if hist_df.empty:
            st.info("No predictions yet.")
        else:
            st.markdown("**Latest predictions**")
            st.dataframe(hist_df, use_container_width=True, hide_index=True)

        st.divider()

        fake_df = load_last_fakes(limit=10)
        if fake_df.empty:
            st.info("No `is_fake = 1` records yet.")
        else:
            st.markdown("**Last 10 FAKE records**")
            st.dataframe(fake_df, use_container_width=True, hide_index=True)

        st.divider()

        scores_df = load_last_scores(limit=int(scores_n))
        if scores_df.empty:
            st.info("No scores to plot yet.")
        else:
            scores = scores_df["score"].astype(float).to_numpy()
            with tempfile.TemporaryDirectory() as tmp:
                outpath = os.path.join(tmp, "scores_hist.png")
                plot_score_hist(scores, outpath)
                st.image(outpath, caption=f"Score distribution (last {len(scores)} records)", use_container_width=True)

    except Exception:
        st.error("Cannot load results from Postgres. Check DB service / credentials.")
