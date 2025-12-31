from dagster import asset
import psycopg2


def get_conn():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="fake_news",
        user="user",
        password="password",
    )


@asset
def minute_stats():
    sql = """
    INSERT INTO stats_total (id, total, fake_cnt, avg_score, updated_at)
    SELECT
      1,
      COUNT(*),
      COALESCE(SUM(is_fake), 0),
      COALESCE(AVG(score), 0),
      NOW()
    FROM news_predictions
    ON CONFLICT (id) DO UPDATE SET
      total = EXCLUDED.total,
      fake_cnt = EXCLUDED.fake_cnt,
      avg_score = EXCLUDED.avg_score,
      updated_at = NOW();
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()

