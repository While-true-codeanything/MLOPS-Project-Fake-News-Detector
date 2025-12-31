CREATE TABLE IF NOT EXISTS news_predictions (
    news_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    is_fake SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_predictions_created_at
    ON news_predictions (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_predictions_is_fake
    ON news_predictions (is_fake);

CREATE TABLE IF NOT EXISTS stats_minute (
  id SMALLINT PRIMARY KEY DEFAULT 1,
  total BIGINT NOT NULL,
  fake_cnt BIGINT NOT NULL,
  avg_score DOUBLE PRECISION NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);