CREATE TABLE news_predictions (
    news_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    is_fake SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX idx_news_predictions_created_at
    ON news_predictions (created_at DESC);

CREATE INDEX idx_news_predictions_is_fake
    ON news_predictions (is_fake);
