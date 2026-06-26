\c bluesky_clean

CREATE TABLE IF NOT EXISTS bluesky_posts_sentiment (
    post_id      TEXT        PRIMARY KEY REFERENCES bluesky_posts_clean(post_id),
    label        TEXT        NOT NULL,   -- 'positive', 'neutral', 'negative'
    score        REAL        NOT NULL,   -- score de confiance [0, 1]
    analyzed_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sentiment_label
    ON bluesky_posts_sentiment (label);

CREATE TABLE IF NOT EXISTS bluesky_posts_fakenews (
    post_id      TEXT        PRIMARY KEY REFERENCES bluesky_posts_clean(post_id),
    label        TEXT        NOT NULL,   -- 'real', 'fake'
    score        REAL        NOT NULL,   -- score de confiance [0, 1]
    analyzed_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fakenews_label
    ON bluesky_posts_fakenews (label);
