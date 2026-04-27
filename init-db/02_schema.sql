\c bluesky_clean

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS bluesky_posts_clean (
    mongo_id              TEXT PRIMARY KEY,
    fetched_at            TIMESTAMP,
    source                TEXT,
    post_id               TEXT,
    cid                   TEXT,
    profile_id            TEXT,
    profile_name          TEXT,
    profile_display_name  TEXT,
    text_raw              TEXT,
    created_at            TIMESTAMP,
    engagement            TEXT,
    nb_urls               INTEGER,
    nb_exclamations       INTEGER,
    nb_questions          INTEGER,
    text_length           INTEGER,
    word_count            INTEGER,
    ratio_uppercase       FLOAT,
    avg_word_length       FLOAT,
    text_clean            TEXT,
    lang_detected         TEXT,
    CONSTRAINT unique_id UNIQUE (mongo_id)
);

CREATE TABLE IF NOT EXISTS bluesky_posts_vectors (
    mongo_id   TEXT PRIMARY KEY REFERENCES bluesky_posts_clean(mongo_id),
    embedding  VECTOR(300)
);