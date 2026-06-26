\c bluesky_clean

CREATE EXTENSION IF NOT EXISTS vector;

-- Table source : alimentée par le DAG bluesky_ingest (functions.ensure_table()
-- la recrée aussi en idempotent au démarrage du DAG, mais on la déclare ici
-- pour que le schéma soit explicite et indépendant du code Python).
CREATE TABLE IF NOT EXISTS posts (
    post_id              TEXT        PRIMARY KEY,
    cid                  TEXT,
    source               TEXT,
    fetched_at           TIMESTAMPTZ,
    profile_id           TEXT,
    profile_name         TEXT,
    profile_display_name TEXT,
    text_raw             TEXT,
    created_at           TIMESTAMPTZ,
    like_count           INTEGER     DEFAULT 0,
    repost_count         INTEGER     DEFAULT 0,
    quote_count          INTEGER     DEFAULT 0,
    bookmark_count       INTEGER     DEFAULT 0,
    reply_count          INTEGER     DEFAULT 0
);

CREATE TABLE IF NOT EXISTS bluesky_posts_clean (
    post_id              TEXT        PRIMARY KEY,
    cid                  TEXT,
    source               TEXT,
    fetched_at           TIMESTAMPTZ,
    cleaned_at           TIMESTAMPTZ DEFAULT NOW(),
    profile_id           TEXT,
    profile_name         TEXT,
    profile_display_name TEXT,
    text_raw             TEXT,
    text_clean           TEXT,
    lang_detected        TEXT,
    created_at           TIMESTAMPTZ,
    like_count           INTEGER,
    repost_count         INTEGER,
    quote_count          INTEGER,
    bookmark_count       INTEGER,
    reply_count          INTEGER,
    nb_urls              INTEGER,
    nb_exclamations      INTEGER,
    nb_questions         INTEGER,
    text_length          INTEGER,
    word_count           INTEGER,
    ratio_uppercase      REAL,
    avg_word_length      REAL
);

CREATE TABLE IF NOT EXISTS bluesky_posts_vectors (
    post_id    TEXT PRIMARY KEY REFERENCES bluesky_posts_clean(post_id),
    embedding  VECTOR(300)
);