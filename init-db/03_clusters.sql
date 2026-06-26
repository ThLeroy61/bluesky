\c bluesky_clean

CREATE TABLE IF NOT EXISTS bluesky_posts_clusters (
    post_id     TEXT        PRIMARY KEY REFERENCES bluesky_posts_vectors(post_id),
    cluster_id  INTEGER     NOT NULL,
    assigned_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clusters_cluster_id
    ON bluesky_posts_clusters (cluster_id);
