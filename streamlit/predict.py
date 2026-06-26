import os
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(encoding="utf-8-sig")

MODELS_DIR = Path(os.getenv("MODELS_DIR", "/opt/kedro-clean/data/06_models"))


def _build_con_string() -> str:
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd  = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "bluesky_clean")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def get_clusters(
    n_posts: int = 10000,
    days_back: Optional[int] = 7,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    cluster_id: Optional[int] = None,
    keyword: Optional[str] = None,
    sentiment: Optional[list] = None,
    max_credibility: Optional[float] = None,
) -> pd.DataFrame:
    """
    Récupère les posts avec cluster, sentiment et score de crédibilité.

    Filtres combinables :
      - days_back OU date_start/date_end  → fenêtre temporelle
      - cluster_id                         → un seul cluster
      - keyword                            → ILIKE sur text_clean
      - sentiment                          → liste de labels {positive,neutral,negative}
      - max_credibility                    → ne garder que les posts au score <= seuil
                                             (= les plus douteux)
      - n_posts                            → limite finale

    Les scores sont en LEFT JOIN : un post sans score (modèle pas encore passé)
    reste visible, avec sentiment_label / credibility_score à NULL.
    """
    engine = create_engine(_build_con_string())

    where = ["c.text_clean IS NOT NULL", "cl.cluster_id IS NOT NULL"]
    params: dict = {"n_posts": n_posts}

    if days_back is not None:
        where.append("c.created_at >= NOW() - (:days_back * INTERVAL '1 day')")
        params["days_back"] = days_back
    else:
        if date_start:
            where.append("c.created_at >= :date_start")
            params["date_start"] = date_start
        if date_end:
            where.append("c.created_at <= :date_end")
            params["date_end"] = date_end

    if cluster_id is not None:
        where.append("cl.cluster_id = :cluster_id")
        params["cluster_id"] = cluster_id

    if keyword:
        where.append("c.text_clean ILIKE :keyword")
        params["keyword"] = f"%{keyword}%"

    if sentiment:
        where.append("s.label = ANY(:sentiment)")
        params["sentiment"] = list(sentiment)

    if max_credibility is not None:
        where.append("f.score <= :max_credibility")
        params["max_credibility"] = max_credibility

    where_sql = " AND ".join(where)

    query = text(f"""
        SELECT
            c.post_id,
            c.profile_name,
            c.text_clean,
            c.text_raw,
            c.created_at,
            c.lang_detected,
            c.like_count,
            c.repost_count,
            c.reply_count,
            c.nb_urls,
            c.nb_exclamations,
            c.ratio_uppercase,
            cl.cluster_id,
            s.label AS sentiment_label,
            s.score AS sentiment_score,
            f.label AS fakenews_label,
            f.score AS credibility_score
        FROM bluesky_posts_clean c
        JOIN bluesky_posts_clusters  cl ON c.post_id = cl.post_id
        LEFT JOIN bluesky_posts_sentiment s ON c.post_id = s.post_id
        LEFT JOIN bluesky_posts_fakenews  f ON c.post_id = f.post_id
        WHERE {where_sql}
        ORDER BY c.created_at DESC
        LIMIT :n_posts
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params=params)

    return df


def summarize_clusters(df: pd.DataFrame, n_top_words: int = 10, n_examples: int = 5) -> dict:
    """
    Pour chaque cluster :
      - top_words  : mots les plus représentatifs via le TF-IDF du LSA
      - examples   : posts exemples (avec sentiment + crédibilité)
      - stats      : taille, langues, engagement, tonalité, crédibilité
    """
    if df.empty:
        return {}

    vectorizer = joblib.load(MODELS_DIR / "lsa_pipeline.pkl").named_steps["tfidf"]
    feature_names = np.array(vectorizer.get_feature_names_out())

    result = {}

    for cid, sub in df.groupby("cluster_id"):
        texts = sub["text_clean"].astype(str).tolist()
        X     = vectorizer.transform(texts)

        tfidf_sum   = np.asarray(X.sum(axis=0)).ravel()
        top_indices = tfidf_sum.argsort()[::-1][:n_top_words]
        top_words   = [(feature_names[i], float(tfidf_sum[i])) for i in top_indices]

        example_cols = ["profile_name", "text_raw", "created_at",
                        "sentiment_label", "credibility_score"]
        examples = (
            sub[example_cols]
            .head(n_examples)
            .to_dict(orient="records")
        )

        lang_counts      = sub["lang_detected"].value_counts().to_dict()
        sentiment_counts = sub["sentiment_label"].value_counts(dropna=True).to_dict()

        # Crédibilité moyenne + part de posts classés "fake" (sur les posts scorés)
        cred = sub["credibility_score"].dropna()
        fake = sub["fakenews_label"].dropna()
        avg_credibility = round(float(cred.mean()), 3) if not cred.empty else None
        pct_fake        = round(float((fake == "fake").mean()) * 100, 1) if not fake.empty else None

        stats = {
            "n_posts":          len(sub),
            "lang_counts":      lang_counts,
            "sentiment_counts": sentiment_counts,
            "avg_credibility":  avg_credibility,
            "pct_fake":         pct_fake,
            "n_scored_fake":    int(fake.notna().sum()),
            "avg_likes":        round(sub["like_count"].mean(), 1),
            "avg_reposts":      round(sub["repost_count"].mean(), 1),
            "avg_replies":      round(sub["reply_count"].mean(), 1),
            "avg_urls":         round(sub["nb_urls"].mean(), 2),
            "avg_exclamations": round(sub["nb_exclamations"].mean(), 2),
            "avg_uppercase":    round(sub["ratio_uppercase"].mean(), 3),
        }

        result[int(cid)] = {
            "top_words": top_words,
            "examples":  examples,
            "stats":     stats,
        }

    return result
