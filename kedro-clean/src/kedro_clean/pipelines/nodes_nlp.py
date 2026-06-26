import os

import pandas as pd
from sqlalchemy import create_engine, text

# Modèles validés sur HuggingFace :
#   - sentiment : variante MULTILINGUE (FR + EN + 6 autres), labels Positive/Neutral/Negative
#   - fake news : entraîné sur des articles de presse EN, labels Fake/Real, attend
#                 un format "<title>...<content>...<end>"
SENTIMENT_MODEL = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
FAKENEWS_MODEL  = "hamzab/roberta-fake-news-classification"

MAX_LENGTH = 512  # suffisant : un post Bluesky est court, mais on tronque par sécurité


def _build_con_string() -> str:
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd  = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "bluesky_clean")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _get_device() -> str:
    """GPU si disponible, CPU sinon (cf consigne : 'GPU si dispo, CPU sinon')."""
    import torch
    return "cuda" if torch.cuda.is_available() else "cpu"


def _preprocess_tweet(s: str) -> str:
    tokens = []
    for t in str(s).split(" "):
        if t.startswith("@") and len(t) > 1:
            t = "@user"
        elif t.startswith("http"):
            t = "http"
        tokens.append(t)
    return " ".join(tokens)


def _load_unscored(score_table: str) -> pd.DataFrame:
    engine = create_engine(_build_con_string())
    query = text(f"""
        SELECT c.post_id, c.text_raw
        FROM bluesky_posts_clean c
        LEFT JOIN {score_table} s ON c.post_id = s.post_id
        WHERE s.post_id IS NULL
          AND c.text_raw IS NOT NULL
          AND LENGTH(c.text_raw) > 0
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    print(f"[NLP] {len(df)} posts à scorer (cible : {score_table}).")
    return df


def _upsert_scores(df: pd.DataFrame, score_table: str) -> None:
    """Upsert (post_id, label, score, NOW()) via table temporaire — même pattern que upsert_clusters."""
    if df.empty:
        print(f"[NLP] Rien à upserter dans {score_table}.")
        return

    engine = create_engine(_build_con_string())
    rows   = df[["post_id", "label", "score"]].to_dict(orient="records")
    tmp    = f"{score_table}_tmp"

    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TEMPORARY TABLE {tmp} (
                post_id TEXT,
                label   TEXT,
                score   REAL
            ) ON COMMIT DROP
        """))
        conn.execute(
            text(f"INSERT INTO {tmp} (post_id, label, score) VALUES (:post_id, :label, :score)"),
            rows,
        )
        result = conn.execute(text(f"""
            INSERT INTO {score_table} (post_id, label, score, analyzed_at)
            SELECT post_id, label, score, NOW()
            FROM {tmp}
            ON CONFLICT (post_id) DO UPDATE SET
                label       = EXCLUDED.label,
                score       = EXCLUDED.score,
                analyzed_at = NOW()
        """))

    print(f"[NLP] {result.rowcount} lignes upsertées dans {score_table}.")


# ── Analyse émotionnelle ──────────────────────────────────────────────────────

def load_posts_for_sentiment() -> pd.DataFrame:
    return _load_unscored("bluesky_posts_sentiment")


def run_sentiment(df: pd.DataFrame, batch_size: int) -> pd.DataFrame:
    if df.empty:
        print("[NLP] Aucun post à analyser (sentiment).")
        return pd.DataFrame(columns=["post_id", "label", "score"])

    from transformers import pipeline

    device = 0 if _get_device() == "cuda" else -1  # convention pipeline : 0=GPU, -1=CPU
    print(f"[NLP] Sentiment sur {len(df)} posts (device={'GPU' if device == 0 else 'CPU'})...")

    clf = pipeline(
        "sentiment-analysis",
        model=SENTIMENT_MODEL,
        tokenizer=SENTIMENT_MODEL,
        device=device,
        truncation=True,
        max_length=MAX_LENGTH,
    )

    texts = df["text_raw"].astype(str).map(_preprocess_tweet).tolist()
    preds = clf(texts, batch_size=batch_size)

    out = df[["post_id"]].copy()
    out["label"] = [p["label"].lower() for p in preds]   # 'Positive' → 'positive'
    out["score"] = [float(p["score"]) for p in preds]
    print(f"[NLP] Sentiment calculé pour {len(out)} posts.")
    return out


def upsert_sentiment(df: pd.DataFrame) -> None:
    _upsert_scores(df, "bluesky_posts_sentiment")


# ── Détection de fake news ────────────────────────────────────────────────────

def load_posts_for_fakenews() -> pd.DataFrame:
    return _load_unscored("bluesky_posts_fakenews")


def run_fakenews(df: pd.DataFrame, batch_size: int) -> pd.DataFrame:
    """
    Écart de domaine assumé : modèle entraîné sur des articles de presse longs,
    appliqué à des posts courts → scores indicatifs, à relativiser dans le rapport.
    """
    if df.empty:
        print("[NLP] Aucun post à analyser (fake news).")
        return pd.DataFrame(columns=["post_id", "label", "score"])

    import torch
    from transformers import AutoTokenizer, AutoModelForSequenceClassification

    device = _get_device()
    print(f"[NLP] Fake news sur {len(df)} posts (device={device})...")

    tok   = AutoTokenizer.from_pretrained(FAKENEWS_MODEL)
    model = AutoModelForSequenceClassification.from_pretrained(FAKENEWS_MODEL).to(device).eval()

    texts  = df["text_raw"].astype(str).tolist()
    inputs = [f"<title><content>{t}<end>" for t in texts]

    p_real = []
    with torch.no_grad():
        for i in range(0, len(inputs), batch_size):
            chunk = inputs[i:i + batch_size]
            enc = tok(
                chunk,
                max_length=MAX_LENGTH,
                padding=True,
                truncation=True,
                return_tensors="pt",
            ).to(device)
            logits = model(**enc).logits
            probs  = torch.softmax(logits, dim=1)   # col 0 = Fake, col 1 = Real
            p_real.extend(probs[:, 1].cpu().tolist())

    out = df[["post_id"]].copy()
    out["score"] = [float(p) for p in p_real]                       # crédibilité
    out["label"] = ["real" if p >= 0.5 else "fake" for p in p_real]
    print(f"[NLP] Fake news calculé pour {len(out)} posts.")
    return out


def upsert_fakenews(df: pd.DataFrame) -> None:
    _upsert_scores(df, "bluesky_posts_fakenews")