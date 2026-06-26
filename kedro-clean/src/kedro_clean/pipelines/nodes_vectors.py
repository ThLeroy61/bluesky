import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer, ENGLISH_STOP_WORDS
from sklearn.pipeline import Pipeline
from sqlalchemy import create_engine, text

FRENCH_STOP_WORDS = {
    "le","la","les","un","des","de","du","en","et","est","que","qui","dans",
    "pour","pas","sur","par","se","au","ce","il","elle","nous","vous","ils",
    "elles","je","tu","on","mon","ton","son","ma","ta","sa","mes","tes","ses",
    "mais","ou","donc","or","ni","car","plus","bien","aussi","comme","tout",
    "avec","sans","sous","lors","très","leur","leurs","aux","cet","cette","ces",
    "être","avoir","faire","dit","va","ont","été","une","dont","où","si","même"
}

STOP_WORDS = list(ENGLISH_STOP_WORDS.union(FRENCH_STOP_WORDS))

N_COMPONENTS = 300
MAX_FEATURES = 50_000

MODELS_DIR     = Path("/opt/kedro-clean/data/06_models")
MODEL_PATH     = MODELS_DIR / "lsa_pipeline.pkl"
FIT_COUNT_PATH = MODELS_DIR / "lsa_fit_count.txt"


def _build_con_string() -> str:
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd  = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "bluesky_clean")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _load_model() -> Pipeline | None:
    if MODEL_PATH.exists():
        print(f"[VECTORS] Modèle chargé depuis {MODEL_PATH}")
        return joblib.load(MODEL_PATH)
    print("[VECTORS] Aucun modèle existant, un fit sera effectué.")
    return None


def _save_model(lsa: Pipeline, corpus_size: int) -> None:
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(lsa, MODEL_PATH)
    FIT_COUNT_PATH.write_text(str(corpus_size))
    print(f"[VECTORS] Modèle sauvegardé ({corpus_size} posts au fit).")


def _last_fit_count() -> int:
    if FIT_COUNT_PATH.exists():
        return int(FIT_COUNT_PATH.read_text().strip())
    return 0


def _build_lsa(n_components: int) -> Pipeline:
    return Pipeline([
        ("tfidf", TfidfVectorizer(
            max_features=MAX_FEATURES,
            min_df=2,
            max_df=0.8,
            sublinear_tf=True,
            stop_words=STOP_WORDS,
        )),
        ("svd", TruncatedSVD(
            n_components=n_components,
            random_state=42,
            n_iter=5,
        )),
    ])


def _normalize(embeddings: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return embeddings / norms


def _pad_to_n_components(embeddings: np.ndarray) -> np.ndarray:
    if embeddings.shape[1] < N_COMPONENTS:
        pad = np.zeros((embeddings.shape[0], N_COMPONENTS - embeddings.shape[1]))
        embeddings = np.hstack([embeddings, pad])
    return embeddings


def load_posts_to_vectorize(refit_threshold: float) -> dict:
    engine = create_engine(_build_con_string())

    new_posts_query = text("""
        SELECT c.post_id, c.text_clean
        FROM bluesky_posts_clean c
        LEFT JOIN bluesky_posts_vectors v ON c.post_id = v.post_id
        WHERE v.post_id IS NULL
          AND c.text_clean IS NOT NULL
          AND LENGTH(c.text_clean) > 0
        ORDER BY c.cleaned_at DESC
    """)

    total_query = text("""
        SELECT COUNT(*) AS n
        FROM bluesky_posts_clean
        WHERE text_clean IS NOT NULL AND LENGTH(text_clean) > 0
    """)

    with engine.connect() as conn:
        new_posts   = pd.read_sql(new_posts_query, conn)
        total_clean = conn.execute(total_query).scalar()

    n_at_fit    = _last_fit_count()
    n_since_fit = total_clean - n_at_fit
    ratio       = (n_since_fit / n_at_fit) if n_at_fit > 0 else 1.0
    needs_refit = _load_model() is None or ratio >= refit_threshold

    print(f"[VECTORS] Corpus total : {total_clean} | Au dernier fit : {n_at_fit} | Nouveaux : {len(new_posts)}")

    if needs_refit:
        print(f"[VECTORS] Refit nécessaire (ratio={ratio:.1%} >= seuil={refit_threshold:.1%}). Chargement du corpus complet...")
        full_corpus_query = text("""
            SELECT post_id, text_clean
            FROM bluesky_posts_clean
            WHERE text_clean IS NOT NULL AND LENGTH(text_clean) > 0
        """)
        with engine.connect() as conn:
            full_corpus = pd.read_sql(full_corpus_query, conn)
    else:
        print(f"[VECTORS] Pas de refit (ratio={ratio:.1%} < seuil={refit_threshold:.1%}). Transform uniquement.")
        full_corpus = pd.DataFrame(columns=["post_id", "text_clean"])

    return {
        "new_posts":       new_posts,
        "full_corpus":     full_corpus,
        "needs_refit":     needs_refit,
        "total_clean":     total_clean,
        "refit_threshold": refit_threshold,
    }


def compute_vectors(payload: dict) -> pd.DataFrame:
    new_posts   = payload["new_posts"]
    full_corpus = payload["full_corpus"]
    needs_refit = payload["needs_refit"]
    total_clean = payload["total_clean"]

    if needs_refit:
        fit_corpus = full_corpus["text_clean"].astype(str).tolist()
        n_fit      = len(fit_corpus)

        n_components = min(N_COMPONENTS, n_fit - 1, MAX_FEATURES - 1)
        if n_components < 2:
            print(f"[VECTORS] Corpus trop petit pour SVD ({n_fit} posts).")
            return pd.DataFrame(columns=["post_id", "embedding"])

        print(f"[VECTORS] Fit LSA({n_components}) sur {n_fit} posts...")
        lsa = _build_lsa(n_components)
        lsa.fit(fit_corpus)
        _save_model(lsa, total_clean)

        # Refit → on re-vectorise TOUT le corpus pour cohérence des espaces
        print(f"[VECTORS] Transform sur {n_fit} posts (corpus complet)...")
        embeddings = lsa.transform(fit_corpus)
        embeddings = _normalize(_pad_to_n_components(embeddings))
        result = full_corpus[["post_id"]].copy()

    else:
        if new_posts.empty:
            print("[VECTORS] Aucun nouveau post à vectoriser.")
            return pd.DataFrame(columns=["post_id", "embedding"])

        lsa = _load_model()
        new_corpus = new_posts["text_clean"].astype(str).tolist()

        print(f"[VECTORS] Transform sur {len(new_corpus)} nouveaux posts...")
        embeddings = lsa.transform(new_corpus)
        embeddings = _normalize(_pad_to_n_components(embeddings))
        result = new_posts[["post_id"]].copy()

    result["embedding"] = [row.tolist() for row in embeddings]
    print(f"[VECTORS] Embeddings prêts : shape={embeddings.shape}")
    return result


def upsert_vectors(df: pd.DataFrame) -> None:
    if df.empty or df["embedding"].isna().all():
        print("[VECTORS] Rien à upserter.")
        return

    df = df.dropna(subset=["embedding"]).copy()
    engine = create_engine(_build_con_string())

    df["embedding_str"] = df["embedding"].apply(
        lambda v: "[" + ",".join(f"{x:.8f}" for x in v) + "]"
    )

    rows = df[["post_id", "embedding_str"]].to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TEMPORARY TABLE vectors_tmp (
                post_id   TEXT,
                embedding TEXT
            ) ON COMMIT DROP
        """))
        conn.execute(
            text("INSERT INTO vectors_tmp (post_id, embedding) VALUES (:post_id, :embedding_str)"),
            rows,
        )
        result = conn.execute(text("""
            INSERT INTO bluesky_posts_vectors (post_id, embedding)
            SELECT post_id, embedding::vector
            FROM vectors_tmp
            ON CONFLICT (post_id) DO UPDATE
                SET embedding = EXCLUDED.embedding
        """))

    print(f"[VECTORS] {result.rowcount} vecteurs upsertés dans bluesky_posts_vectors.")
