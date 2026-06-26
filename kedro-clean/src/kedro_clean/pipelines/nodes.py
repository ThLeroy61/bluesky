import os
import re
import unicodedata

import pandas as pd
from sqlalchemy import create_engine, text
from lingua import Language, LanguageDetectorBuilder

# Détecteur de langue : instancié une fois au chargement du module.
# Coûteux à construire, donc singleton réutilisé par tous les batchs.
detector = LanguageDetectorBuilder.from_languages(
    Language.FRENCH, Language.ENGLISH
).build()

URL_RE = re.compile(r"http\S+|www\S+")


def extract_text_signals(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    text_series = df["text_raw"].fillna("")

    df["nb_urls"]         = text_series.str.count(URL_RE)
    df["nb_exclamations"] = text_series.str.count("!")
    df["nb_questions"]    = text_series.str.count(r"\?")
    df["text_length"]     = text_series.str.len()
    df["word_count"]      = text_series.str.split().str.len().fillna(0).astype(int)

    upper = text_series.str.count(r"[A-Z]")
    alpha = text_series.str.count(r"[A-Za-z]")
    df["ratio_uppercase"] = (upper / alpha).fillna(0)

    df["avg_word_length"] = (
        df["text_length"] / df["word_count"]
    ).replace([float("inf")], 0).fillna(0)

    return df


def clean_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def normalize_text(s):
        if not isinstance(s, str):
            return ""
        s = unicodedata.normalize("NFKC", s)
        s = s.lower()
        s = re.sub(r"http\S+|www\S+", "", s)         # URLs
        s = re.sub(r"@\w+", "", s)                   # mentions
        s = re.sub(r"#(\w+)", r"\1", s)              # hashtag → mot
        s = re.sub(
            "["
            "\U0001F600-\U0001F64F"
            "\U0001F300-\U0001F5FF"
            "\U0001F680-\U0001F6FF"
            "\U0001F1E0-\U0001F1FF"
            "]+",
            "",
            s,
        )
        s = re.sub(r"[^\w\s!?\.]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    df["text_clean"] = df["text_raw"].apply(normalize_text)
    return df


def validate_quality(df: pd.DataFrame, min_length: int = 15, allowed_languages=("fr", "en")) -> pd.DataFrame:
    df = df.copy()
    df = df[df["text_clean"].notna()]
    df = df[df["text_clean"].str.len() >= min_length]

    def detect_lang(s: str):
        result = detector.detect_language_of(s)
        if result == Language.FRENCH:
            return "fr"
        if result == Language.ENGLISH:
            return "en"
        return None

    df["lang_detected"] = df["text_clean"].apply(detect_lang)
    df = df[df["lang_detected"].isin(allowed_languages)]
    df = df.drop_duplicates(subset="text_clean")
    return df.reset_index(drop=True)


def _build_con_string() -> str:
    user = os.getenv("POSTGRES_USER", "airflow")
    pwd  = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "bluesky_clean")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def upsert_posts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        print("Rien à upserter, batch vide.")
        return df

    engine = create_engine(_build_con_string())

    target_cols = [
        "post_id", "cid", "source", "fetched_at",
        "profile_id", "profile_name", "profile_display_name",
        "text_raw", "text_clean", "lang_detected", "created_at",
        "like_count", "repost_count", "quote_count", "bookmark_count", "reply_count",
        "nb_urls", "nb_exclamations", "nb_questions",
        "text_length", "word_count", "ratio_uppercase", "avg_word_length",
    ]

    # On sécurise : si une colonne n'a pas été produite (ex: validate_quality
    # filtre tout, ou un node oublié plus tard), on la remplit à None.
    for col in target_cols:
        if col not in df.columns:
            df[col] = None
    df = df[target_cols]

    temp_table = "bluesky_posts_clean_tmp"
    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    cols_sql = ", ".join(target_cols)
    update_clause = ", ".join([
        "text_clean      = EXCLUDED.text_clean",
        "lang_detected   = EXCLUDED.lang_detected",
        "cleaned_at      = NOW()",
        "like_count      = EXCLUDED.like_count",
        "repost_count    = EXCLUDED.repost_count",
        "quote_count     = EXCLUDED.quote_count",
        "bookmark_count  = EXCLUDED.bookmark_count",
        "reply_count     = EXCLUDED.reply_count",
    ])

    with engine.begin() as conn:
        result = conn.execute(text(f"""
            INSERT INTO bluesky_posts_clean ({cols_sql})
            SELECT {cols_sql} FROM {temp_table}
            ON CONFLICT (post_id) DO UPDATE SET
                {update_clause}
        """))
        conn.execute(text(f"DROP TABLE {temp_table}"))
        print(f"[CLEAN] {result.rowcount} lignes upsertées dans bluesky_posts_clean.")

    return df