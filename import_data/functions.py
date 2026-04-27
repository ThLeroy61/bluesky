import os
import json
import requests
import psycopg
from dotenv import load_dotenv

load_dotenv(encoding="utf-8-sig")

LOGIN = os.getenv("LOGIN_BLUESKY")
PASS = os.getenv("PASS_BLUESKY")

TARGET = [
    {"handle": "aoc.bsky.social", "name": "Alexandria Ocasio-Cortez"}
]

SEARCH_QUERIES = {
    "trending": ["breaking", "urgent", "live", "developing"],
    "politics": ["politics", "election", "vote", "campaign", "congress", "senate", "representative"],
    "health": ["covid", "vaccine", "health", "disease", "pandemic", "outbreak", "virus"],
    "science": ["science", "research", "study", "climate", "environment", "discovery"],
    "tech": ["ai", "tech", "startup", "innovation", "algorithm", "data", "blockchain"],
    "economy": ["inflation", "market", "recession", "jobs", "unemployment", "gdp", "currency"],
    "conspiracy": ["conspiracy", "hoax", "fake", "misinformation", "disinformation"],
    "social": ["social media", "twitter", "facebook", "instagram", "tiktok"],
}

API_BASE = "https://bsky.social/xrpc"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(BASE_DIR, "token.json")

# ── PostgreSQL ────────────────────────────────────────────────────────────────

def get_pg_connection():
    """Connexion PostgreSQL via psycopg v3 (gestion Unicode robuste sur Windows)."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "bluesky_posts_clean"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        client_encoding="UTF8",
    )


def ensure_table():
    """
    Crée la table 'posts' si elle n'existe pas encore.
    post_id (URI Bluesky) est la clé primaire : garantit l'unicité côté DB.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS posts (
        post_id             TEXT        PRIMARY KEY,
        cid                 TEXT,
        source              TEXT,
        fetched_at          TIMESTAMPTZ,
        profile_id          TEXT,
        profile_name        TEXT,
        profile_display_name TEXT,
        text_raw            TEXT,
        created_at          TIMESTAMPTZ,
        like_count          INTEGER     DEFAULT 0,
        repost_count        INTEGER     DEFAULT 0,
        quote_count         INTEGER     DEFAULT 0,
        bookmark_count      INTEGER     DEFAULT 0,
        reply_count         INTEGER     DEFAULT 0
    );
    """
    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
        print("Table 'posts' prête.")
    finally:
        conn.close()


# ── Auth Bluesky ──────────────────────────────────────────────────────────────

def login(identifier: str, password: str, timeout: int = 10):
    """Authentification auprès de Bluesky, persistance des tokens dans token.json."""
    url = f"{API_BASE}/com.atproto.server.createSession"
    payload = {"identifier": identifier, "password": password}

    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code != 200:
            print("Erreur de login :", r.status_code, r.text)
            return None
        data = r.json()
        access = data.get("accessJwt")
        refresh = data.get("refreshJwt")
        if not access:
            print("Login échoué — aucun accessJwt reçu")
            return None

        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump({"accessJwt": access, "refreshJwt": refresh}, f, ensure_ascii=False, indent=2)
        print("Connexion OK — token enregistré dans token.json")
        return access
    except Exception as e:
        print("Erreur login :", e)
        return None


def load_token(identifier: str = None, password: str = None):
    """Charge l'accessJwt depuis token.json, ou tente une connexion si absent."""
    if not os.path.exists(TOKEN_FILE):
        print(f"Aucun token trouvé à {TOKEN_FILE}")
        if identifier and password:
            print("Tentative de connexion automatique...")
            return login(identifier, password)
        raise FileNotFoundError("token.json introuvable et aucune credential fournie")

    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("accessJwt")
    except json.JSONDecodeError as e:
        print("Token corrompu :", e)
        if identifier and password:
            return login(identifier, password)


def refresh_token():
    """Renouvelle le token d'accès via le refreshJwt, sans ressaisir les credentials."""
    if not os.path.exists(TOKEN_FILE):
        print("Aucun fichier de token trouvé pour le refresh.")
        return login(LOGIN, PASS)

    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            tokens = json.load(f)
            refresh = tokens.get("refreshJwt")
            if not refresh:
                print("Aucun refresh token trouvé, reconnexion...")
                return login(LOGIN, PASS)

        url = f"{API_BASE}/com.atproto.server.refreshSession"
        headers = {"Authorization": f"Bearer {refresh}"}
        r = requests.post(url, headers=headers, timeout=10)

        if r.status_code != 200:
            print("Échec du refresh :", r.status_code, r.text)
            return login(LOGIN, PASS)

        data = r.json()
        new_access = data.get("accessJwt")
        new_refresh = data.get("refreshJwt")

        if not new_access:
            print("Pas de nouveau token reçu, reconnexion complète...")
            return login(LOGIN, PASS)

        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump({"accessJwt": new_access, "refreshJwt": new_refresh}, f, ensure_ascii=False, indent=2)
        print("Token rafraîchi avec succès.")
        return new_access

    except Exception as e:
        print("Erreur lors du refresh :", e)
        return login(LOGIN, PASS)
