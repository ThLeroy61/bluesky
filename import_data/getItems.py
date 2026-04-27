import functions
import requests
from datetime import datetime, timezone
import time

TEST_MODE = False
MAX_ITEMS = 5 if TEST_MODE else 100000


def fetch_and_store_items(endpoint: str, params: dict, source_name: str):
    """
    Récupère un flux Bluesky page par page et stocke chaque post dans PostgreSQL.

    Stratégie d'upsert :
      - INSERT ... ON CONFLICT (post_id) DO UPDATE ... WHERE posts.cid != EXCLUDED.cid
      → un post existant n'est mis à jour que si son cid a changé (contenu modifié).
      → atomique et sans double requête SELECT préalable.
    """
    token = functions.load_token(functions.LOGIN, functions.PASS)
    if not token:
        raise SystemExit("Impossible d'obtenir un access token.")

    headers = {"Authorization": f"Bearer {token}"}
    total = 0
    inserted = 0
    updated = 0
    cursor = None
    total_items_fetched = 0

    # Une seule connexion réutilisée sur toute la pagination du flux
    conn = functions.get_pg_connection()

    upsert_sql = """
        INSERT INTO posts (
            post_id, cid, source, fetched_at,
            profile_id, profile_name, profile_display_name,
            text_raw, created_at,
            like_count, repost_count, quote_count, bookmark_count, reply_count
        ) VALUES (
            %(post_id)s, %(cid)s, %(source)s, %(fetched_at)s,
            %(profile_id)s, %(profile_name)s, %(profile_display_name)s,
            %(text_raw)s, %(created_at)s,
            %(like_count)s, %(repost_count)s, %(quote_count)s,
            %(bookmark_count)s, %(reply_count)s
        )
        ON CONFLICT (post_id) DO UPDATE SET
            cid                  = EXCLUDED.cid,
            source               = EXCLUDED.source,
            fetched_at           = EXCLUDED.fetched_at,
            profile_display_name = EXCLUDED.profile_display_name,
            text_raw             = EXCLUDED.text_raw,
            like_count           = EXCLUDED.like_count,
            repost_count         = EXCLUDED.repost_count,
            quote_count          = EXCLUDED.quote_count,
            bookmark_count       = EXCLUDED.bookmark_count,
            reply_count          = EXCLUDED.reply_count
        WHERE posts.cid != EXCLUDED.cid   -- mise à jour seulement si le contenu a changé
        RETURNING (xmax = 0) AS was_inserted;
        -- xmax = 0  → ligne nouvellement insérée
        -- xmax != 0 → ligne mise à jour
    """

    try:
        while True:
            if cursor:
                params["cursor"] = cursor

            r = requests.get(f"{functions.API_BASE}/{endpoint}", headers=headers, params=params)

            # Token expiré entre deux pages : refresh et relance
            if r.status_code == 400 and "ExpiredToken" in r.text:
                print("Token expiré, tentative de refresh...")
                token = functions.refresh_token()
                if not token:
                    raise SystemExit("Échec du rafraîchissement du token.")
                headers = {"Authorization": f"Bearer {token}"}
                r = requests.get(f"{functions.API_BASE}/{endpoint}", headers=headers, params=params)

            if r.status_code != 200:
                print(f"Erreur API ({endpoint}) :", r.status_code, r.text)
                break

            data = r.json()

            # Certains endpoints renvoient "posts", d'autres "feed"
            items = data.get("posts") or data.get("feed") or []
            cursor = data.get("cursor")

            if not items:
                print("Aucun item trouvé ou fin de flux.")
                break

            batch_inserted = 0
            batch_updated = 0

            # psycopg v3 : conn.transaction() commit/rollback par batch SANS fermer la connexion.
            # (À ne pas confondre avec `with conn:` de psycopg2 — en v3 ce dernier ferme la connexion.)
            with conn.transaction():
                with conn.cursor() as cur:
                    for it in items:
                        total_items_fetched += 1

                        # Certains flux encapsulent le post sous la clé 'post'
                        post = it.get("post", it)
                        record = post.get("record", {})
                        author = post.get("author", {})
                        row = {
                            "post_id":              post.get("uri"),
                            "cid":                  post.get("cid"),
                            "source":               source_name,
                            "fetched_at":           datetime.now(timezone.utc),
                            "profile_id":           author.get("did"),
                            "profile_name":         author.get("handle", "unknown"),
                            "profile_display_name": author.get("displayName") or "Unknown",
                            "text_raw":             record.get("text", ""),
                            "created_at":           record.get("createdAt"),
                            "like_count":           post.get("likeCount", 0),
                            "repost_count":         post.get("repostCount", 0),
                            "quote_count":          post.get("quoteCount", 0),
                            "bookmark_count":       post.get("bookmarkCount", 0),
                            "reply_count":          post.get("replyCount", 0),
                        }

                        cur.execute(upsert_sql, row)
                        result = cur.fetchone()

                        if result is None:
                            # ON CONFLICT mais cid identique → aucun changement, pas de RETURNING
                            pass
                        elif result[0]:
                            batch_inserted += 1
                            print(f"  ✚ Nouveau  : {row['profile_name']} → {row['text_raw'][:60]!r}")
                        else:
                            batch_updated += 1
                            print(f"  ↺ Mis à jour : {row['profile_name']} ({row['post_id']})")

                        if total_items_fetched >= MAX_ITEMS:
                            print(f"Limite de {MAX_ITEMS} items atteinte, arrêt du flux '{source_name}'.")
                            inserted += batch_inserted
                            updated += batch_updated
                            return

            inserted += batch_inserted
            updated += batch_updated
            total += len(items)
            print(f"Batch traité : {len(items)} posts | ✚{batch_inserted} insérés, ↺{batch_updated} mis à jour | Total : {total} | Cursor : {cursor}\n")

            if not cursor or total_items_fetched >= MAX_ITEMS:
                break

            time.sleep(0.2)

    finally:
        conn.close()

    print(f"Fin du flux '{source_name}' — {inserted} insérés, {updated} mis à jour, {total} lus.")


def get_search_post(query: str, limit: int = 100):
    """Recherche des posts par mot-clé et les stocke dans PostgreSQL."""
    params = {"q": query, "limit": limit}
    fetch_and_store_items(
        "app.bsky.feed.searchPosts",
        params,
        source_name=f"search_{query.replace(' ', '_')}",
    )
