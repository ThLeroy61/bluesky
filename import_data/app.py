import getItems


def get_author_feed(actor: str, limit: int = 100):
    """Récupère les posts du fil d'un auteur spécifique."""
    params = {"actor": actor, "limit": limit}
    getItems.fetch_and_store_items("app.bsky.feed.getAuthorFeed", params, source_name="author_feed")


def get_timeline(limit: int = 100):
    """Récupère le fil d'actualité général du compte connecté."""
    params = {"limit": limit}
    getItems.fetch_and_store_items("app.bsky.feed.getTimeline", params, source_name="timeline")


if __name__ == "__main__":
    if not getItems.functions.LOGIN or not getItems.functions.PASS:
        raise SystemExit("LOGIN_BLUESKY et PASS_BLUESKY non définis dans .env")

    # Création de la table si elle n'existe pas encore
    getItems.functions.ensure_table()

    # Récupération des fils des comptes ciblés
    for target in getItems.functions.TARGET:
        get_author_feed(target["handle"])

    # Récupération du fil général
    get_timeline()

    # Récupération par mots-clés, organisés par catégorie
    for collection, queries in getItems.functions.SEARCH_QUERIES.items():
        print(f"\n=== Traitement catégorie : {collection} ===")
        for q in queries:
            getItems.get_search_post(q, limit=100)
