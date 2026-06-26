import os
import sys

from dotenv import load_dotenv
load_dotenv(encoding="utf-8-sig")

import streamlit as st
import pandas as pd
from datetime import date

from predict import get_clusters, summarize_clusters

st.set_page_config(page_title="Bluesky Clustering", layout="wide")
st.title("Bluesky — Clustering par thème")

# ── Sidebar : filtres ─────────────────────────────────────────────────────────
with st.sidebar:
    if st.button("⚡ Suivi énergétique", use_container_width=True):
        st.switch_page("pages/2_Suivi_energetique.py")

    st.header("Filtres")

    n_posts = st.slider("Nombre de posts", 500, 50000, 10000, step=500)

    st.subheader("Période")
    mode = st.radio("Mode", ["X derniers jours", "Plage de dates"], index=0)

    days_back  = None
    date_start = None
    date_end   = None

    if mode == "X derniers jours":
        days_back = st.slider("Jours en arrière", 1, 30, 7)
    else:
        col1, col2 = st.columns(2)
        with col1:
            date_start = st.date_input("Début", value=date(2026, 1, 1))
        with col2:
            date_end = st.date_input("Fin", value=date.today())
        date_start = date_start.isoformat()
        date_end   = date_end.isoformat()

    st.subheader("Autres filtres")

    cluster_filter = st.number_input(
        "Cluster (laisser à -1 pour tous)", min_value=-1, value=-1, step=1
    )
    cluster_id = int(cluster_filter) if cluster_filter >= 0 else None

    keyword = st.text_input("Mot-clé dans le texte (optionnel)", value="")
    keyword = keyword.strip() or None

    # ── Filtres NLP ────────────────────────────────────────────────────────────
    sentiment_sel = st.multiselect(
        "Tonalité", ["positive", "neutral", "negative"], default=[]
    )
    sentiment = sentiment_sel or None

    cred_threshold = st.slider(
        "Crédibilité max (1.0 = pas de filtre)", 0.0, 1.0, 1.0, step=0.05,
        help="Ne garder que les posts dont le score de crédibilité est ≤ ce seuil "
             "(0 = très douteux, 1 = très fiable). Utile pour isoler les posts suspects.",
    )
    max_credibility = cred_threshold if cred_threshold < 1.0 else None

    st.subheader("Affichage")
    n_top_words = st.slider("Top mots par cluster", 5, 20, 10)
    n_examples  = st.slider("Exemples par cluster", 1, 10, 3)

    run = st.button("Lancer l'analyse", type="primary", use_container_width=True)

# ── Helpers d'affichage ───────────────────────────────────────────────────────
SENTIMENT_EMOJI = {"positive": "🟢", "neutral": "⚪", "negative": "🔴"}


def credibility_badge(score):
    if score is None or pd.isna(score):
        return "crédibilité : —"
    pct = round(float(score) * 100)
    flag = "⚠️ " if score < 0.5 else ""
    return f"{flag}crédibilité : {pct}%"

# ── Résultats ─────────────────────────────────────────────────────────────────
if run:
    with st.spinner("Récupération des posts..."):
        df = get_clusters(
            n_posts=n_posts,
            days_back=days_back if mode == "X derniers jours" else None,
            date_start=date_start if mode == "Plage de dates" else None,
            date_end=date_end if mode == "Plage de dates" else None,
            cluster_id=cluster_id,
            keyword=keyword,
            sentiment=sentiment,
            max_credibility=max_credibility,
        )

    if df.empty:
        st.warning("Aucun post trouvé avec ces paramètres.")
        st.stop()

    st.success(f"{len(df)} posts récupérés — {df['cluster_id'].nunique()} clusters")

    # ── Vue globale ───────────────────────────────────────────────────────────
    with st.expander("Vue globale — répartition des clusters", expanded=True):
        cluster_counts = (
            df.groupby("cluster_id")
            .size()
            .reset_index(name="nb_posts")
            .sort_values("nb_posts", ascending=False)
        )
        st.bar_chart(cluster_counts.set_index("cluster_id")["nb_posts"])

        # Vue d'ensemble tonalité + crédibilité sur l'échantillon filtré
        st.caption("Répartition des tonalités")
        sent_global = df["sentiment_label"].value_counts(dropna=True)
        if not sent_global.empty:
            st.bar_chart(sent_global)
        else:
            st.info("Aucun score de sentiment disponible.")

    # ── Résumé par cluster ────────────────────────────────────────────────────
    with st.spinner("Calcul des résumés..."):
        summary = summarize_clusters(df, n_top_words=n_top_words, n_examples=n_examples)

    for cid, info in sorted(summary.items()):
        s = info["stats"]
        st.divider()
        st.subheader(f"Cluster {cid} — {s['n_posts']} posts")

        # Bandeau NLP : tonalité dominante + crédibilité moyenne + % fake
        sent_counts = s["sentiment_counts"]
        dominant = max(sent_counts, key=sent_counts.get) if sent_counts else None
        b1, b2, b3 = st.columns(3)
        b1.metric(
            "Tonalité dominante",
            f"{SENTIMENT_EMOJI.get(dominant, '')} {dominant}" if dominant else "—",
        )
        b2.metric(
            "Crédibilité moyenne",
            f"{round(s['avg_credibility'] * 100)}%" if s["avg_credibility"] is not None else "—",
        )
        b3.metric(
            "Posts classés fake",
            f"{s['pct_fake']}%" if s["pct_fake"] is not None else "—",
            help=f"Sur {s['n_scored_fake']} posts scorés dans ce cluster",
        )

        # Top mots — pleine largeur
        st.markdown("**Top mots**")
        words_df = pd.DataFrame(info["top_words"], columns=["mot", "score TF-IDF"])
        words_df["score TF-IDF"] = words_df["score TF-IDF"].round(3)
        st.dataframe(words_df, hide_index=True, use_container_width=True)

        # Exemples de posts — avec badges sentiment + crédibilité
        st.markdown("**Exemples de posts**")
        for ex in info["examples"]:
            with st.container(border=True):
                emoji = SENTIMENT_EMOJI.get(ex.get("sentiment_label"), "")
                st.caption(
                    f"{ex['profile_name']} · {str(ex['created_at'])[:16]} · "
                    f"{emoji} {ex.get('sentiment_label') or '—'} · "
                    f"{credibility_badge(ex.get('credibility_score'))}"
                )
                st.write(ex["text_raw"])

        # Stats engagement
        st.markdown("**Stats**")
        langs = ", ".join(f"{l} ({n})" for l, n in s["lang_counts"].items()) if s["lang_counts"] else "—"
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Likes moy.", s["avg_likes"])
        c2.metric("Reposts moy.", s["avg_reposts"])
        c3.metric("Réponses moy.", s["avg_replies"])
        c4.metric("URLs moy.", s["avg_urls"])
        c5.metric("Exclamations moy.", s["avg_exclamations"])
        c6.metric("Ratio majuscules", s["avg_uppercase"])
        st.caption(f"Langues : {langs}")
