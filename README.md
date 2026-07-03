# Bluesky Clustering — Pipeline d'analyse de posts (NLP + ML)

Pipeline de données complet qui ingère des posts publics [Bluesky](https://bsky.social), les nettoie, les vectorise (TF-IDF + LSA), les regroupe par thème (MiniBatchKMeans), puis leur applique une analyse de sentiment et une détection de fake news. Le tout est orchestré par **Airflow**, construit avec **Kedro**, stocké dans **PostgreSQL/pgvector**, et exposé via un dashboard **Streamlit**. Le suivi de la consommation énergétique des pipelines est assuré par **CodeCarbon**.

## Sommaire

- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Structure du projet](#structure-du-projet)
- [Prérequis](#prérequis)
- [Installation](#installation)
- [Lancer le projet](#lancer-le-projet)
- [Pipelines Airflow / Kedro](#pipelines-airflow--kedro)
- [Schéma de base de données](#schéma-de-base-de-données)
- [Dashboard Streamlit](#dashboard-streamlit)
- [Suivi énergétique](#suivi-énergétique)
- [Dépannage](#dépannage)

## Architecture

```
Bluesky API
    │
    ▼
[import_data/] ──── Airflow DAG "bluesky_ingest" ────► table `posts` (PostgreSQL)
                                                              │
                                                              ▼
                                    Airflow DAG "bluesky_clean"
                                    → Kedro pipeline "data_engineering"
                                    (nettoyage, langue, signaux textuels)
                                                              │
                                                              ▼
                                                   `bluesky_posts_clean`
                                                              │
                          ┌───────────────────────────────────┼───────────────────────────────────┐
                          ▼                                                                         ▼
        Airflow DAG "bluesky_vectors"                                          Kedro pipeline "ml" (nodes_nlp.py)
        → Kedro pipeline "vectors"                                             - Sentiment (XLM-RoBERTa)
        (TF-IDF + TruncatedSVD/LSA)                                            - Fake news (RoBERTa)
                          │                                                                         │
                          ▼                                                                         ▼
              `bluesky_posts_vectors`                                    `bluesky_posts_sentiment` / `bluesky_posts_fakenews`
                          │
                          ▼
        Airflow DAG "bluesky_ml_retrain"
        → Kedro pipeline "ml" (nodes_ml.py)
        (MiniBatchKMeans)
                          │
                          ▼
              `bluesky_posts_clusters`
                          │
                          ▼
                Dashboard Streamlit (predict.py / app.py)
```

Chaque étape Kedro est déclenchée par Airflow via `BashOperator` (`kedro run --pipeline=...`), sauf l'ingestion qui utilise des `@task` Airflow natifs (un par source : auteur ciblé, timeline, catégorie de recherche).

## Stack technique

| Domaine | Outils |
|---|---|
| Orchestration | Apache Airflow (CeleryExecutor, Redis, PostgreSQL) |
| Pipeline de données | Kedro 1.1.1 + kedro-airflow + kedro-datasets |
| Base de données | PostgreSQL 16 + extension `pgvector` |
| ML / NLP | scikit-learn (TF-IDF, TruncatedSVD, MiniBatchKMeans), HuggingFace Transformers, PyTorch |
| Modèles HF | `cardiffnlp/twitter-xlm-roberta-base-sentiment` (sentiment), `hamzab/roberta-fake-news-classification` (fake news) |
| Dashboard | Streamlit |
| Suivi énergétique | CodeCarbon |
| Conteneurisation | Docker / Docker Compose |

## Structure du projet

```
.
├── .env                        # Variables d'environnement (non versionné)
├── docker-compose.yaml         # Orchestration Airflow + PostgreSQL + Redis
├── Dockerfile                  # Image Airflow custom avec les dépendances du projet
├── docker_requirements.txt     # Dépendances installées dans l'image Docker
├── requirements.txt            # Dépendances pour l'environnement local (import_data)
│
├── airflow/
│   └── dags/
│       ├── bluesky_ingest_dag.py     # Ingestion Bluesky → table `posts`
│       ├── bluesky_clean_dag.py      # Nettoyage → `bluesky_posts_clean`
│       ├── bluesky_vectors_dag.py    # Vectorisation TF-IDF/LSA → `bluesky_posts_vectors`
│       └── ml_retrain_dag.py         # Clustering + NLP → `bluesky_posts_clusters`, sentiment, fake news
│
├── import_data/
│   ├── app.py                  # Points d'entrée (author feed, timeline)
│   ├── functions.py            # Auth Bluesky, connexion PostgreSQL, config des sources
│   ├── getItems.py             # Pagination API Bluesky + upsert PostgreSQL
│   └── token.json              # Cache du token de session (⚠️ ne jamais versionner)
│
├── init-db/
│   ├── 01_init.sql             # Création de la base + extension vector
│   ├── 02_schema.sql           # Tables `posts`, `bluesky_posts_clean`, `bluesky_posts_vectors`
│   ├── 03_clusters.sql         # Table `bluesky_posts_clusters`
│   └── 04_sentiment_fakenews.sql  # Tables `bluesky_posts_sentiment`, `bluesky_posts_fakenews`
│
├── kedro-clean/
│   ├── conf/base/
│   │   ├── catalog.yml         # Déclaration des datasets Kedro (non fourni ici, à créer)
│   │   └── parameters.yml      # Paramètres des pipelines (voir ci-dessous)
│   ├── data/
│   │   ├── 06_models/          # Modèles sérialisés (lsa_pipeline.pkl, kmeans.pkl, ...)
│   │   └── 08_reporting/       # emissions.csv (CodeCarbon)
│   └── src/kedro_clean/
│       ├── hooks.py            # Hook CodeCarbon (mesure énergie par run de pipeline)
│       ├── pipeline_registry.py
│       ├── settings.py
│       └── pipelines/
│           ├── nodes.py / pipeline.py             # Nettoyage texte + détection de langue
│           ├── nodes_vectors.py / pipeline_vectors.py  # TF-IDF + LSA
│           ├── nodes_ml.py                        # MiniBatchKMeans
│           ├── nodes_nlp.py                        # Sentiment + fake news
│           └── pipeline_ml.py                      # Regroupe clustering + NLP
│
└── streamlit/
    ├── app.py                  # Dashboard principal (filtres, clusters, sentiment, crédibilité)
    ├── predict.py               # Requêtes PostgreSQL + résumé des clusters
    └── pages/
        └── 2_Suivi_energetique.py  # Visualisation des émissions CodeCarbon
```

## Prérequis

- Docker et Docker Compose (v2)
- Au moins **4 Go de RAM** et **2 CPU** alloués à Docker (recommandé par Airflow), 10 Go d'espace disque libre
- Python 3.11 en local si vous souhaitez lancer le dashboard Streamlit hors conteneur
- Un compte Bluesky (identifiant + mot de passe d'application) pour l'ingestion

## Installation

### 1. Cloner le projet

```bash
git clone <url-du-repo>
cd <nom-du-repo>
```

### 2. Configurer les variables d'environnement

Créer un fichier `.env` à la racine (non versionné) avec au minimum :

```dotenv
# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# PostgreSQL (utilisé en local, hors conteneur, par exemple pour Streamlit)
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_DB=bluesky_clean
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# Authentification Bluesky (compte utilisé pour l'ingestion)
LOGIN_BLUESKY=votre-handle.bsky.social
PASS_BLUESKY=votre-mot-de-passe-application

# CodeCarbon / reporting
EMISSIONS_DIR=/opt/kedro-clean/data/08_reporting
```

> ⚠️ En conteneur Docker, `docker-compose.yaml` **surcharge** automatiquement `POSTGRES_HOST` sur `postgres` (nom du service réseau) et les identifiants sur `airflow`/`airflow` — ces valeurs du `.env` ne servent donc qu'à l'exécution **locale** (Streamlit hors Docker, scripts `import_data` en local, etc.).

Utilisez de préférence un **App Password** Bluesky (Paramètres → Confidentialité et sécurité → App Passwords) plutôt que votre mot de passe principal.

### 3. Build de l'image Docker

```bash
docker compose build
```

## Lancer le projet

### 1. Initialiser Airflow

```bash
docker compose up airflow-init
```

### 2. Démarrer tous les services

```bash
docker compose up -d
```

Services exposés :

| Service | URL | Identifiants par défaut |
|---|---|---|
| Airflow Webserver | http://localhost:8081 | `airflow` / `airflow` |
| PostgreSQL | `localhost:5432` | `airflow` / `airflow` (db `airflow`) |

La base applicative du pipeline est **`bluesky_clean`** (créée par les scripts SQL de `init-db/`, montés sur `/docker-entrypoint-initdb.d` du conteneur `postgres`), à ne pas confondre avec la base `airflow` utilisée en interne par Airflow.

### 3. Activer les DAGs

Les DAGs sont créés en pause par défaut (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION`). Dans l'UI Airflow, activez-les **dans cet ordre** (ou laissez les dépendances de données faire foi si vous les déclenchez manuellement) :

1. `bluesky_ingest` — alimente la table `posts`
2. `bluesky_clean` — nettoie vers `bluesky_posts_clean`
3. `bluesky_vectors` — vectorise vers `bluesky_posts_vectors`
4. `bluesky_ml_retrain` — clustering + sentiment + fake news

Chaque DAG est planifié en `@daily`, avec `max_active_runs=1`.

### 4. Lancer le dashboard Streamlit

En local (hors Docker), avec les variables du `.env` pointant vers `127.0.0.1:5432` :

```bash
cd streamlit
pip install -r ../requirements.txt streamlit pandas sqlalchemy psycopg2-binary joblib scikit-learn python-dotenv
streamlit run app.py
```

Le dashboard nécessite que `kedro-clean/data/06_models/lsa_pipeline.pkl` existe déjà (généré par le pipeline `vectors`), car `predict.py` charge le vectorizer TF-IDF pour calculer les mots-clés par cluster.

## Pipelines Airflow / Kedro

### `bluesky_ingest` (Airflow natif, sans Kedro)

Récupère les posts Bluesky via l'API publique (`app.bsky.feed.*`) et les upsert dans la table `posts` :
- un feed par compte ciblé (`TARGET` dans `functions.py`)
- le fil d'actualité du compte connecté
- une recherche par mots-clés, organisée par catégorie (`SEARCH_QUERIES` : trending, politics, health, science, tech, economy, conspiracy, social)

Gère le rafraîchissement automatique du token (access/refresh JWT) en cas d'expiration.

### `data_engineering` (Kedro, DAG `bluesky_clean`)

1. `extract_text_signals` — signaux bruts (nb d'URL, majuscules, ponctuation, longueur…)
2. `clean_text` — normalisation Unicode, suppression URLs/mentions/emojis, hashtags → mots
3. `validate_quality` — filtre longueur minimale, détection de langue (FR/EN via `lingua`), déduplication
4. `upsert_posts` — upsert dans `bluesky_posts_clean`

### `vectors` (Kedro, DAG `bluesky_vectors`)

Pipeline TF-IDF + LSA (`TruncatedSVD`, 300 composantes) avec **stratégie de refit adaptative** : le modèle n'est ré-entraîné que si le nombre de nouveaux posts depuis le dernier fit dépasse `refit_threshold` (sinon, simple `.transform()` sur les nouveaux posts). Le modèle est sérialisé dans `data/06_models/lsa_pipeline.pkl`.

### `ml` (Kedro, DAG `bluesky_ml_retrain`)

- **Clustering** (`nodes_ml.py`) : `MiniBatchKMeans`, même logique de refit adaptative que pour le LSA, modèle sauvegardé dans `data/06_models/kmeans.pkl`.
- **Sentiment** (`nodes_nlp.py`) : modèle HuggingFace multilingue, labels `positive` / `neutral` / `negative`.
- **Fake news** (`nodes_nlp.py`) : modèle HuggingFace entraîné sur des articles de presse longs, appliqué ici à des posts courts — les scores sont donc indicatifs plutôt qu'une vérité absolue (à mentionner explicitement dans toute restitution des résultats).

Ces trois traitements tournent sur GPU si disponible (`torch.cuda.is_available()`), sinon CPU.

### Paramètres Kedro (`conf/base/parameters.yml`)

Les pipelines attendent au minimum les clés suivantes (fichier à créer, non fourni dans ce dépôt) :

```yaml
refit_threshold: 0.2        # % de nouveaux posts déclenchant un refit LSA/KMeans
n_clusters: 15               # nombre de clusters KMeans
kmeans_batch_size: 1024
sentiment_batch_size: 32
fakenews_batch_size: 16
min_text_length: 15
allowed_languages: ["fr", "en"]
```

## Schéma de base de données

Toutes les tables vivent dans la base `bluesky_clean` (créée par `init-db/01_init.sql`) :

| Table | Rôle | Clé |
|---|---|---|
| `posts` | Données brutes ingérées depuis Bluesky | `post_id` |
| `bluesky_posts_clean` | Posts nettoyés + signaux textuels + langue | `post_id` |
| `bluesky_posts_vectors` | Embeddings LSA (`VECTOR(300)`, pgvector) | `post_id` → FK `bluesky_posts_clean` |
| `bluesky_posts_clusters` | Cluster assigné par post | `post_id` → FK `bluesky_posts_vectors` |
| `bluesky_posts_sentiment` | Label + score de sentiment | `post_id` → FK `bluesky_posts_clean` |
| `bluesky_posts_fakenews` | Label + score de crédibilité | `post_id` → FK `bluesky_posts_clean` |

Les scripts SQL dans `init-db/` sont exécutés automatiquement au premier démarrage du conteneur `postgres` (montés sur `/docker-entrypoint-initdb.d`), dans l'ordre alphabétique de leur nom (d'où le préfixe numérique).

## Dashboard Streamlit

- **Filtres** : nombre de posts, fenêtre temporelle (jours glissants ou plage de dates), cluster, mot-clé, tonalité, seuil de crédibilité maximale
- **Vue globale** : répartition des posts par cluster, répartition des tonalités
- **Par cluster** : top mots (TF-IDF), tonalité dominante, crédibilité moyenne, % de posts classés "fake", exemples de posts, statistiques d'engagement (likes, reposts, réponses…)

## Suivi énergétique

Le hook `EnergyTrackingHooks` (`kedro-clean/src/kedro_clean/hooks.py`) démarre un `OfflineEmissionsTracker` CodeCarbon avant chaque run de pipeline Kedro et l'arrête à la fin (ou en cas d'erreur), en écrivant dans `data/08_reporting/emissions.csv`. La page Streamlit `2_Suivi_energetique.py` affiche :

- cumul d'énergie (kWh), CO₂ (g) et temps de calcul sur tous les runs
- consommation par pipeline (`data_engineering`, `vectors`, `ml`)
- répartition CPU / GPU / RAM
- évolution des émissions dans le temps

## Dépannage

- **`emissions.csv` introuvable dans Streamlit** : aucun pipeline Kedro n'a encore tourné avec le hook actif — lancez au moins un `kedro run --pipeline=...`.
- **`lsa_pipeline.pkl` introuvable** : le pipeline `vectors` n'a jamais tourné, ou le volume `kedro-clean/data` n'est pas bien monté entre Airflow et Streamlit si vous les exécutez dans des contextes différents.
- **Erreur de connexion PostgreSQL en local** : vérifiez que `POSTGRES_HOST` pointe vers `127.0.0.1` (et non `postgres`, valable uniquement à l'intérieur du réseau Docker) et que le port `5432` est bien publié par `docker-compose.yaml`.
- **Token Bluesky expiré / invalide** : supprimez `import_data/token.json`, le prochain run déclenchera une reconnexion complète via `LOGIN_BLUESKY` / `PASS_BLUESKY`.
- **Refit LSA/KMeans trop fréquent ou trop rare** : ajustez `refit_threshold` dans `parameters.yml` (exprimé en proportion de nouveaux posts par rapport au corpus au dernier fit).

## Sécurité

- Ne jamais committer `.env`, `token.json`, `conf/local/credentials.yml` ou tout fichier contenant des identifiants — vérifiez qu'ils figurent bien dans `.gitignore`.
- Utilisez un **App Password** Bluesky dédié, révocable indépendamment de votre mot de passe principal.
- Les identifiants PostgreSQL par défaut (`airflow`/`airflow`) sont adaptés à un usage local/démo uniquement ; changez-les avant tout déploiement exposé.
