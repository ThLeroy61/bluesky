# =============================================================================
# Dockerfile — Image unique pour Airflow et Streamlit
# =============================================================================
# On part de l'image officielle Airflow en Python 3.11.
# Cette même image est utilisée par :
#   - airflow-init      (initialisation de la BDD)
#   - airflow-webserver (interface web Airflow)
#   - airflow-scheduler (planificateur de tâches)
#   - streamlit         (interface de l'application, commande différente)
#
# Le comportement de chaque service est déterminé par la clé "command"
# dans le docker-compose.yaml, pas par ce Dockerfile.
# =============================================================================

# Image de base : Airflow 2.10.4 (dernière patch stable de la 2.10) sur Python 3.11
# Le tag "-python3.11" force l'utilisation de Python 3.11 dans le conteneur,
# ce qui correspond à notre environnement local (venv Python 3.11).
FROM apache/airflow:2.10.4-python3.11

# On passe temporairement en root pour installer des dépendances système
# si nécessaire. Ici on n'en a pas besoin, mais c'est une bonne pratique
# de montrer explicitement les changements d'utilisateur.
USER airflow

# Copie du fichier de dépendances dans le répertoire courant du conteneur.
# On copie UNIQUEMENT ce fichier avant le reste du code pour profiter du
# cache Docker : si docker_requirements.txt n'a pas changé, pip ne se
# réexécute pas au prochain build même si le code a changé.
COPY docker_requirements.txt .

# Installation des dépendances.
# --no-cache-dir : ne stocke pas le cache pip dans l'image (la garde légère)
# --user est inutile ici car on est déjà sous l'utilisateur "airflow"
RUN pip install --no-cache-dir -r docker_requirements.txt
