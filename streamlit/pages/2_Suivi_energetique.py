import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(encoding="utf-8-sig")

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Suivi énergétique", layout="wide")
st.title("Suivi énergétique — pipelines Kedro")

EMISSIONS_PATH = Path(
    os.getenv("EMISSIONS_DIR", "/opt/kedro-clean/data/08_reporting")
) / "emissions.csv"


@st.cache_data(ttl=60)
def load_emissions(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df


if not EMISSIONS_PATH.exists():
    st.info(
        f"Aucun rapport trouvé à `{EMISSIONS_PATH}`.\n\n"
        "Lance au moins un pipeline (`kedro run --pipeline=...`) avec le hook "
        "codecarbon activé pour générer `emissions.csv`."
    )
    st.stop()

df = load_emissions(str(EMISSIONS_PATH))

if df.empty:
    st.warning("Le fichier emissions.csv est vide.")
    st.stop()

# codecarbon : energy_consumed en kWh, emissions en kg CO2eq, duration en s.
total_kwh = df["energy_consumed"].sum()
total_co2 = df["emissions"].sum()
total_dur = df["duration"].sum()
n_runs    = len(df)

st.subheader("Cumul sur tous les runs")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Énergie totale", f"{total_kwh:.4f} kWh")
c2.metric("CO₂ émis", f"{total_co2 * 1000:.1f} g")
c3.metric("Temps de calcul", f"{total_dur / 60:.1f} min")
c4.metric("Nombre de runs", n_runs)

st.divider()

# ── Énergie par pipeline ──────────────────────────────────────────────────────
st.subheader("Consommation par pipeline")
by_pipeline = (
    df.groupby("project_name")
    .agg(
        energie_kwh=("energy_consumed", "sum"),
        co2_g=("emissions", lambda s: s.sum() * 1000),
        duree_min=("duration", lambda s: s.sum() / 60),
        runs=("energy_consumed", "size"),
    )
    .sort_values("energie_kwh", ascending=False)
)

col_g, col_t = st.columns([1, 1])
with col_g:
    st.caption("Énergie consommée (kWh)")
    st.bar_chart(by_pipeline["energie_kwh"])
with col_t:
    st.dataframe(
        by_pipeline.round(
            {"energie_kwh": 4, "co2_g": 2, "duree_min": 2}
        ),
        use_container_width=True,
    )

# ── Répartition CPU / GPU / RAM ───────────────────────────────────────────────
energy_cols = [c for c in ["cpu_energy", "gpu_energy", "ram_energy"] if c in df.columns]
if energy_cols:
    st.subheader("Répartition CPU / GPU / RAM")
    breakdown = df[energy_cols].sum()
    breakdown.index = [c.replace("_energy", "").upper() for c in breakdown.index]
    st.bar_chart(breakdown)
    st.caption("Énergie cumulée par composant (kWh). GPU à 0 si exécution CPU.")

st.divider()

# ── Évolution dans le temps ───────────────────────────────────────────────────
if "timestamp" in df.columns and df["timestamp"].notna().any():
    st.subheader("Évolution des émissions par run")
    ts = (
        df.dropna(subset=["timestamp"])
        .set_index("timestamp")
        .sort_index()
    )
    ts_co2 = ts["emissions"] * 1000
    ts_co2.name = "CO₂ (g)"
    st.line_chart(ts_co2)

# ── Détail brut ───────────────────────────────────────────────────────────────
with st.expander("Détail des runs (données brutes codecarbon)"):
    show_cols = [
        c for c in [
            "timestamp", "project_name", "duration", "emissions",
            "energy_consumed", "cpu_energy", "gpu_energy", "ram_energy",
            "cpu_model", "gpu_model", "country_name",
        ] if c in df.columns
    ]
    st.dataframe(
        df[show_cols].sort_values("timestamp", ascending=False)
        if "timestamp" in show_cols else df[show_cols],
        use_container_width=True,
        hide_index=True,
    )
