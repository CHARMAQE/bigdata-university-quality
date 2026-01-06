import streamlit as st
import pandas as pd
import numpy as np
import os
import glob
import subprocess
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime
import time  # ADD THIS
import altair as alt  # ADD THIS

# Page Config (Must be first)
st.set_page_config(
    page_title="Data Quality Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS for "Premium" Look
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.3);
        text-align: center;
    }
    .metric-value {
        font-size: 2em;
        font-weight: bold;
        color: #4CAF50;
    }
    .metric-label {
        color: #AAA;
        font-size: 0.9em;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
    }
</style>
""", unsafe_allow_html=True)

# Configuration
DATA_DIR = os.path.join("data", "cleaned_data", "output")
LOADED_DATA_DIR = os.path.join("data", "loaded_dataset", "output")
METRICS_HISTORY = os.path.join("data", "metrics_history.csv")

# Initialize session state
if "pipeline_running" not in st.session_state:
    st.session_state.pipeline_running = False
if "last_error" not in st.session_state:
    st.session_state.last_error = None

# --- Metric Functions ---
# --- Metric Functions ---
def completeness_score(df):
    # Treat empty strings and "NULL" as missing for scoring
    df_clean = df.replace([r'^\s*$', 'NULL', 'None'], np.nan, regex=True)
    per_col = (1 - df_clean.isna().mean()) * 100
    return per_col.to_dict(), per_col.mean()

# ... (omitted unchanged functions for brevity if not touching them, but I should probably keep them to avoid confusion or use replace carefully)
# Actually, I will just redefine completeness_score and fixing the main function variable.

# I will use separate replace for completeness_score.
# And inside main(), restore raw_compl.

def uniqueness_score(df):
    # Robust duplicate check even with mixed types
    keys = [c for c in ["COD_ETU", "COD_ELP", "COD_SES"] if c in df.columns]
    if keys:
        # Normalize types for comparison if possible
        subset = df[keys].astype(str)
        dup_rate = subset.duplicated().mean()
        return 100 * (1 - dup_rate)
    return 100.0

def coherence_score(df):
    df_clean = df.copy()
    # Ensure correct types for logic checking
    if "COD_SES" in df_clean.columns:
        # Convert to numeric, errors='coerce' turns non-numeric to NaN
        df_clean["COD_SES"] = pd.to_numeric(df_clean["COD_SES"], errors='coerce')

    rule1 = df_clean["COD_TRE"].notna() & df_clean["NOT_ELP"].isna()

    if "COD_SES" in df_clean.columns and "COD_ETU" in df_clean.columns and "COD_ELP" in df_clean.columns:
        ses2 = df_clean[df_clean["COD_SES"] == 2][["COD_ETU", "COD_ELP"]]
        ses1 = df_clean[df_clean["COD_SES"] == 1][["COD_ETU", "COD_ELP"]]
        invalid_pairs = (
            ses2.merge(ses1, on=["COD_ETU", "COD_ELP"], how="left", indicator=True)
                .query('_merge == "left_only"')[["COD_ETU", "COD_ELP"]]
                .drop_duplicates()
        )
        rule2 = df_clean.merge(
            invalid_pairs, on=["COD_ETU", "COD_ELP"], how="left", indicator=True
        )["_merge"] == "both"
    else:
        rule2 = pd.Series(False, index=df_clean.index)

    incoherent = rule1 | rule2
    return 100 * (1 - incoherent.sum() / len(df) if len(df) > 0 else 1)

def validity_score(df):
    if "COD_SES" not in df.columns or "COD_TRE" not in df.columns:
        return 0.0

    # Normalize and clean for checking
    # 1/2 can be floats 1.0, 2.0 or strings "1", "2"
    valid_sessions = df["COD_SES"].astype(str).str.replace(r'\.0$', '', regex=True).isin(["1", "2"])

    # Valid results codes
    valid_cod_tre = ["V", "AJ", "RAT", "VAR", "ADM", "NV", "ACAR", "ABSN"]
    curr_codes = df["COD_TRE"].astype(str).replace(["nan", "None", "NULL"], np.nan)
    valid_results = curr_codes.isin(valid_cod_tre) | curr_codes.isna()

    return 100 * (valid_sessions & valid_results).mean()

def distribution_score(df):
    if "COD_SES" in df.columns:
        counts = df["COD_SES"].value_counts()
        p = counts / counts.sum()
        ent = -(p * np.log2(p)).sum()
        max_ent = np.log2(len(p)) if len(p) > 0 else 1
        return 100 * (ent / max_ent) if max_ent > 0 else 100.0
    return 100.0

def schema_integrity_score(df):
    if "row_status" not in df.columns:
        return 100.0
    # "Lignes Valides" = Rows that are either OK or successfully FIXED.
    # We exclude only INVALID rows which failed parsing.
    valid_rows = df["row_status"].ne("INVALID")
    return 100.0 * valid_rows.mean()

def compute_scores(df):
    per_col_compl, compl = completeness_score(df)
    uniq = uniqueness_score(df)
    coh = coherence_score(df)
    val = validity_score(df)
    dist = distribution_score(df)
    # schema = schema_integrity_score(df)  # REMOVED
    exact = val

    scores = {
        "Complétude": round(compl, 2),
        "Unicité": round(uniq, 2),
        "Cohérence": round(coh, 2),
        "Validité": round(val, 2),
        "Exactitude": round(exact, 2),
        "Distribution": round(dist, 2),
        # "Intégrité Schéma": round(schema, 2),  # REMOVED
    }

    weights = {
        "Complétude": 0.20,
        "Validité": 0.20,
        "Exactitude": 0.15,
        "Cohérence": 0.15,
        "Unicité": 0.05,
        "Distribution": 0.05,
        # "Intégrité Schéma": 0.20,  # REMOVED
    }

    total = sum(weights[k] * float(scores.get(k, 0.0)) for k in weights)
    return scores, round(total, 2), per_col_compl

# --- Data Loading ---
@st.cache_data(ttl=60)
def load_data():
    """Load cleaned CSV data."""
    try:
        if not os.path.exists(DATA_DIR):
            return pd.DataFrame()
        files = glob.glob(os.path.join(DATA_DIR, "**", "*.csv"), recursive=True)
        if not files:
            files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
        if not files:
            return pd.DataFrame()
        dfs = [pd.read_csv(f, low_memory=False) for f in files]
        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        st.error(f"Erreur chargement données nettoyées: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_raw_data():
    """Load ingested (raw) dataset produced by Spark into data/loaded_dataset/output.

    Tries pyarrow first, then fastparquet as a fallback.
    Skips unreadable part files instead of crashing the app.
    """
    if not os.path.exists(LOADED_DATA_DIR):
        return pd.DataFrame()

    # Only actual Spark parquet parts (ignore _SUCCESS, *.crc, hidden files)
    part_files = sorted(glob.glob(os.path.join(LOADED_DATA_DIR, "part-*.parquet")))
    if not part_files:
        part_files = sorted(glob.glob(os.path.join(LOADED_DATA_DIR, "**", "part-*.parquet"), recursive=True))
    if not part_files:
        return pd.DataFrame()

    def _try_read(path: str) -> pd.DataFrame:
        # 1) pyarrow
        try:
            return pd.read_parquet(path, engine="pyarrow")
        except Exception:
            pass
        # 2) fastparquet fallback
        try:
            return pd.read_parquet(path, engine="fastparquet")
        except Exception as e:
            raise e

    ok_dfs: list[pd.DataFrame] = []
    failed: list[str] = []
    first_err: str | None = None

    for f in part_files:
        try:
            df_part = _try_read(f)
            if not df_part.empty:
                ok_dfs.append(df_part)
        except Exception as e:
            failed.append(os.path.basename(f))
            if first_err is None:
                first_err = str(e)

    if not ok_dfs:
        st.error(
            "Impossible de lire les fichiers Parquet générés par Spark.\n\n"
            "Cause probable: incompatibilité de versions (pyarrow/fastparquet) avec le Parquet écrit par Spark.\n"
            f"Détail: {first_err or 'Erreur inconnue'}"
        )
        if failed:
            st.caption("Fichiers en échec (exemples): " + ", ".join(failed[:5]) + ("..." if len(failed) > 5 else ""))
        st.info(
            "Correctifs possibles:\n"
            "- Installer fastparquet: `pip install fastparquet`\n"
            "- Mettre à jour pyarrow: `pip install -U pyarrow`\n"
            "- Redémarrer Streamlit après installation."
        )
        return pd.DataFrame()

    if failed:
        st.warning(f"⚠️ {len(failed)} fichier(s) Parquet ignoré(s) car illisible(s).")
        st.caption("Exemples: " + ", ".join(failed[:5]) + ("..." if len(failed) > 5 else ""))

    return pd.concat(ok_dfs, ignore_index=True)

def append_metrics_history(scores, global_score, stage="cleaned"):
    os.makedirs(os.path.dirname(METRICS_HISTORY), exist_ok=True)
    now = datetime.utcnow().isoformat()
    cur = {"timestamp": now, "stage": stage}
    for k, v in scores.items():
        cur[k] = v
    cur["global_score"] = float(global_score)

    if os.path.exists(METRICS_HISTORY):
        hist = pd.read_csv(METRICS_HISTORY)
        if "stage" not in hist.columns:
            hist["stage"] = "cleaned"
        new_row = pd.DataFrame([cur])
        hist = pd.concat([hist, new_row], ignore_index=True, sort=False)
        hist.to_csv(METRICS_HISTORY, index=False)
    else:
        pd.DataFrame([cur]).to_csv(METRICS_HISTORY, index=False)

# --- Visualization Components ---
def card_metric(label, value, suffix=""):
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{value}{suffix}</div>
        <div class="metric-label">{label}</div>
    </div>
    """, unsafe_allow_html=True)

def plot_heatmap_missing(df):
    if df.empty:
        return

    # Exclude technical/flag columns from the missingness correlation heatmap
    exclude_cols = {"note_out_of_range", "note_incoherent"}
    cols_to_use = [c for c in df.columns if c not in exclude_cols]
    df_use = df[cols_to_use].copy()

    # Limit size to keep UI responsive
    df_small = df_use
    if len(df_small) > 20000:
        df_small = df_small.sample(n=20000, random_state=42)

    # Only consider columns with some missingness + limit number of columns
    miss_rate = df_small.isna().mean()
    cols = miss_rate[miss_rate > 0].sort_values(ascending=False).head(40).index.tolist()
    if not cols:
        st.info("Aucune valeur manquante détectée, heatmap inutile.")
        return

    miss = df_small[cols].isna().astype(int)
    corr = miss.corr()

    # CHANGED: make the whole figure smaller/denser for Streamlit page
    fig, ax = plt.subplots(figsize=(4.8, 3.0), dpi=180)
    sns.heatmap(
        corr,
        annot=True,
        fmt=".2f",
        cmap="coolwarm",
        ax=ax,
        annot_kws={"size": 6},
        cbar_kws={"shrink": 0.75}
    )

    # Tight layout reduces whitespace around the plot
    fig.tight_layout(pad=0.3)

    # CHANGED: do NOT expand to full width (reduces page space usage)
    st.pyplot(fig, use_container_width=False)

    plt.close(fig)
def run_cleaning(cmd: str | None = None):
    if cmd:
        try:
            completed = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=3600)
            out = (completed.stdout or "") + "\n" + (completed.stderr or "")
            return (completed.returncode == 0, out)
        except Exception as e:
            return (False, str(e))
    # Fallback to local python script
    script = os.path.join("src", "jobs", "run_cleaning.py")
    if not os.path.exists(script): return (False, f"Script not found: {script}")
    try:
        completed = subprocess.run(["python", script], capture_output=True, text=True, timeout=3600)
        return (completed.returncode == 0, completed.stdout + "\n" + completed.stderr)
    except Exception as e:
        return (False, str(e))

# --- Main App ---
def main():
    st.title("🛡️ Data Quality Monitor")
    st.markdown("---")

    # Load data with error handling
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Erreur chargement données: {e}")
        df = pd.DataFrame()

    try:
        df_raw = load_raw_data()
    except Exception as e:
        st.error(f"Erreur chargement données brutes: {e}")
        df_raw = pd.DataFrame()


    # --- TABS LAYOUT ---
    tab_actions, tab_overview, tab_analysis = st.tabs(["⚙️ Actions / Nettoyage", "📊 Vue d'ensemble", "🔍 Analyse Détaillée"])

    # Compute Raw Scores if available
    raw_scores = {}
    raw_global_score = 0
    if not df_raw.empty:
        raw_scores, raw_global_score, _ = compute_scores(df_raw)

    if df.empty:
        with tab_overview:
            st.warning("⚠️ Aucune donnée nettoyée disponible. Veuillez lancer le nettoyage dans l'onglet 'Actions'.")
            if not df_raw.empty:
                 st.info(f"ℹ️ Données brutes détectées : {len(df_raw)} lignes en attente de nettoyage.")
                 if st.button("💾 Initialiser l'historique avec les Données Brutes"):
                     append_metrics_history(raw_scores, raw_global_score, stage="raw")
                     st.success("Métriques brutes enregistrées !")
        scores = {}
        global_score = 0
    else:
        scores, global_score, per_col_compl = compute_scores(df)

        raw_count = len(df_raw) if not df_raw.empty else 0
        raw_compl = raw_scores.get("Complétude", 0)
        # raw_validity = (df_raw["row_status"] != "INVALID").mean() * 100 if "row_status" in df_raw.columns else 0  # REMOVED

        with tab_overview:
            st.subheader("🏁 Comparaison: Avant vs Après")

            # 1. Main KPIs with Tooltips
            c1, c2, c3 = st.columns(3)

            increase = len(df) - raw_count
            c1.metric(
                "Lignes Totales",
                f"{len(df):,}",
                delta=f"{increase} (Nettoyage)" if increase != 0 else None,
                help="Nombre total de lignes dans le jeu de données actuel."
            )

            c2.metric(
                "Score Global Qualité",
                f"{global_score}%",
                delta=f"{global_score - raw_global_score:.1f}% vs Brut" if raw_global_score > 0 else None,
                help="Moyenne pondérée de toutes les dimensions de qualité (Complétude, Unicité, etc.)."
            )

            # REPLACED KPI (was 'Intégrité Schéma')
            current_dist = scores.get("Distribution", 0)
            raw_dist = raw_scores.get("Distribution", 0) if raw_scores else 0

            c3.metric(
                "Distribution (Sessions)",
                f"{current_dist}%",
                delta=f"{current_dist - raw_dist:.1f}% vs Brut" if raw_scores else None,
                help="Mesure l'équilibre de la distribution des sessions (plus proche de 100% = plus équilibré)."
            )

            st.markdown("### 🔍 Gains par Dimension")

            # 2. Detailed Comparison Grid
            if raw_scores:
                # Prepare data for dataframe comparison
                comp_data = []
                for dimension in scores.keys():
                    after = scores.get(dimension, 0)
                    before = raw_scores.get(dimension, 0)
                    diff = after - before
                    comp_data.append({
                        "Dimension": dimension,
                        "Avant (Brut)": f"{before:.2f}%",
                        "Après (Nettoyé)": f"{after:.2f}%",
                        "Gain": f"+{diff:.2f}%" if diff >= 0 else f"{diff:.2f}%"
                    })

                # Styled dataframe
                st.dataframe(
                    pd.DataFrame(comp_data).set_index("Dimension"),
                    # width="stretch",  # CHANGED from use_container_width=True
                    use_container_width=True,
                    column_config={
                        "Gain": st.column_config.TextColumn(
                            "Gain",
                            help="Amélioration après nettoyage",
                            validate="^[-+]",
                        )
                    }
                )
            else:
                st.info("Données brutes non disponibles pour comparaison détaillée.")

            # Save Raw Metrics Button (if not already appearing in empty state)
            if not df_raw.empty:
                if st.button("💾 Enregistrer métriques BRUTES (Historique)", key="btn_save_raw_ov"):
                    append_metrics_history(raw_scores, raw_global_score, stage="raw")
                    st.toast("Métriques brutes enregistrées !", icon="✅")

            st.markdown("---")

            # 3. New Historical Chart Design
            st.markdown("### 📈 Évolution Historique")
            if os.path.exists(METRICS_HISTORY):
                hist = pd.read_csv(METRICS_HISTORY, parse_dates=["timestamp"])

                if not hist.empty:
                    # Sort by timestamp to ensure line connects properly
                    hist = hist.sort_values("timestamp")

                    # Create combined chart: Line + Points
                    base = alt.Chart(hist).encode(
                        x=alt.X('timestamp:T', title='Temps'),
                        y=alt.Y('global_score:Q', title='Score Global (%)', scale=alt.Scale(domain=[0, 100])),
                        tooltip=['timestamp', 'stage', 'global_score', 'Complétude', 'Validité']
                    )

                    # Grey line connecting all points
                    line = base.mark_line(color='grey', opacity=0.5)

                    # Colored points for stage
                    points = base.mark_circle(size=100).encode(
                        color=alt.Color(
                            'stage',
                            legend=alt.Legend(title="Étape"),
                            scale=alt.Scale(domain=['raw', 'cleaned'], range=['#e74c3c', '#2ecc71'])
                        )
                    )

                    st.altair_chart(line + points, use_container_width=True)

                    # REMOVED: Voir les données brutes de l'historique expander
                else:
                    st.info("Aucune donnée historique disponible.")
            else:
                st.info("Lancez 'Enregistrer métriques' pour commencer à suivre l'historique.")

            # 3. Historical chart (Bars: Raw vs Cleaned per metric)
            st.markdown("### 📊 Comparaison DQ (Brut vs Nettoyé) — Barres")

            if os.path.exists(METRICS_HISTORY):
                hist = pd.read_csv(METRICS_HISTORY, parse_dates=["timestamp"])

                if hist.empty:
                    st.info("Aucune donnée historique disponible.")
                else:
                    metric_cols = [
                        "Complétude", "Unicité", "Cohérence", "Validité",
                        "Exactitude", "Distribution", "global_score"
                    ]
                    metric_cols = [c for c in metric_cols if c in hist.columns]

                    # Take latest RAW and latest CLEANED (if present)
                    raw_latest = hist[hist["stage"] == "raw"].sort_values("timestamp").tail(1)
                    cleaned_latest = hist[hist["stage"] == "cleaned"].sort_values("timestamp").tail(1)

                    if raw_latest.empty or cleaned_latest.empty:
                        st.warning("Il faut au moins une ligne 'raw' ET une ligne 'cleaned' dans l'historique.")
                        st.caption("Utilise les boutons: 'Enregistrer Métriques BRUTES' puis 'Enregistrer Métriques NETTOYÉES'.")
                    else:
                        comp = pd.concat([raw_latest, cleaned_latest], ignore_index=True)

                        # Build long format: metric / value / stage
                        long_df = comp.melt(
                            id_vars=["timestamp", "stage"],
                            value_vars=metric_cols,
                            var_name="metric",
                            value_name="score"
                        )

                        # Friendly ordering (put global_score last)
                        order = [m for m in metric_cols if m != "global_score"] + (["global_score"] if "global_score" in metric_cols else [])
                        long_df["metric"] = pd.Categorical(long_df["metric"], categories=order, ordered=True)

                        chart = (
                            alt.Chart(long_df)
                            .mark_bar()
                            .encode(
                                x=alt.X("metric:N", title="Métrique", sort=order),
                                y=alt.Y("score:Q", title="Score (%)", scale=alt.Scale(domain=[0, 100])),
                                xOffset="stage:N",
                                color=alt.Color(
                                    "stage:N",
                                    title="Étape",
                                    scale=alt.Scale(domain=["raw", "cleaned"], range=["#e74c3c", "#2ecc71"])
                                ),
                                tooltip=[
                                    alt.Tooltip("stage:N", title="Étape"),
                                    alt.Tooltip("metric:N", title="Métrique"),
                                    alt.Tooltip("score:Q", title="Score", format=".2f"),
                                    alt.Tooltip("timestamp:T", title="Timestamp"),
                                ],
                            )
                            .properties(height=350)
                        )

                        # Add value label on each bar
                        labels = (
                            alt.Chart(long_df)
                            .mark_text(dy=-8, fontSize=12, color="#E6E6E6")
                            .encode(
                                x=alt.X("metric:N", sort=order),
                                y=alt.Y("score:Q"),
                                xOffset="stage:N",
                                text=alt.Text("score:Q", format=".1f")
                            )
                        )

                        st.altair_chart(chart + labels, use_container_width=True)

                        # REMOVED: Voir les données brutes de l'historique expander

            else:
                st.info("Lancez 'Enregistrer métriques' pour commencer à suivre l'historique.")

        with tab_analysis:
            st.header("🔍 Analyse Exploratoire & Qualité")

            # 1. Column Explorer
            st.subheader("1. Inspecteur de Colonnes")
            all_cols = df.columns.tolist() if not df.empty else []
            if all_cols:
                selected_col = st.selectbox("Choisir une colonne à analyser :", all_cols)

                col_data = df[selected_col]
                n_unique = col_data.nunique()
                n_missing = col_data.isna().sum()
                pct_missing = 100 * n_missing / len(df)
                top_val = col_data.mode()[0] if not col_data.empty else "N/A"

                # Metrics for the column
                c_a, c_b, c_c, c_d = st.columns(4)
                c_a.metric("Type", str(col_data.dtype))
                c_b.metric("Valeurs Uniques", n_unique)
                c_c.metric("Manquants", f"{n_missing} ({pct_missing:.1f}%)")
                c_d.metric("Top Valeur", str(top_val))

                # Distribution Chart
                st.markdown("**Distribution des Valeurs (Top 20)**")
                if n_unique < 50 or col_data.dtype == 'object':
                    top_counts = col_data.value_counts().head(20).reset_index()
                    top_counts.columns = [selected_col, "Count"]

                    chart_dist = alt.Chart(top_counts).mark_bar().encode(
                        x=alt.X(f"{selected_col}:N", sort='-y'),
                        y='Count:Q',
                        tooltip=[selected_col, 'Count']
                    ).properties(height=300)
                    st.altair_chart(chart_dist, use_container_width=True)
                else:
                    st.info("Distribution non affichée pour les colonnes numériques continues (voir statistiques descriptives ci-dessous).")
                    st.write(col_data.describe())

            # 2. Drill Down into Errors
            st.markdown("---")
            st.subheader("2. Focus sur les Erreurs")

            error_type = st.radio("Type d'erreur à inspecter :", ["Lignes INVALIDES (Parsing)", "Valeurs Manquantes", "Doublons (Unicité)"], horizontal=True)

            if error_type == "Lignes INVALIDES (Parsing)":
                if "row_status" in df.columns:
                    invalid_df = df[df["row_status"] == "INVALID"]
                    st.dataframe(invalid_df, use_container_width=True)
                    if invalid_df.empty:
                        st.success("Aucune ligne invalide de parsing trouvée !")
                else:
                    st.warning("Colonne 'row_status' non trouvée.")

            elif error_type == "Valeurs Manquantes":
                st.write("Lignes contenant des valeurs manquantes :")
                na_df = df[df.isna().any(axis=1)]
                st.dataframe(na_df.head(100), use_container_width=True)
                if na_df.empty:
                     st.success("Aucune valeur manquante !")
                else:
                     st.caption(f"Affichage des 100 premières lignes sur {len(na_df)} incomplètes.")

            elif error_type == "Doublons (Unicité)":
                # Check based on keys
                keys = [c for c in ["COD_ETU", "COD_ELP", "COD_SES"] if c in df.columns]
                if keys:
                     dupes = df[df.duplicated(subset=keys, keep=False)].sort_values(keys)
                     st.dataframe(dupes, use_container_width=True)
                     if dupes.empty:
                         st.success("Aucun doublon trouvé sur la clé primaire composées (ETU + ELP + SES).")
                else:
                     st.info("Clés primaires (ETU, ELP, SES) non complètes pour l'analyse des doublons.")

            # 3. Global Correlations
            st.markdown("---")
            st.subheader("3. Corrélations (Heatmap Manquants)")

            # CHANGED: always show heatmap (no expander)
            plot_heatmap_missing(df)

    with tab_actions:
        st.header("⚙️ Contrôle du Pipeline")
        st.markdown("Suivez les étapes dans l'ordre pour garantir la cohérence des données.")

        # --- PHASE 1: INGESTION ---
        st.subheader("1️⃣ Ingestion (Chargement)")
        st.info("Cette étape charge le fichier brut `dataset_metier.txt`, corrige les délimiteurs et prépare les données dans `data/loaded_dataset`.")

        col_ing1, col_ing2 = st.columns([1, 3])
        with col_ing1:
             if st.button(
                "📥 Lancer Ingestion",
                type="primary",
                key="btn_ingest",
                disabled=st.session_state.pipeline_running,
            ):
                st.session_state.pipeline_running = True
                try:
                    cmd = 'docker exec spark-master_1 bash -c "PYTHONPATH=/opt/project/src /opt/spark/bin/spark-submit --master local[*] /opt/project/src/jobs/load_data.py"'
                    with st.spinner("Exécution de l'ingestion Spark..."):
                        ok, out = run_cleaning(cmd)
                finally:
                    st.session_state.pipeline_running = False

                if ok:
                    st.success("✅ Ingestion terminée !")

                    # IMPORTANT: clear cached loaders so step 2 instantly sees the new parquet files
                    load_raw_data.clear()
                    load_data.clear()

                    # Also clear session state values if you stored previous df/df_raw anywhere
                    st.session_state.last_error = None

                    st.rerun()
                else:
                    st.error("❌ Erreur lors de l'ingestion")
                    st.code(out)

        # --- PHASE 2: SAVE RAW METRICS ---
        st.markdown("---")
        st.subheader("2️⃣ Sauvegarde Métriques (Brutes)")
        st.info("Calcule et enregistre la qualité des données AVANT nettoyage.")

        if not df_raw.empty:
            # Calculate raw scores on the fly for saving
            if not raw_scores: # ensure calculated if not done in main
                 raw_scores, raw_global_score, _ = compute_scores(df_raw)

            c_raw1, c_raw2 = st.columns([1, 3])
            with c_raw1:
                if st.button("💾 Enregistrer Métriques BRUTES", key="btn_save_raw"):
                    append_metrics_history(raw_scores, raw_global_score, stage="raw")
                    st.success("✅ Métriques brutes enregistrées dans l'historique !")
            with c_raw2:
                 st.caption(f"Score Brut Actuel: **{raw_global_score}%** (Basé sur {len(df_raw)} lignes)")
        else:
            st.warning("⚠️ Veuillez d'abord lancer l'ingestion (Étape 1).")

        # --- PHASE 3: CLEANING ---
        st.markdown("---")
        st.subheader("3️⃣ Nettoyage (Cleaning)")
        st.info("Applique les règles de qualité et génère les fichiers finaux dans `data/cleaned_data`.")

        col_clean1, col_clean2 = st.columns([1, 3])
        with col_clean1:
            if st.button("🚀 Lancer Nettoyage Complet", type="primary", key="btn_clean"):
                cmd = 'docker exec spark-master_1 bash -c "PYTHONPATH=/opt/project/src /opt/spark/bin/spark-submit --master local[*] /opt/project/src/jobs/run_cleaning.py"'
                with st.spinner("Exécution du nettoyage Spark..."):
                    ok, out = run_cleaning(cmd)
                if ok:
                    st.success("✅ Nettoyage terminé !")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Erreur")
                    st.code(out)

        # --- PHASE 4: SAVE CLEANED METRICS ---
        st.markdown("---")
        st.subheader("4️⃣ Sauvegarde Métriques (Nettoyées)")
        st.info("Calcule et enregistre la qualité des données APRÈS nettoyage.")

        if not df.empty:
            c_clean1, c_clean2 = st.columns([1, 3])
            with c_clean1:
                if st.button("💾 Enregistrer Métriques NETTOYÉES", key="btn_save_clean"):
                    # Recalculate if needed, though 'scores' should be available from main
                    if not scores:
                        scores, global_score, _ = compute_scores(df)
                    append_metrics_history(scores, global_score, stage="cleaned")
                    st.success("✅ Métriques nettoyées enregistrées dans l'historique !")
            with c_clean2:
                 st.caption(f"Score Nettoyé Actuel: **{global_score}%** (Basé sur {len(df)} lignes)")
        else:
            st.warning("⚠️ Veuillez d'abord lancer le nettoyage (Étape 3).")

if __name__ == "__main__":
    main()