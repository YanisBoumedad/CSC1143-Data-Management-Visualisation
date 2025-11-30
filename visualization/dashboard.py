import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from load_data import (
    load_carburants_by_region,
    load_carburants_by_departement,
    load_carburants_by_date,
    load_carburants_top_stations,
    load_carburants_cheapest_stations,
    add_region_names,
    REGION_NAMES
)

from charts import (
    create_price_evolution_chart,
    create_fuel_comparison_radar,
    create_regional_heatmap,
    create_stats_summary_table,
    FUEL_COLORS
)

st.set_page_config(
    page_title="Dashboard Carburants France",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #2c3e50;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 10px;
        padding: 1rem;
        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
    }
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600)
def load_all_data():
    data = {}
    try:
        data["region"] = load_carburants_by_region()
        data["departement"] = load_carburants_by_departement()
        data["date"] = load_carburants_by_date()
        data["top"] = load_carburants_top_stations()
        data["bottom"] = load_carburants_cheapest_stations()
    except Exception as e:
        st.error(f"Erreur de chargement: {e}")
    return data


def main():
    st.markdown('<h1 class="main-header">Dashboard Prix Carburants France</h1>', unsafe_allow_html=True)

    with st.sidebar:
        st.image("https://upload.wikimedia.org/wikipedia/fr/0/0f/Logo_france.svg", width=100)
        st.title("Navigation")

        page = st.radio(
            "Choisir une vue:",
            ["Vue d'ensemble", "Évolution temporelle", "Carte régionale",
             "Comparaisons", "Top/Bottom stations", "Statistiques"]
        )

        st.markdown("---")
        st.info("Les données sont issues du traitement Spark des prix carburants en France.")

    with st.spinner("Chargement des données..."):
        data = load_all_data()

    if not data or all(df is None or df.empty for df in data.values() if isinstance(df, pd.DataFrame)):
        st.warning("Aucune donnée disponible. Vérifiez que les jobs Spark ont été exécutés.")
        st.code("gcloud auth application-default login", language="bash")
        return

    if page == "Vue d'ensemble":
        show_overview(data)
    elif page == "Évolution temporelle":
        show_temporal_evolution(data)
    elif page == "Carte régionale":
        show_regional_map(data)
    elif page == "Comparaisons":
        show_comparisons(data)
    elif page == "Top/Bottom stations":
        show_top_bottom(data)
    elif page == "Statistiques":
        show_statistics(data)


def show_overview(data):
    st.header("Vue d'ensemble des prix carburants")

    df_region = data.get("region", pd.DataFrame())
    df_date = data.get("date", pd.DataFrame())

    if df_region.empty:
        st.warning("Données régionales non disponibles")
        return

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if "avg_gazole" in df_region.columns:
            avg_price = df_region["avg_gazole"].mean()
            st.metric("Gazole moyen", f"{avg_price:.3f} €/L")

    with col2:
        if "avg_sp95" in df_region.columns:
            avg_price = df_region["avg_sp95"].mean()
            st.metric("SP95 moyen", f"{avg_price:.3f} €/L")

    with col3:
        if "avg_sp98" in df_region.columns:
            avg_price = df_region["avg_sp98"].mean()
            st.metric("SP98 moyen", f"{avg_price:.3f} €/L")

    with col4:
        if "avg_e10" in df_region.columns:
            avg_price = df_region["avg_e10"].mean()
            st.metric("E10 moyen", f"{avg_price:.3f} €/L")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Prix par région")
        fig = create_regional_heatmap(df_region)
        st.plotly_chart(fig, use_container_width=True, key="overview_heatmap")

    with col2:
        st.subheader("Évolution récente")
        if not df_date.empty:
            fig = create_price_evolution_chart(df_date)
            st.plotly_chart(fig, use_container_width=True, key="overview_evolution")
        else:
            st.info("Données temporelles non disponibles")


def show_temporal_evolution(data):
    st.header("Évolution temporelle des prix")

    df_date = data.get("date", pd.DataFrame())

    if df_date.empty:
        st.warning("Données temporelles non disponibles")
        return

    fuel_options = ["Gazole", "SP95", "SP98", "E10", "E85", "GPL"]
    selected_fuels = st.multiselect(
        "Sélectionner les carburants:",
        fuel_options,
        default=["Gazole", "SP95", "E10"]
    )

    fuel_cols = [f"avg_{f.lower()}" for f in selected_fuels]

    fig = create_price_evolution_chart(df_date, fuel_types=fuel_cols)
    st.plotly_chart(fig, use_container_width=True, key="temporal_evolution")

    st.subheader("Statistiques par période")

    col1, col2 = st.columns(2)

    with col1:
        if "prix_maj_date" in df_date.columns:
            df_date["prix_maj_date"] = pd.to_datetime(df_date["prix_maj_date"])
            date_min = df_date["prix_maj_date"].min()
            date_max = df_date["prix_maj_date"].max()
            st.info(f"Période: {date_min.strftime('%d/%m/%Y')} - {date_max.strftime('%d/%m/%Y')}")

    with col2:
        if "avg_gazole" in df_date.columns:
            variation = df_date["avg_gazole"].iloc[-1] - df_date["avg_gazole"].iloc[0]
            st.metric("Variation Gazole", f"{variation:+.3f} €/L")


def show_regional_map(data):
    st.header("Carte des prix par région")

    df_region = data.get("region", pd.DataFrame())

    if df_region.empty:
        st.warning("Données régionales non disponibles")
        return

    fuel = st.selectbox(
        "Sélectionner le carburant:",
        ["Gazole", "SP95", "SP98", "E10"],
        index=0
    )

    fuel_col = f"avg_{fuel.lower()}"

    df_region = add_region_names(df_region.copy())

    df_plot = df_region[df_region["region_code"] != "99"].sort_values(fuel_col, ascending=True)

    fig = px.bar(
        df_plot,
        x=fuel_col,
        y="region_name",
        orientation="h",
        color=fuel_col,
        color_continuous_scale="RdYlGn_r",
        title=f"Prix moyen du {fuel} par région",
        labels={fuel_col: "Prix (€/L)", "region_name": "Région"}
    )

    fig.update_layout(height=600)
    st.plotly_chart(fig, use_container_width=True, key="regional_bar")

    st.subheader("Détails par région")
    df_display = df_region[["region_name", "avg_gazole", "avg_sp95", "avg_sp98", "avg_e10", "station_count"]].copy()
    df_display.columns = ["Région", "Gazole", "SP95", "SP98", "E10", "Nb Stations"]

    for col in ["Gazole", "SP95", "SP98", "E10"]:
        if col in df_display.columns:
            df_display[col] = df_display[col].apply(lambda x: f"{x:.3f} €" if pd.notna(x) else "-")

    st.dataframe(df_display, use_container_width=True, hide_index=True)


def show_comparisons(data):
    st.header("Comparaisons entre carburants et régions")

    df_region = data.get("region", pd.DataFrame())

    if df_region.empty:
        st.warning("Données régionales non disponibles")
        return

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Radar par région")
        df_region = add_region_names(df_region.copy())
        regions = df_region[df_region["region_code"] != "99"]["region_name"].unique()

        selected_regions = st.multiselect(
            "Sélectionner les régions à comparer:",
            regions,
            default=list(regions)[:5]
        )

        if selected_regions:
            df_filtered = df_region[df_region["region_name"].isin(selected_regions)]
            fig = create_fuel_comparison_radar(df_filtered)
            st.plotly_chart(fig, use_container_width=True, key="comparison_radar")

    with col2:
        st.subheader("Heatmap prix")
        fig = create_regional_heatmap(df_region)
        st.plotly_chart(fig, use_container_width=True, key="comparison_heatmap")

    st.subheader("Écart avec la moyenne nationale")

    fuel = st.selectbox("Carburant:", ["Gazole", "SP95", "SP98", "E10"], key="ecart_fuel")
    fuel_col = f"avg_{fuel.lower()}"

    if fuel_col in df_region.columns:
        mean_price = df_region[fuel_col].mean()
        df_region["ecart"] = df_region[fuel_col] - mean_price
        df_region["ecart_pct"] = (df_region["ecart"] / mean_price) * 100

        df_sorted = df_region.sort_values("ecart", ascending=True)

        fig = px.bar(
            df_sorted[df_sorted["region_code"] != "99"],
            x="region_name",
            y="ecart",
            color="ecart",
            color_continuous_scale="RdYlGn_r",
            title=f"Écart au prix moyen national ({fuel}: {mean_price:.3f} €/L)",
            labels={"region_name": "Région", "ecart": "Écart (€/L)"}
        )

        fig.add_hline(y=0, line_dash="dash", line_color="black")
        fig.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True, key="comparison_ecart")


def show_top_bottom(data):
    st.header("Stations les plus et moins chères")

    df_top = data.get("top", pd.DataFrame())
    df_bottom = data.get("bottom", pd.DataFrame())

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Stations les plus chères")
        if not df_top.empty:
            display_cols = ["ville", "adresse", "avg_gazole", "region_code"]
            available_cols = [c for c in display_cols if c in df_top.columns]

            if "avg_gazole" in df_top.columns:
                df_display = df_top.nlargest(15, "avg_gazole")[available_cols]
                st.dataframe(df_display, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_top.head(15), use_container_width=True, hide_index=True)
        else:
            st.info("Données non disponibles")

    with col2:
        st.subheader("Stations les moins chères")
        if not df_bottom.empty:
            display_cols = ["ville", "adresse", "avg_gazole", "region_code"]
            available_cols = [c for c in display_cols if c in df_bottom.columns]

            if "avg_gazole" in df_bottom.columns:
                df_display = df_bottom.nsmallest(15, "avg_gazole")[available_cols]
                st.dataframe(df_display, use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_bottom.head(15), use_container_width=True, hide_index=True)
        else:
            st.info("Données non disponibles")


def show_statistics(data):
    st.header("Statistiques détaillées")

    df_region = data.get("region", pd.DataFrame())

    if df_region.empty:
        st.warning("Données non disponibles")
        return

    st.subheader("Récapitulatif par carburant")
    fig = create_stats_summary_table(df_region)
    st.plotly_chart(fig, use_container_width=True, key="stats_table")

    st.subheader("Distribution des prix")

    fuel = st.selectbox("Carburant:", ["Gazole", "SP95", "SP98", "E10"], key="stats_fuel")
    fuel_col = f"avg_{fuel.lower()}"

    if fuel_col in df_region.columns:
        fig = px.histogram(
            df_region,
            x=fuel_col,
            nbins=20,
            title=f"Distribution des prix moyens du {fuel} par région",
            labels={fuel_col: "Prix (€/L)"}
        )
        fig.add_vline(x=df_region[fuel_col].mean(), line_dash="dash",
                      annotation_text=f"Moyenne: {df_region[fuel_col].mean():.3f}€")
        st.plotly_chart(fig, use_container_width=True, key="stats_histogram")

    st.markdown("---")
    st.subheader("Export des données")

    if st.button("Télécharger les données (CSV)"):
        csv = df_region.to_csv(index=False)
        st.download_button(
            label="Cliquer pour télécharger",
            data=csv,
            file_name="carburants_regions.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()
