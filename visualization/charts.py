import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
from typing import List, Optional

from load_data import (
    load_carburants_by_date,
    load_carburants_by_region,
    load_carburants_by_departement,
    load_carburants_top_stations,
    load_carburants_cheapest_stations,
    add_region_names,
    REGION_NAMES
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FUEL_COLORS = {
    "gazole": "#3498db",
    "sp95": "#e74c3c",
    "sp98": "#9b59b6",
    "e10": "#2ecc71",
    "e85": "#f39c12",
    "gplc": "#1abc9c"
}


def create_price_evolution_chart(df: pd.DataFrame,
                                 date_col: str = "prix_maj_date",
                                 fuel_types: List[str] = None,
                                 title: str = "Évolution des prix des carburants") -> go.Figure:

    logger.info(f"Création du graphique d'évolution: {title}")
    logger.info(f"Colonnes disponibles: {df.columns.tolist()}")

    if fuel_types is None:
        fuel_types = ["avg_gazole", "avg_sp95", "avg_sp98", "avg_e10", "avg_e85", "avg_gplc"]

    available_fuels = [f for f in fuel_types if f in df.columns]

    if not available_fuels:
        fuel_types_no_prefix = ["gazole", "sp95", "sp98", "e10", "e85", "gplc"]
        available_fuels = [f for f in fuel_types_no_prefix if f in df.columns]

    if not available_fuels:
        logger.warning(f"Aucune colonne de prix trouvée dans: {df.columns.tolist()}")
        return go.Figure()

    df = df.sort_values(date_col)

    fig = go.Figure()

    for fuel in available_fuels:
        fuel_name = fuel.replace("avg_", "").upper()
        color = FUEL_COLORS.get(fuel.replace("avg_", ""), "#666666")

        fig.add_trace(go.Scatter(
            x=df[date_col],
            y=df[fuel],
            mode='lines',
            name=fuel_name,
            line=dict(color=color, width=2),
            hovertemplate=f"<b>{fuel_name}</b><br>" +
                          "Date: %{x}<br>" +
                          "Prix: %{y:.3f} €/L<extra></extra>"
        ))

    fig.update_layout(
        title=dict(text=title, x=0.5),
        xaxis_title="Date",
        yaxis_title="Prix (€/L)",
        legend_title="Carburant",
        hovermode="x unified",
        template="plotly_white"
    )

    return fig


def create_fuel_comparison_radar(df: pd.DataFrame,
                                 region_codes: List[str] = None) -> go.Figure:

    logger.info("Création du graphique radar de comparaison")

    df = add_region_names(df.copy())

    if region_codes:
        df = df[df["region_code"].isin(region_codes)]

    fuel_cols = ["avg_gazole", "avg_sp95", "avg_sp98", "avg_e10"]
    categories = [col.replace("avg_", "").upper() for col in fuel_cols if col in df.columns]

    fig = go.Figure()

    for _, row in df.iterrows():
        values = [row[col] for col in fuel_cols if col in df.columns]
        values.append(values[0])

        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=categories + [categories[0]],
            fill='toself',
            name=row.get("region_name", row.get("region_code", "Inconnu")),
            opacity=0.7
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[1.5, 2.2])),
        showlegend=True,
        title=dict(text="Comparaison des prix par région", x=0.5)
    )

    return fig


def create_price_distribution_box(df: pd.DataFrame,
                                  fuel_type: str = "gazole") -> go.Figure:

    logger.info(f"Création du boxplot pour {fuel_type}")

    price_col = f"avg_{fuel_type}" if f"avg_{fuel_type}" in df.columns else fuel_type

    if price_col not in df.columns:
        logger.warning(f"Colonne {price_col} non trouvée")
        return go.Figure()

    df = add_region_names(df.copy())

    fig = px.box(
        df,
        x="region_name",
        y=price_col,
        color="region_name",
        title=f"Distribution des prix du {fuel_type.upper()} par région",
        labels={price_col: "Prix (€/L)", "region_name": "Région"}
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        showlegend=False,
        title_x=0.5
    )

    return fig


def create_top_bottom_stations_chart(df_top: pd.DataFrame,
                                     df_bottom: pd.DataFrame,
                                     n: int = 10) -> go.Figure:

    logger.info("Création du graphique Top/Bottom stations")

    price_col = "avg_gazole" if "avg_gazole" in df_top.columns else "gazole"

    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=(f"Top {n} stations les plus chères", f"Top {n} stations les moins chères")
    )

    if not df_top.empty:
        df_top_n = df_top.nlargest(n, price_col)
        fig.add_trace(
            go.Bar(
                x=df_top_n.get("ville", df_top_n.index),
                y=df_top_n[price_col],
                marker_color="#e74c3c",
                name="Plus chères",
                text=df_top_n[price_col].round(3),
                textposition="outside"
            ),
            row=1, col=1
        )

    if not df_bottom.empty:
        df_bottom_n = df_bottom.nsmallest(n, price_col)
        fig.add_trace(
            go.Bar(
                x=df_bottom_n.get("ville", df_bottom_n.index),
                y=df_bottom_n[price_col],
                marker_color="#2ecc71",
                name="Moins chères",
                text=df_bottom_n[price_col].round(3),
                textposition="outside"
            ),
            row=1, col=2
        )

    fig.update_layout(
        title=dict(text="Comparaison des stations extrêmes (Gazole)", x=0.5),
        showlegend=False,
        height=500
    )

    fig.update_xaxes(tickangle=-45)
    fig.update_yaxes(title_text="Prix (€/L)")

    return fig


def create_anomaly_analysis_chart(df: pd.DataFrame) -> go.Figure:

    logger.info("Création du graphique d'analyse des anomalies")

    anomaly_cols = [col for col in df.columns if "anomaly" in col.lower() or "outlier" in col.lower()]

    if not anomaly_cols:
        logger.warning("Aucune colonne d'anomalie trouvée")
        return go.Figure()

    df = add_region_names(df.copy())

    if "has_any_anomaly" in df.columns:
        anomaly_counts = df.groupby("region_name")["has_any_anomaly"].sum().reset_index()
        anomaly_counts.columns = ["Région", "Nombre d'anomalies"]

        fig = px.bar(
            anomaly_counts,
            x="Région",
            y="Nombre d'anomalies",
            color="Nombre d'anomalies",
            color_continuous_scale="Reds",
            title="Nombre de stations avec anomalies par région"
        )

        fig.update_layout(
            xaxis_tickangle=-45,
            title_x=0.5
        )

        return fig

    return go.Figure()


def create_regional_heatmap(df: pd.DataFrame) -> go.Figure:

    logger.info("Création de la heatmap régionale")

    df = add_region_names(df.copy())
    df = df[df["region_code"] != "99"]

    fuel_cols = ["avg_gazole", "avg_sp95", "avg_sp98", "avg_e10", "avg_e85", "avg_gplc"]
    available_cols = [col for col in fuel_cols if col in df.columns]

    if not available_cols:
        return go.Figure()

    matrix_data = df[["region_name"] + available_cols].set_index("region_name")
    matrix_data.columns = [col.replace("avg_", "").upper() for col in matrix_data.columns]

    fig = px.imshow(
        matrix_data.values,
        x=matrix_data.columns,
        y=matrix_data.index,
        color_continuous_scale="RdYlGn_r",
        aspect="auto",
        title="Heatmap des prix moyens par région et type de carburant",
        labels=dict(x="Carburant", y="Région", color="Prix (€/L)")
    )

    fig.update_layout(title_x=0.5)

    return fig


def create_stats_summary_table(df: pd.DataFrame) -> go.Figure:

    logger.info("Création du tableau récapitulatif")

    fuel_types = ["gazole", "sp95", "sp98", "e10", "e85", "gplc"]
    stats = []

    for fuel in fuel_types:
        col = f"avg_{fuel}" if f"avg_{fuel}" in df.columns else fuel
        if col in df.columns:
            stats.append({
                "Carburant": fuel.upper(),
                "Prix min": f"{df[col].min():.3f} €",
                "Prix max": f"{df[col].max():.3f} €",
                "Prix moyen": f"{df[col].mean():.3f} €",
                "Écart-type": f"{df[col].std():.3f} €"
            })

    if not stats:
        return go.Figure()

    stats_df = pd.DataFrame(stats)

    fig = go.Figure(data=[go.Table(
        header=dict(
            values=list(stats_df.columns),
            fill_color='#3498db',
            font=dict(color='white', size=12),
            align='center'
        ),
        cells=dict(
            values=[stats_df[col] for col in stats_df.columns],
            fill_color='#ecf0f1',
            align='center'
        )
    )])

    fig.update_layout(
        title=dict(text="Statistiques récapitulatives des prix", x=0.5),
        height=300
    )

    return fig


def save_all_charts(output_dir: str = "output"):

    import os
    os.makedirs(output_dir, exist_ok=True)

    print("Génération de tous les graphiques...")

    try:
        print("\n1. Chargement données temporelles...")
        df_date = load_carburants_by_date()
        if not df_date.empty:
            fig = create_price_evolution_chart(df_date)
            fig.write_html(os.path.join(output_dir, "evolution_prix.html"))
            print("   evolution_prix.html")

        print("\n2. Chargement données régionales...")
        df_region = load_carburants_by_region()
        if not df_region.empty:
            fig = create_fuel_comparison_radar(df_region)
            fig.write_html(os.path.join(output_dir, "radar_regions.html"))
            print("   radar_regions.html")

            fig = create_regional_heatmap(df_region)
            fig.write_html(os.path.join(output_dir, "heatmap_regions.html"))
            print("   heatmap_regions.html")

            fig = create_stats_summary_table(df_region)
            fig.write_html(os.path.join(output_dir, "stats_summary.html"))
            print("   stats_summary.html")

        print("\n3. Chargement données par département...")
        df_dept = load_carburants_by_departement()
        if not df_dept.empty:
            fig = create_price_distribution_box(df_dept)
            fig.write_html(os.path.join(output_dir, "boxplot_departements.html"))
            print("   boxplot_departements.html")

        print("\n4. Chargement top/bottom stations...")
        df_top = load_carburants_top_stations()
        df_bottom = load_carburants_cheapest_stations()
        if not df_top.empty and not df_bottom.empty:
            fig = create_top_bottom_stations_chart(df_top, df_bottom)
            fig.write_html(os.path.join(output_dir, "top_bottom_stations.html"))
            print("   top_bottom_stations.html")

        print(f"\nTous les graphiques générés dans {output_dir}/")

    except Exception as e:
        print(f"Erreur: {e}")
        print("Assurez-vous d'être authentifié: gcloud auth application-default login")


if __name__ == "__main__":
    import os
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    save_all_charts(output_dir)
