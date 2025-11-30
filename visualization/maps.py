import pandas as pd
import folium
from folium import plugins
import plotly.express as px
import plotly.graph_objects as go
import json
import requests
import logging
from typing import Optional

from load_data import (
    load_carburants_by_region,
    load_carburants_by_departement,
    load_carburants_detailed,
    add_region_names,
    REGION_NAMES
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REGIONS_GEOJSON_URL = "https://france-geojson.gregoiredavid.fr/repo/regions.geojson"
DEPARTEMENTS_GEOJSON_URL = "https://france-geojson.gregoiredavid.fr/repo/departements.geojson"


def load_geojson(url: str) -> dict:
    response = requests.get(url)
    return response.json()


def create_region_choropleth(df: pd.DataFrame,
                             value_column: str = "avg_gazole",
                             title: str = "Prix moyen du Gazole par région") -> go.Figure:

    logger.info(f"Création de la carte choroplèthe: {title}")

    geojson = load_geojson(REGIONS_GEOJSON_URL)

    df = add_region_names(df.copy())

    region_mapping = {
        "11": "Île-de-France",
        "24": "Centre-Val de Loire",
        "27": "Bourgogne-Franche-Comté",
        "28": "Normandie",
        "32": "Hauts-de-France",
        "44": "Grand Est",
        "52": "Pays de la Loire",
        "53": "Bretagne",
        "75": "Nouvelle-Aquitaine",
        "76": "Occitanie",
        "84": "Auvergne-Rhône-Alpes",
        "93": "Provence-Alpes-Côte d'Azur",
        "94": "Corse"
    }

    df["region_name"] = df["region_code"].astype(str).map(region_mapping)

    fig = px.choropleth(
        df,
        geojson=geojson,
        locations="region_name",
        featureidkey="properties.nom",
        color=value_column,
        color_continuous_scale="RdYlGn_r",
        title=title,
        labels={value_column: "Prix (€/L)"}
    )

    fig.update_geos(
        fitbounds="locations",
        visible=False
    )

    fig.update_layout(
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
        title_x=0.5
    )

    return fig


def create_folium_map(df: pd.DataFrame,
                      lat_col: str = "latitude",
                      lon_col: str = "longitude",
                      popup_cols: list = None,
                      color_by: str = None) -> folium.Map:

    logger.info("Création de la carte Folium interactive...")

    france_center = [46.603354, 1.888334]

    m = folium.Map(
        location=france_center,
        zoom_start=6,
        tiles="cartodbpositron"
    )

    if popup_cols is None:
        popup_cols = ["ville", "adresse", "gazole", "sp95", "sp98"]

    df_valid = df.dropna(subset=[lat_col, lon_col])

    if len(df_valid) > 5000:
        logger.warning(f"Dataset trop grand ({len(df_valid)} points), échantillonnage à 5000")
        df_valid = df_valid.sample(n=5000, random_state=42)

    marker_cluster = plugins.MarkerCluster()

    for _, row in df_valid.iterrows():
        popup_html = "<b>Station</b><br>"
        for col in popup_cols:
            if col in row.index and pd.notna(row[col]):
                value = row[col]
                if isinstance(value, float):
                    value = f"{value:.3f} €/L"
                popup_html += f"<b>{col}:</b> {value}<br>"

        if color_by and color_by in row.index:
            if row.get("has_any_anomaly", False):
                color = "red"
            elif row.get("competitive_gazole", 0) > 0:
                color = "green"
            else:
                color = "blue"
        else:
            color = "blue"

        folium.Marker(
            location=[row[lat_col], row[lon_col]],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color=color, icon="info-sign")
        ).add_to(marker_cluster)

    marker_cluster.add_to(m)

    legend_html = """
    <div style="position: fixed; bottom: 50px; left: 50px; z-index: 1000;
                background-color: white; padding: 10px; border-radius: 5px;
                border: 2px solid grey;">
        <p><b>Légende</b></p>
        <p><i class="fa fa-map-marker" style="color:green"></i> Station compétitive</p>
        <p><i class="fa fa-map-marker" style="color:blue"></i> Station normale</p>
        <p><i class="fa fa-map-marker" style="color:red"></i> Anomalie détectée</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    logger.info(f"Carte créée avec {len(df_valid)} stations")
    return m


def create_heatmap(df: pd.DataFrame,
                   lat_col: str = "latitude",
                   lon_col: str = "longitude",
                   weight_col: str = "gazole") -> folium.Map:

    logger.info("Création de la heatmap des prix...")

    france_center = [46.603354, 1.888334]
    m = folium.Map(location=france_center, zoom_start=6, tiles="cartodbpositron")

    df_valid = df.dropna(subset=[lat_col, lon_col, weight_col])

    heat_data = df_valid[[lat_col, lon_col, weight_col]].values.tolist()

    plugins.HeatMap(
        heat_data,
        min_opacity=0.3,
        radius=15,
        blur=10,
        gradient={0.4: 'green', 0.65: 'yellow', 1: 'red'}
    ).add_to(m)

    logger.info(f"Heatmap créée avec {len(df_valid)} points")
    return m


def create_region_comparison_bar(df: pd.DataFrame,
                                 fuel_types: list = None) -> go.Figure:

    if fuel_types is None:
        fuel_types = ["avg_gazole", "avg_sp95", "avg_sp98", "avg_e10"]

    df = add_region_names(df.copy())
    df = df[df["region_code"] != "99"]

    df_melted = df.melt(
        id_vars=["region_name"],
        value_vars=[f for f in fuel_types if f in df.columns],
        var_name="Carburant",
        value_name="Prix (€/L)"
    )

    df_melted["Carburant"] = df_melted["Carburant"].str.replace("avg_", "").str.upper()

    fig = px.bar(
        df_melted,
        x="region_name",
        y="Prix (€/L)",
        color="Carburant",
        barmode="group",
        title="Comparaison des prix par région et type de carburant",
        labels={"region_name": "Région"}
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        legend_title="Type de carburant",
        title_x=0.5
    )

    return fig


def save_map(map_obj: folium.Map, filename: str):
    map_obj.save(filename)
    logger.info(f"Carte sauvegardée: {filename}")


def save_plotly_figure(fig: go.Figure, filename: str, format: str = "html"):
    if format == "html":
        fig.write_html(filename)
    elif format == "png":
        fig.write_image(filename)
    logger.info(f"Figure sauvegardée: {filename}")


if __name__ == "__main__":
    import os

    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    print("Génération des cartes de visualisation...")

    try:
        print("\n1. Chargement données régionales...")
        df_region = load_carburants_by_region()

        if not df_region.empty:
            print("   Création carte choroplèthe...")
            fig_choropleth = create_region_choropleth(df_region)
            save_plotly_figure(fig_choropleth, os.path.join(output_dir, "carte_regions_gazole.html"))

            print("   Création graphique comparatif...")
            fig_comparison = create_region_comparison_bar(df_region)
            save_plotly_figure(fig_comparison, os.path.join(output_dir, "comparaison_regions.html"))

        print("\n2. Chargement données détaillées...")
        try:
            df_detailed = load_carburants_detailed()
            if not df_detailed.empty and "latitude" in df_detailed.columns:
                print("   Création carte interactive stations...")
                map_stations = create_folium_map(df_detailed)
                save_map(map_stations, os.path.join(output_dir, "carte_stations.html"))

                print("   Création heatmap des prix...")
                map_heatmap = create_heatmap(df_detailed)
                save_map(map_heatmap, os.path.join(output_dir, "heatmap_prix.html"))
        except Exception as e:
            print(f"   Données détaillées non disponibles: {e}")

        print(f"\nCartes générées dans {output_dir}/")

    except Exception as e:
        print(f"Erreur: {e}")
        print("Assurez-vous d'être authentifié: gcloud auth application-default login")
