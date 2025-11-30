import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import requests
from io import BytesIO
from google.cloud import storage
from datetime import datetime, timedelta
import folium
from streamlit_folium import st_folium
import branca.colormap as cm

GCP_PROJECT_ID = "regal-sun-478114-q5"
GCS_BUCKET_NAME = "csc1142-projet"
GCS_PROCESSED_PATH = "processed"

st.set_page_config(
    page_title="Fuel Prices - France",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    * {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    .stApp {
        background: #F8FAFC;
    }

    .main {
        background: #F8FAFC;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1E293B 0%, #0F172A 100%);
    }

    [data-testid="stSidebar"] * {
        color: #E2E8F0 !important;
    }

    [data-testid="stSidebar"] .stMarkdown h3 {
        color: #F1F5F9 !important;
        font-weight: 600;
        font-size: 14px;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-top: 24px;
    }

    [data-testid="stSidebar"] .stSelectbox label,
    [data-testid="stSidebar"] .stSlider label {
        color: #CBD5E1 !important;
        font-weight: 500;
        font-size: 13px;
    }

    .main .stMarkdown h1,
    .main .stMarkdown h2,
    .main .stMarkdown h3,
    .main .stMarkdown p {
        color: #0F172A !important;
    }

    .main [data-testid="stMarkdownContainer"] {
        color: #0F172A !important;
    }

    .main .stExpander {
        color: #0F172A !important;
    }

    .main .stExpander summary {
        color: #0F172A !important;
        font-weight: 600;
    }

    .main .stExpander [data-testid="stMarkdownContainer"] h4 {
        color: #0F172A !important;
        font-weight: 600;
    }

    .main .stExpander [data-testid="stMarkdownContainer"] p,
    .main .stExpander [data-testid="stMarkdownContainer"] li {
        color: #64748B !important;
    }

    .main .stExpander [data-testid="stMarkdownContainer"] strong {
        color: #0F172A !important;
    }

    .metric-card {
        background: white;
        padding: 24px;
        border-radius: 12px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        border: 1px solid #E2E8F0;
        transition: all 0.2s ease;
    }

    .metric-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        transform: translateY(-2px);
    }

    .metric-card h3 {
        font-size: 13px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin: 0 0 8px 0;
        color: #64748B;
    }

    .metric-card h2 {
        font-size: 32px;
        font-weight: 700;
        margin: 0;
        color: #0F172A;
    }

    h1 {
        color: #0F172A;
        font-weight: 700;
        font-size: 36px;
        margin-bottom: 8px;
        letter-spacing: -0.02em;
    }

    .subtitle {
        color: #64748B;
        font-size: 16px;
        font-weight: 400;
        margin-bottom: 32px;
    }

    .stButton button {
        background: linear-gradient(135deg, #3B82F6 0%, #2563EB 100%);
        color: white;
        border: none;
        padding: 12px 24px;
        border-radius: 8px;
        font-weight: 600;
        font-size: 14px;
        transition: all 0.2s ease;
        width: 100%;
        margin-top: 16px;
    }

    .stButton button:hover {
        background: linear-gradient(135deg, #2563EB 0%, #1D4ED8 100%);
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3);
    }

    .stExpander {
        background: white;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        margin-top: 16px;
    }

    div[data-testid="stExpander"] details {
        background: white;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=3600)
def load_geojson():
    try:
        url = "https://france-geojson.gregoiredavid.fr/repo/departements.geojson"
        response = requests.get(url, timeout=15)
        return response.json()
    except Exception as e:
        st.error(f"Error loading departments GeoJSON: {e}")
        return None

@st.cache_data(ttl=3600)
def load_regions_geojson():
    try:
        url = "https://france-geojson.gregoiredavid.fr/repo/regions.geojson"
        response = requests.get(url, timeout=15)
        return response.json()
    except Exception as e:
        st.error(f"Error loading regions GeoJSON: {e}")
        return None

def get_department_to_region_mapping():
    mapping = {
        # Auvergne-Rhône-Alpes (84)
        '01': '84', '03': '84', '07': '84', '15': '84', '26': '84', '38': '84', '42': '84', '43': '84', '63': '84', '69': '84', '73': '84', '74': '84',
        # Bourgogne-Franche-Comté (27)
        '21': '27', '25': '27', '39': '27', '58': '27', '70': '27', '71': '27', '89': '27', '90': '27',
        # Bretagne (53)
        '22': '53', '29': '53', '35': '53', '56': '53',
        # Centre-Val de Loire (24)
        '18': '24', '28': '24', '36': '24', '37': '24', '41': '24', '45': '24',
        # Corse (94)
        '2A': '94', '2B': '94',
        # Grand Est (44)
        '08': '44', '10': '44', '51': '44', '52': '44', '54': '44', '55': '44', '57': '44', '67': '44', '68': '44', '88': '44',
        # Hauts-de-France (32)
        '02': '32', '59': '32', '60': '32', '62': '32', '80': '32',
        # Île-de-France (11)
        '75': '11', '77': '11', '78': '11', '91': '11', '92': '11', '93': '11', '94': '11', '95': '11',
        # Normandie (28)
        '14': '28', '27': '28', '50': '28', '61': '28', '76': '28',
        # Nouvelle-Aquitaine (75)
        '16': '75', '17': '75', '19': '75', '23': '75', '24': '75', '33': '75', '40': '75', '47': '75', '64': '75', '79': '75', '86': '75', '87': '75',
        # Occitanie (76)
        '09': '76', '11': '76', '12': '76', '30': '76', '31': '76', '32': '76', '34': '76', '46': '76', '48': '76', '65': '76', '66': '76', '81': '76', '82': '76',
        # Pays de la Loire (52)
        '44': '52', '49': '52', '53': '52', '72': '52', '85': '52',
        # Provence-Alpes-Côte d'Azur (93)
        '04': '93', '05': '93', '06': '93', '13': '93', '83': '93', '84': '93'
    }
    return mapping

@st.cache_data(ttl=3600)
def load_departement_data():
    try:
        client = storage.Client(project=GCP_PROJECT_ID)
        bucket = client.bucket(GCS_BUCKET_NAME)

        blobs = bucket.list_blobs(prefix=f"{GCS_PROCESSED_PATH}/carburants/aggregations/by_departement_date/")

        dfs = []
        for blob in blobs:
            if blob.name.endswith('.parquet'):
                content = blob.download_as_bytes()
                df = pd.read_parquet(BytesIO(content))
                dfs.append(df)

        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            if 'date' in df.columns:
                df = df[df['date'].notna()].copy()
                df['date'] = pd.to_datetime(df['date'])
                df = df[df['date'].dt.year == 2025].copy()
            if 'departement' in df.columns:
                df['departement'] = df['departement'].astype(str).str.zfill(2)
            return df
        else:
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Error loading department data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def load_sample_data():
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=30*i) for i in range(24)]
    departments = [str(i).zfill(2) for i in range(1, 96) if i not in [20]]

    data = []
    for date_idx, date in enumerate(dates):
        trend = 1 + (date_idx * 0.02)
        for dept in departments:
            base_price_gazole = 1.65 * trend + (int(dept) % 20) * 0.02
            base_price_sp95 = 1.75 * trend + (int(dept) % 20) * 0.02

            data.append({
                'departement': dept,
                'date': date,
                'gazole_moyen': base_price_gazole + (hash(f"{dept}gazole{date}") % 100 - 50) / 1000,
                'sp95_moyen': base_price_sp95 + (hash(f"{dept}sp95{date}") % 100 - 50) / 1000,
                'sp98_moyen': base_price_sp95 + 0.1 + (hash(f"{dept}sp98{date}") % 100 - 50) / 1000,
                'e10_moyen': base_price_sp95 - 0.05 + (hash(f"{dept}e10{date}") % 100 - 50) / 1000,
                'nombre_stations': 50 + (int(dept) % 30)
            })

    return pd.DataFrame(data)

def unpivot_fuel_prices(df):
    fuel_columns = {
        'gazole_moyen': 'Gazole',
        'sp95_moyen': 'SP95',
        'sp98_moyen': 'SP98',
        'e10_moyen': 'E10',
        'e85_moyen': 'E85',
        'gplc_moyen': 'GPLc'
    }

    available_fuels = {col: name for col, name in fuel_columns.items() if col in df.columns}

    if not available_fuels:
        return pd.DataFrame()

    unpivoted_dfs = []
    for col, fuel_name in available_fuels.items():
        cols_to_keep = ['departement', col, 'nombre_stations']
        if 'date' in df.columns:
            cols_to_keep.append('date')

        df_fuel = df[cols_to_keep].copy()
        df_fuel = df_fuel.rename(columns={col: 'mean_price'})
        df_fuel['fuel_type'] = fuel_name
        df_fuel = df_fuel[df_fuel['mean_price'].notna()]
        unpivoted_dfs.append(df_fuel)

    if unpivoted_dfs:
        return pd.concat(unpivoted_dfs, ignore_index=True)
    else:
        return pd.DataFrame()

def aggregate_by_region(df_dept):
    dept_to_region = get_department_to_region_mapping()

    df = df_dept.copy()
    df['region'] = df['departement'].map(dept_to_region)

    df = df[df['region'].notna()].copy()

    group_cols = ['region']
    if 'date' in df.columns:
        group_cols.append('date')
    if 'fuel_type' in df.columns:
        group_cols.append('fuel_type')

    agg_dict = {'mean_price': 'mean'}

    if 'n_stations' in df.columns:
        agg_dict['n_stations'] = 'sum'
    elif 'nombre_stations' in df.columns:
        agg_dict['nombre_stations'] = 'sum'

    df_region = df.groupby(group_cols).agg(agg_dict).reset_index()

    return df_region

def create_hover_text(row, level='department'):
    if level == 'region':
        text = f"<b>Region {row['region']}</b><br><br>"
    else:
        text = f"<b>Department {row['departement']}</b><br><br>"

    text += f"<b>Average Price:</b> {row['mean_price']:.3f} €/L<br>"
    text += f"<b>Stations:</b> {int(row['n_stations'])}<br>"
    return text

def create_choropleth_map(df_viz, geojson_data, fuel_type, level='department'):
    if df_viz.empty or geojson_data is None:
        return None

    location_col = 'region' if level == 'region' else 'departement'

    vmin = df_viz['mean_price'].quantile(0.05)
    vmax = df_viz['mean_price'].quantile(0.95)

    m = folium.Map(
        location=[46.603354, 1.888334],
        zoom_start=6,
        tiles='OpenStreetMap',
        attr='OpenStreetMap',
        scrollWheelZoom=False,
        dragging=True,
        zoomControl=True,
        doubleClickZoom=True
    )

    colormap = cm.LinearColormap(
        colors=['#10b981', '#fbbf24', '#ef4444'],
        vmin=vmin,
        vmax=vmax
    )

    avg_price = df_viz['mean_price'].mean()
    n_entities = len(df_viz)
    level_label = "Regions" if level == 'region' else "Departments"

    price_range = vmax - vmin
    tick_1 = vmin
    tick_2 = vmin + price_range * 0.25
    tick_3 = vmin + price_range * 0.5
    tick_4 = vmin + price_range * 0.75
    tick_5 = vmax

    legend_html = f'''
    <div style="position: fixed;
                bottom: 30px;
                left: 30px;
                width: 280px;
                background: linear-gradient(135deg, #ffffff 0%, #f8fafc 100%);
                border: 2px solid #1e293b;
                border-radius: 12px;
                padding: 20px;
                font-family: Inter, sans-serif;
                box-shadow: 0 10px 25px rgba(0,0,0,0.15);
                z-index: 9999;">

        <!-- Header -->
        <div style="text-align: center; margin-bottom: 15px;">
            <h3 style="margin: 0 0 5px 0;
                       font-size: 16px;
                       font-weight: 700;
                       color: #0f172a;
                       letter-spacing: -0.02em;">
                {fuel_type} Price Scale
            </h3>
            <p style="margin: 0;
                      font-size: 11px;
                      color: #64748b;
                      font-weight: 500;">
                Average price by {level_label.lower()}
            </p>
        </div>

        <!-- Color Gradient Bar -->
        <div style="position: relative; margin: 15px 0;">
            <div style="background: linear-gradient(to right, #10b981, #84cc16, #fbbf24, #fb923c, #ef4444);
                        height: 20px;
                        border-radius: 6px;
                        box-shadow: inset 0 2px 4px rgba(0,0,0,0.1);">
            </div>

            <!-- Tick marks on the gradient -->
            <div style="position: relative; height: 30px; margin-top: 5px;">
                <div style="position: absolute; left: 0%; transform: translateX(-50%);">
                    <div style="width: 2px; height: 8px; background: #1e293b; margin: 0 auto;"></div>
                    <span style="font-size: 10px; color: #475569; font-weight: 600; white-space: nowrap;">
                        {tick_1:.3f}€
                    </span>
                </div>
                <div style="position: absolute; left: 25%; transform: translateX(-50%);">
                    <div style="width: 2px; height: 6px; background: #64748b; margin: 0 auto;"></div>
                    <span style="font-size: 9px; color: #64748b; font-weight: 500;">
                        {tick_2:.3f}€
                    </span>
                </div>
                <div style="position: absolute; left: 50%; transform: translateX(-50%);">
                    <div style="width: 2px; height: 8px; background: #1e293b; margin: 0 auto;"></div>
                    <span style="font-size: 10px; color: #475569; font-weight: 600;">
                        {tick_3:.3f}€
                    </span>
                </div>
                <div style="position: absolute; left: 75%; transform: translateX(-50%);">
                    <div style="width: 2px; height: 6px; background: #64748b; margin: 0 auto;"></div>
                    <span style="font-size: 9px; color: #64748b; font-weight: 500;">
                        {tick_4:.3f}€
                    </span>
                </div>
                <div style="position: absolute; left: 100%; transform: translateX(-50%);">
                    <div style="width: 2px; height: 8px; background: #1e293b; margin: 0 auto;"></div>
                    <span style="font-size: 10px; color: #475569; font-weight: 600; white-space: nowrap;">
                        {tick_5:.3f}€
                    </span>
                </div>
            </div>
        </div>

        <!-- Price Indicators -->
        <div style="margin: 20px 0 15px 0;
                    padding: 12px;
                    background: #f1f5f9;
                    border-radius: 8px;
                    border-left: 4px solid #3b82f6;">
            <div style="display: flex; justify-content: space-between; margin-bottom: 8px;">
                <span style="font-size: 11px; color: #64748b; font-weight: 500;">
                    National Average:
                </span>
                <span style="font-size: 12px; color: #0f172a; font-weight: 700;">
                    {avg_price:.3f}€/L
                </span>
            </div>
            <div style="display: flex; justify-content: space-between;">
                <span style="font-size: 11px; color: #64748b; font-weight: 500;">
                    Price Range:
                </span>
                <span style="font-size: 11px; color: #0f172a; font-weight: 600;">
                    {price_range:.3f}€
                </span>
            </div>
        </div>

        <!-- Color Legend -->
        <div style="padding: 12px 0;
                    border-top: 1px solid #e2e8f0;
                    border-bottom: 1px solid #e2e8f0;">
            <div style="display: flex; align-items: center; margin-bottom: 6px;">
                <div style="width: 14px; height: 14px; background: #10b981; border-radius: 3px; margin-right: 8px;"></div>
                <span style="font-size: 11px; color: #0f172a; font-weight: 500;">Low prices (cheapest)</span>
            </div>
            <div style="display: flex; align-items: center; margin-bottom: 6px;">
                <div style="width: 14px; height: 14px; background: #fbbf24; border-radius: 3px; margin-right: 8px;"></div>
                <span style="font-size: 11px; color: #0f172a; font-weight: 500;">Medium prices</span>
            </div>
            <div style="display: flex; align-items: center;">
                <div style="width: 14px; height: 14px; background: #ef4444; border-radius: 3px; margin-right: 8px;"></div>
                <span style="font-size: 11px; color: #0f172a; font-weight: 500;">High prices (expensive)</span>
            </div>
        </div>

        <!-- Footer Info -->
        <div style="margin-top: 12px; text-align: center;">
            <span style="font-size: 10px; color: #94a3b8; font-weight: 500;">
                {n_entities} {level_label} analyzed
            </span>
        </div>
    </div>
    '''

    price_dict = df_viz.set_index(location_col)['mean_price'].to_dict()

    def style_function(feature):
        code = feature['properties']['code']
        price = price_dict.get(code, None)

        if price is None:
            return {
                'fillColor': '#d1d5db',
                'color': '#1e293b',
                'weight': 1.5,
                'fillOpacity': 0.3
            }

        return {
            'fillColor': colormap(price),
            'color': '#1e293b',
            'weight': 1.5,
            'fillOpacity': 0.85
        }

    def highlight_function(feature):
        return {
            'fillOpacity': 1.0,
            'weight': 3,
            'color': '#0f172a'
        }

    n_stations_dict = df_viz.set_index(location_col)['n_stations'].to_dict() if 'n_stations' in df_viz.columns else {}

    for feature in geojson_data['features']:
        code = feature['properties']['code']
        price = price_dict.get(code)
        n_stations = n_stations_dict.get(code, 'N/A')

        if price is not None:
            feature['properties']['price'] = f"{price:.3f}"
            feature['properties']['n_stations'] = str(n_stations)

    folium.GeoJson(
        geojson_data,
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['nom', 'code', 'price', 'n_stations'],
            aliases=['Name:', f'{level.capitalize()} Code:', 'Price (€/L):', 'Stations:'],
            style='font-family: Inter, sans-serif; font-size: 13px;'
        ),
        popup=folium.GeoJsonPopup(
            fields=['nom', 'code', 'price', 'n_stations'],
            aliases=['Name:', f'{level.capitalize()}:', 'Price (€/L):', 'Stations:'],
            labels=True,
            style='font-family: Inter, sans-serif;'
        )
    ).add_to(m)

    m.get_root().html.add_child(folium.Element(legend_html))

    return m

def create_evolution_chart(df, fuel_type):
    if 'date' not in df.columns or df.empty:
        return None

    df_chart = df.groupby('date')['mean_price'].mean().reset_index()
    df_chart = df_chart.sort_values('date')

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_chart['date'],
        y=df_chart['mean_price'],
        mode='lines+markers',
        name=fuel_type,
        line=dict(color='#3B82F6', width=3),
        marker=dict(size=6, color='#2563EB'),
        fill='tozeroy',
        fillcolor='rgba(59, 130, 246, 0.1)'
    ))

    fig.update_layout(
        title=dict(
            text=f"<b>National Average Price Evolution - {fuel_type}</b>",
            font=dict(size=20, color='#0F172A', family='Inter'),
            x=0.5,
            xanchor='center'
        ),
        xaxis=dict(
            title="Date",
            gridcolor='#E2E8F0',
            showgrid=True,
            zeroline=False,
            color='#64748B'
        ),
        yaxis=dict(
            title="Price (€/L)",
            gridcolor='#E2E8F0',
            showgrid=True,
            zeroline=False,
            tickformat='.3f',
            color='#64748B'
        ),
        height=300,
        margin=dict(l=60, r=20, t=60, b=40),
        paper_bgcolor='white',
        plot_bgcolor='white',
        hovermode='x unified',
        font=dict(family='Inter', size=12)
    )

    return fig

def main():
    st.markdown("<h1>Fuel Prices Map - France</h1>", unsafe_allow_html=True)
    st.markdown("<p class='subtitle'>Interactive visualization of price evolution by department</p>", unsafe_allow_html=True)

    with st.spinner("Loading data..."):
        geojson_dept = load_geojson()
        geojson_regions = load_regions_geojson()
        df_dept = load_departement_data()

        if not df_dept.empty:
            st.sidebar.success("Data loaded from GCS")

    if df_dept.empty:
        st.error("No data available.")
        return

    st.sidebar.markdown("---")
    st.sidebar.header("Settings")

    geo_level = st.sidebar.radio(
        "Geographic Level",
        options=["Departments", "Regions"],
        index=0,
        horizontal=True
    )
    level = 'region' if geo_level == "Regions" else 'department'

    df_unpivoted = unpivot_fuel_prices(df_dept)

    if df_unpivoted.empty:
        st.error("Unable to load price data")
        return

    if level == 'region':
        df_unpivoted = aggregate_by_region(df_unpivoted)
        geojson_data = geojson_regions
    else:
        geojson_data = geojson_dept

    fuel_types = sorted(df_unpivoted['fuel_type'].unique().tolist())
    default_fuel = 'SP95' if 'SP95' in fuel_types else fuel_types[0]
    default_idx = fuel_types.index(default_fuel)

    fuel_type = st.sidebar.selectbox("Fuel Type", fuel_types, index=default_idx)

    df_fuel = df_unpivoted[df_unpivoted['fuel_type'] == fuel_type].copy()

    if 'date' in df_fuel.columns:
        dates_available = sorted(df_fuel['date'].dt.date.unique())
        selected_date = dates_available[-1]
        df_viz = df_fuel[df_fuel['date'].dt.date == selected_date].copy()
        selected_date_dt = pd.to_datetime(selected_date)
    else:
        df_viz = df_fuel.copy()
        selected_date_dt = None

    df_viz = df_viz.rename(columns={'nombre_stations': 'n_stations'})

    if level == 'department':
        df_viz['departement'] = df_viz['departement'].astype(str).str.zfill(2)
    else:
        df_viz['region'] = df_viz['region'].astype(str)

    if not df_viz.empty:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(f"""
            <div class='metric-card'>
                <h3>Average Price</h3>
                <h2>{df_viz['mean_price'].mean():.3f} €/L</h2>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
            <div class='metric-card'>
                <h3>Minimum Price</h3>
                <h2>{df_viz['mean_price'].min():.3f} €/L</h2>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            st.markdown(f"""
            <div class='metric-card'>
                <h3>Maximum Price</h3>
                <h2>{df_viz['mean_price'].max():.3f} €/L</h2>
            </div>
            """, unsafe_allow_html=True)

        with col4:
            geo_count_label = "Regions" if level == 'region' else "Departments"
            st.markdown(f"""
            <div class='metric-card'>
                <h3>{geo_count_label}</h3>
                <h2>{len(df_viz)}</h2>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    level_label = "Region" if level == 'region' else "Department"
    title_html = f"<h2 style='text-align: center; color: #0F172A;'>{fuel_type} Prices by {level_label}</h2>"
    if selected_date_dt:
        date_str = selected_date_dt.strftime('%B %Y')
        title_html += f"<p style='text-align: center; color: #64748B; font-size: 16px;'>{date_str}</p>"
    st.markdown(title_html, unsafe_allow_html=True)

    with st.spinner("Creating map..."):
        folium_map = create_choropleth_map(df_viz, geojson_data, fuel_type, level=level)

    if folium_map:
        st_folium(folium_map, width=1400, height=700, key='main_map', returned_objects=[])
    else:
        st.error("Unable to create map. Please check the data.")

    st.sidebar.markdown("---")
    geo_col = 'region' if level == 'region' else 'departement'
    geo_label = 'Regions' if level == 'region' else 'Departments'

    st.sidebar.markdown(f"""
    ### Details
    **GCS Bucket**: `{GCS_BUCKET_NAME}`
    **{geo_label}**: {df_viz[geo_col].nunique()}
    """)

if __name__ == "__main__":
    main()
