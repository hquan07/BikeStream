import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
import folium
from streamlit_folium import st_folium
import requests
from datetime import datetime, timedelta
import os
import json

# --- Page Config ---
st.set_page_config(
    page_title="BikeStream Real-Time",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- Custom Glassmorphism CSS ---
st.markdown("""
<style>
/* Hide Default Streamlit Elements for a cleaner app look */
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
header {visibility: hidden;}

/* Global Background */
.stApp {
    background: linear-gradient(135deg, #0f0c29 0%, #302b63 50%, #24243e 100%) !important;
    color: #ffffff;
    font-family: 'Outfit', sans-serif;
}

/* Animations */
@keyframes fadeIn {
    from { opacity: 0; transform: translateY(20px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes slideDown {
    from { opacity: 0; transform: translateY(-30px); }
    to { opacity: 1; transform: translateY(0); }
}
@keyframes pulse {
    0% { box-shadow: 0 0 0 0 rgba(220, 53, 69, 0.7); }
    70% { box-shadow: 0 0 0 15px rgba(220, 53, 69, 0); }
    100% { box-shadow: 0 0 0 0 rgba(220, 53, 69, 0); }
}

/* Apply fade in to the main container */
.main .block-container {
    animation: fadeIn 1s ease;
}

/* Main Title Gradient & Center */
h1 {
    text-align: center !important;
    background: -webkit-linear-gradient(45deg, #00d2ff, #3a7bd5);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    font-weight: 800;
    font-size: 3.5rem !important;
    margin-bottom: 30px;
    animation: slideDown 1s ease;
}

/* Headings */
h2, h3 {
    color: #ffffff !important;
    text-align: center;
    margin-top: 10px;
    margin-bottom: 20px;
}

/* Metric Cards (Value Boxes) */
div[data-testid="stMetric"] {
    background: rgba(25, 25, 35, 0.5) !important;
    border: 1px solid rgba(255, 255, 255, 0.1) !important;
    border-radius: 15px !important;
    padding: 20px !important;
    backdrop-filter: blur(12px) !important;
    -webkit-backdrop-filter: blur(12px) !important;
    box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3) !important;
    transition: all 0.3s ease !important;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
}

div[data-testid="stMetric"]:hover {
    transform: translateY(-5px) !important;
    box-shadow: 0 0 25px rgba(0, 212, 255, 0.5) !important;
    border-color: rgba(0, 212, 255, 0.7) !important;
}

/* Target the specific alert metric by its label using nth-child or if we can */
div[data-testid="stMetric"]:nth-of-type(4) {
    animation: pulse 2s infinite !important;
    border-color: rgba(220, 53, 69, 0.5) !important;
}

/* Center Metric Labels & Values */
div[data-testid="stMetricValue"] {
    text-align: center !important;
    justify-content: center !important;
    font-size: 2.8rem !important;
    font-weight: 700 !important;
    background: -webkit-linear-gradient(45deg, #00d2ff, #3a7bd5);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
div[data-testid="stMetricLabel"] {
    text-align: center !important;
    justify-content: center !important;
    font-size: 1.2rem !important;
    color: #a0a0b0 !important;
}
div[data-testid="stMetricDelta"] {
    justify-content: center !important;
}

/* Sleek Tabs */
.stTabs [data-baseweb="tab-list"] {
    gap: 15px;
    background-color: rgba(25, 25, 35, 0.4);
    padding: 10px;
    border-radius: 15px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    backdrop-filter: blur(12px);
    justify-content: center;
}
.stTabs [data-baseweb="tab"] {
    height: 45px;
    white-space: pre-wrap;
    background-color: transparent;
    border-radius: 10px;
    color: #ffffff;
    font-weight: 600;
    font-size: 1.1rem;
    padding: 0 20px;
    transition: all 0.3s ease;
}
.stTabs [aria-selected="true"] {
    background-color: rgba(0, 212, 255, 0.3) !important;
    border: 1px solid rgba(0, 212, 255, 0.5);
    color: white !important;
    box-shadow: 0 0 15px rgba(0, 212, 255, 0.4);
}
.stTabs [data-baseweb="tab"]:hover {
    background-color: rgba(255, 255, 255, 0.2);
}

/* Dataframe glassmorphism wrapper */
[data-testid="stDataFrame"] {
    border-radius: 15px;
    overflow: hidden;
    box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
    border: 1px solid rgba(255, 255, 255, 0.1);
}
</style>
""", unsafe_allow_html=True)

# --- Database Connection ---
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "bikestream"),
        password=os.getenv("POSTGRES_PASSWORD", "bikestream_2024"),
        dbname=os.getenv("POSTGRES_DB", "bikestream")
    )

conn = init_connection()

# --- Data Fetching Functions ---
def get_live_stations():
    query = """
        SELECT DISTINCT ON (city, station_id)
            city, station_id, station_name, lat, lon,
            num_bikes_available, num_docks_available, num_ebikes_available,
            fill_ratio, status, capacity, time
        FROM station_snapshots
        ORDER BY city, station_id, time DESC;
    """
    return pd.read_sql(query, conn)

def get_kpi_data():
    query = """
        SELECT city,
            SUM(num_bikes_available) as total_bikes,
            SUM(num_docks_available) as total_docks,
            SUM(num_ebikes_available) as total_ebikes,
            COUNT(*) as total_stations,
            SUM(CASE WHEN needs_rebalancing THEN 1 ELSE 0 END) as alert_count,
            AVG(fill_ratio) as avg_utilization
        FROM station_snapshots
        WHERE time > NOW() - INTERVAL '1 minute'
        GROUP BY city;
    """
    df = pd.read_sql(query, conn)
    if df.empty:
        return pd.DataFrame({"city": ["No Data"], "total_stations":[0], "total_bikes":[0], "total_docks":[0], "alert_count":[0], "avg_utilization":[0.0]})
    return df

def get_health_trend():
    query = """
        SELECT time_bucket('15 minutes', time) AS bucket,
               status,
               COUNT(*) as count
        FROM station_snapshots
        WHERE time > NOW() - INTERVAL '12 hours'
          AND status IS NOT NULL
        GROUP BY bucket, status
        ORDER BY bucket, status;
    """
    return pd.read_sql(query, conn)

def get_heatmap_data():
    query = """
        SELECT time_bucket('15 minutes', time) AS bucket,
            city,
            AVG(fill_ratio) AS avg_fill
        FROM station_snapshots
        WHERE time > NOW() - INTERVAL '12 hours'
        GROUP BY bucket, city
        ORDER BY bucket, city;
    """
    return pd.read_sql(query, conn)

def get_alerts():
    query = """
        SELECT city as "City", station_name as "Station", status as "Status",
            num_bikes_available as "Bikes", num_docks_available as "Docks", capacity as "Capacity",
            time as "Last_Seen"
        FROM station_snapshots
        WHERE needs_rebalancing = TRUE
          AND time > NOW() - INTERVAL '5 minutes'
        ORDER BY time DESC
        LIMIT 100;
    """
    return pd.read_sql(query, conn)

def get_routing_stations(city):
    q_full = f"""
      SELECT DISTINCT ON (station_id)
          station_id, station_name, lat, lon,
          num_bikes_available, num_docks_available, status, 'PICKUP' as action
      FROM station_snapshots
      WHERE city = '{city}' AND status = 'FULL' AND time > NOW() - INTERVAL '5 minutes'
      ORDER BY station_id, time DESC LIMIT 3;
    """
    q_empty = f"""
      SELECT DISTINCT ON (station_id)
          station_id, station_name, lat, lon,
          num_bikes_available, num_docks_available, status, 'DROPOFF' as action
      FROM station_snapshots
      WHERE city = '{city}' AND status = 'EMPTY' AND time > NOW() - INTERVAL '5 minutes'
      ORDER BY station_id, time DESC LIMIT 3;
    """
    df_full = pd.read_sql(q_full, conn)
    df_empty = pd.read_sql(q_empty, conn)
    return pd.concat([df_full, df_empty])

# --- Main App ---
st.title("🚲 BikeStream Real-Time")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Live Map", "System KPIs", "Alerts (Rebalancing)", "Trends", "Fleet Routing"])

# --- Tab 1: Live Map ---
with tab1:
    df_stations = get_live_stations()
    
    col_map, col_stats = st.columns([3, 1])
    
    with col_map:
        m = folium.Map(location=[41.88, -87.63], zoom_start=4, tiles="https://services.arcgisonline.com/arcgis/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}", attr="Esri")
        if not df_stations.empty:
            color_map = {"EMPTY": "red", "LOW": "orange", "HEALTHY": "green", "FULL": "blue"}
            for _, row in df_stations.iterrows():
                folium.CircleMarker(
                    location=[row['lat'], row['lon']],
                    radius=6,
                    color=color_map.get(row['status'], "gray"),
                    fill=True,
                    fill_opacity=0.9,
                    popup=f"<b>{row['station_name']}</b><br>Status: {row['status']}<br>Bikes: {row['num_bikes_available']}<br>Docks: {row['num_docks_available']}",
                    tooltip=f"🚲 {row['station_name']} ({row['num_bikes_available']} bikes)"
                ).add_to(m)
        st_folium(m, use_container_width=True, height=600, returned_objects=[])
        
    with col_stats:
        st.markdown("### 📊 Quick Insights")
        if not df_stations.empty:
            st.markdown("#### 🗺️ Map Legend")
            st.markdown("🔴 **EMPTY:** No bikes available")
            st.markdown("🟠 **LOW:** Almost empty (1-3 bikes)")
            st.markdown("🟢 **HEALTHY:** Good status")
            st.markdown("🔵 **FULL:** No docks available")
            st.markdown("---")
            
            total_live = len(df_stations)
            empty_stations = len(df_stations[df_stations['status'] == 'EMPTY'])
            healthy_stations = len(df_stations[df_stations['status'] == 'HEALTHY'])
            
            st.info(f"**📍 Tracking {total_live} Stations**")
            st.error(f"**🚨 {empty_stations} Stations Empty**")
            st.success(f"**✅ {healthy_stations} Stations Healthy**")
            
        else:
            st.warning("Waiting for live data...")

# --- Tab 2: System KPIs ---
with tab2:
    df_kpi = get_kpi_data()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Active Stations", int(df_kpi['total_stations'].sum()))
    with col2:
        st.metric("Total Bikes Available", int(df_kpi['total_bikes'].sum()))
    with col3:
        st.metric("Total Docks Available", int(df_kpi['total_docks'].sum()))
    with col4:
        st.metric("Needs Rebalancing", int(df_kpi['alert_count'].sum()))
    
    st.subheader("City Overview")
    st.dataframe(df_kpi[['city', 'total_stations', 'total_bikes', 'total_docks', 'alert_count', 'avg_utilization']], use_container_width=True)

# --- Tab 3: Alerts ---
with tab3:
    st.subheader("Stations needing immediate rebalancing")
    df_alerts = get_alerts()
    st.dataframe(df_alerts, use_container_width=True)

# --- Tab 4: Trends ---
with tab4:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("System Health Composition")
        df_health = get_health_trend()
        if not df_health.empty:
            fig = px.area(df_health, x="bucket", y="count", color="status",
                          color_discrete_map={"EMPTY": "#dc3545", "LOW": "#fd7e14", "HEALTHY": "#20c997", "FULL": "#0d6efd"})
            fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig, use_container_width=True)
            
    with col2:
        st.subheader("City Utilization Heatmap")
        df_heat = get_heatmap_data()
        if not df_heat.empty:
            fig2 = px.density_heatmap(df_heat, x="bucket", y="city", z="avg_fill", histfunc="avg", color_continuous_scale="Viridis")
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font_color="white")
            st.plotly_chart(fig2, use_container_width=True)

# --- Tab 5: Fleet Routing ---
with tab5:
    col1, col2 = st.columns([1, 2])
    with col1:
        city_sel = st.selectbox("Select City", ["chicago", "new_york", "san_francisco", "washington_dc", "boston"])
        if st.button("Calculate Route"):
            df_route = get_routing_stations(city_sel)
            if len(df_route) < 2:
                st.warning("Not enough stations to route.")
            else:
                st.dataframe(df_route[['action', 'station_name', 'num_bikes_available']])
                
                coords = ";".join([f"{lon},{lat}" for lon, lat in zip(df_route['lon'], df_route['lat'])])
                osrm_url = f"https://router.project-osrm.org/route/v1/driving/{coords}?overview=full&geometries=geojson"
                try:
                    resp = requests.get(osrm_url).json()
                    if resp.get('code') == 'Ok':
                        route_geom = resp['routes'][0]['geometry']
                        distance = round(resp['routes'][0]['distance'] / 1000, 1)
                        duration = round(resp['routes'][0]['duration'] / 60, 0)
                        st.success(f"Route calculated! {distance} km, {duration} mins.")
                        
                        m_route = folium.Map(location=[df_route['lat'].mean(), df_route['lon'].mean()], zoom_start=12, tiles="https://services.arcgisonline.com/arcgis/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}", attr="Esri")
                        folium.GeoJson(route_geom, name="Route").add_to(m_route)
                        for _, r in df_route.iterrows():
                            c = "#0d6efd" if r['action'] == "PICKUP" else "#dc3545"
                            folium.Marker([r['lat'], r['lon']], icon=folium.Icon(color="blue" if r['action'] == "PICKUP" else "red")).add_to(m_route)
                        with col2:
                            st.subheader("Fleet Dispatch Route")
                            st_folium(m_route, width=800, height=500, returned_objects=[])
                except Exception as e:
                    st.error(f"Routing failed: {e}")
