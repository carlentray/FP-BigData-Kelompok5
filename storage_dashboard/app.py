import streamlit as st
import pandas as pd
import psycopg2
import folium
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime

# Setup Page Configuration
st.set_page_config(
    page_title="ShelterEye - Transjakarta Real-Time Analytics",
    page_icon="🚌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Sleek CSS Styles for Premium Aesthetics
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
        color: #ffffff;
    }
    .stMetric {
        background-color: #1e222b;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #ff4b4b;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    .recommendation-card {
        background-color: #1f1b24;
        border-left: 5px solid #ff9800;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 10px;
        color: #ffffff;
    }
    .header-style {
        background: linear-gradient(90deg, #ff4b4b 0%, #ff9800 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        font-size: 2.8rem;
        margin-bottom: 10px;
    }
    .sub-header-style {
        color: #8892b0;
        font-size: 1.1rem;
        margin-bottom: 25px;
    }
    </style>
""", unsafe_allow_html=True)

# Database connection helper
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="db_sheltereye",
        user="admin",
        password="sheltereyepassword"
    )

# Halte Ground Truth Coordinates
HALTE_COORDINATES = {
    "Dukuh Atas 1": {"lat": -6.2001, "lon": 106.8000},
    "Karet Sudirman": {"lat": -6.2120, "lon": 106.8200},
    "Monas": {"lat": -6.1754, "lon": 106.8271},
    "Harmoni Central": {"lat": -6.1601, "lon": 106.8164},
    "Manggarai": {"lat": -6.2084, "lon": 106.8483},
    "Kampung Melayu": {"lat": -6.2244, "lon": 106.8576},
    "Senen": {"lat": -6.1744, "lon": 106.8431}
}

# Load Data from Database
def load_data():
    try:
        conn = get_db_connection()
        
        # 1. Load Bus ETA
        df_eta = pd.read_sql("SELECT * FROM bus_eta", conn)
        
        # 2. Load Passenger Density
        df_density = pd.read_sql("SELECT * FROM passenger_density", conn)
        
        # 3. Load Recommendations
        df_recommendations = pd.read_sql("SELECT * FROM system_recommendations", conn)
        
        conn.close()
        return df_eta, df_density, df_recommendations, None
    except Exception as e:
        return None, None, None, str(e)

# Layout Title
st.markdown('<div class="header-style">ShelterEye Dashboard</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header-style">Transjakarta Real-Time Streaming Analytics & Dynamic Headway Recommendation System</div>', unsafe_allow_html=True)

# Auto Refresh Control in Sidebar
st.sidebar.header("⏱️ Controls & Info")
auto_refresh = st.sidebar.checkbox("Auto Refresh (Real-Time 3s)", value=True)
refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", min_value=1, max_value=10, value=3)

# Load data
df_eta, df_density, df_recommendations, error_msg = load_data()

if error_msg:
    st.error(f"Failed to connect to database. Make sure Docker is running! Error: {error_msg}")
else:
    # ------------------ SIDEBAR STATS ------------------
    st.sidebar.markdown("---")
    st.sidebar.subheader("📈 Quick Statistics")
    
    total_active_buses = len(df_eta[df_eta['bus_id'].str.startswith('TJ-')]) if df_eta is not None else 0
    st.sidebar.metric("Active Buses", total_active_buses)
    
    total_passengers = int(df_density['passenger_count'].sum()) if df_density is not None else 0
    st.sidebar.metric("Total Waiting Passengers", total_passengers)
    
    critical_count = len(df_recommendations[df_recommendations['status'] == 'CRITICAL_SEND_BACKUP']) if df_recommendations is not None else 0
    st.sidebar.metric("Critical Alerts", critical_count)

    # ------------------ MAIN BODY ------------------
    col_map, col_details = st.columns([2, 1])

    with col_map:
        st.subheader("🗺️ Live Halte Spatial Map (ETA & Telemetry)")
        
        # Initialize Folium Map centered on Central Jakarta
        m = folium.Map(location=[-6.195, 106.825], zoom_start=13, tiles="CartoDB dark_matter")
        
        # Render Markers for each Halte
        for name, coords in HALTE_COORDINATES.items():
            # Get Passenger Density for this Halte
            halte_density_row = df_density[df_density['halte_name'] == name]
            pass_count = int(halte_density_row['passenger_count'].values[0]) if not halte_density_row.empty else 0
            status_text = halte_density_row['predicted_status'].values[0] if not halte_density_row.empty else "Normal"
            overload_pct = float(halte_density_row['predicted_overload_pct'].values[0]) if not halte_density_row.empty else 0.0
            
            # Get recommendations
            rec_row = df_recommendations[df_recommendations['halte_name'] == name]
            rec_status = rec_row['status'].values[0] if not rec_row.empty else "NORMAL"
            rec_text = rec_row['recommendation_text'].values[0] if not rec_row.empty else "Kondisi halte aman terkendali."

            # Get incoming buses
            incoming_buses = df_eta[df_eta['halte_name'] == name].sort_values(by="eta_minutes")
            buses_html = ""
            if not incoming_buses.empty:
                buses_html = "<h4>Incoming Buses:</h4><table style='width:100%; border: 1px solid white; border-collapse: collapse; text-align: left;'>"
                buses_html += "<tr style='border-bottom: 1px solid white;'><th>Bus ID</th><th>ETA (Min)</th><th>Occupancy</th></tr>"
                for idx, bus in incoming_buses.head(3).iterrows():
                    occ_style = "color:#ff4b4b;" if bus['occupancy_pct'] > 70 else "color:#4caf50;"
                    buses_html += f"<tr><td>{bus['bus_id']}</td><td>{bus['eta_minutes']:.1f}m</td><td style='{occ_style}'>{bus['occupancy_pct']:.0f}%</td></tr>"
                buses_html += "</table>"
            else:
                buses_html = "<p style='color:#ff9800;'>⚠️ No buses heading to this stop</p>"

            # Determine Color Code based on status
            color = "green"
            if rec_status == "CRITICAL_SEND_BACKUP" or status_text.startswith("Potensi Overload"):
                color = "red"
            elif pass_count > 100:
                color = "orange"
                
            # Popup HTML
            popup_html = f"""
            <div style="font-family: Arial, sans-serif; color: black; width: 250px;">
                <h3 style="margin: 0 0 5px 0; color:#ff4b4b;">Halte {name}</h3>
                <p><b>Waiting Passengers:</b> {pass_count} ({status_text})</p>
                <p><b>Overload Probability:</b> {overload_pct:.1f}%</p>
                <p><b>Alert Status:</b> <span style="color:{'red' if rec_status=='CRITICAL_SEND_BACKUP' else 'green'}; font-weight:bold;">{rec_status}</span></p>
                {buses_html}
                <p style="margin-top: 10px; font-size:0.85em; color:gray;"><i>Updated: {datetime.now().strftime('%H:%M:%S')}</i></p>
            </div>
            """
            
            folium.Marker(
                location=[coords["lat"], coords["lon"]],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"Halte {name} ({pass_count} passengers)",
                icon=folium.Icon(color=color, icon="bus", prefix="fa")
            ).add_to(m)
            
        # Display Folium Map in Streamlit
        folium_static(m, width=700, height=450)

    with col_details:
        st.subheader("⚠️ System Recommendations")
        
        # Filter for recommendations that need action
        critical_recs = df_recommendations[df_recommendations['status'] == 'CRITICAL_SEND_BACKUP']
        
        if not critical_recs.empty:
            for idx, r in critical_recs.iterrows():
                st.markdown(f"""
                    <div class="recommendation-card">
                        <h4>🚨 Halte {r['halte_name']} (CRITICAL)</h4>
                        <p>{r['recommendation_text']}</p>
                        <small style="color: #8892b0;">Timestamp: {r['created_at']}</small>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.success("🟢 All stations operating within normal capacity. No backup buses required.")

    # ------------------ CHARTS SECTION ------------------
    st.markdown("---")
    col_chart1, col_chart2 = st.columns([1, 1])

    with col_chart1:
        st.subheader("📊 Halte Passenger Densities (Real-Time)")
        if not df_density.empty:
            # Render a nice Plotly bar chart
            fig_density = px.bar(
                df_density.sort_values(by="passenger_count", ascending=False),
                x="halte_name",
                y="passenger_count",
                color="predicted_overload_pct",
                color_continuous_scale=px.colors.sequential.OrRd,
                labels={"passenger_count": "Waiting Passengers", "halte_name": "Halte Name", "predicted_overload_pct": "Overload %"},
                template="plotly_dark"
            )
            fig_density.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_density, use_container_width=True)
        else:
            st.info("No passenger density data available yet.")

    with col_chart2:
        st.subheader("🚌 Live Bus Positions & Occupancies")
        
        real_buses = df_eta[df_eta['bus_id'].str.startswith('TJ-')]
        if not real_buses.empty:
            # Plot occupancy of current active buses
            fig_buses = px.bar(
                real_buses.sort_values(by="occupancy_pct", ascending=False).head(10),
                x="bus_id",
                y="occupancy_pct",
                color="occupancy_pct",
                color_continuous_scale=px.colors.sequential.Viridis,
                labels={"occupancy_pct": "Occupancy Percentage (%)", "bus_id": "Bus ID"},
                template="plotly_dark"
            )
            fig_buses.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_buses, use_container_width=True)
        else:
            st.info("No active simulated buses tracked yet.")

    # ------------------ DETAILED TABLES ------------------
    st.markdown("---")
    st.subheader("📋 Raw Database Tables View")
    tab1, tab2 = st.tabs(["🚌 Live Bus Coordinates & ETA", "👤 Halte Passenger & AI Forecast"])
    
    with tab1:
        st.dataframe(df_eta.sort_values(by="last_updated", ascending=False), use_container_width=True)
        
    with tab2:
        st.dataframe(df_density.sort_values(by="last_updated", ascending=False), use_container_width=True)

# Auto refresh logic using st.rerun()
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
