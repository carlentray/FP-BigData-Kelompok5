import streamlit as st
import pandas as pd
import psycopg2
import folium
from folium import DivIcon
from streamlit_folium import folium_static
import plotly.express as px
import plotly.graph_objects as go
import time
from datetime import datetime

# Setup Page Configuration (No emojis, professional title)
st.set_page_config(
    page_title="ShelterEye - Sistem Informasi Pemantauan Real-Time Transjakarta",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS Styles for Professional Government Web Portal (Light Theme & Navy Corporate Accent)
st.markdown("""
    <style>
    /* Main Background and Typography */
    .stApp {
        background-color: #f8fafc;
        color: #1e293b;
    }
    .main {
        background-color: #f8fafc;
        color: #1e293b;
    }
    
    /* Sidebar styling adjustment */
    [data-testid="stSidebar"] {
        background-color: #f1f5f9;
        border-right: 1px solid #cbd5e1;
    }
    
    /* Metrics block styling */
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 6px;
        border: 1px solid #cbd5e1;
        border-left: 5px solid #0b2f64;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #0b2f64;
        font-weight: 700;
    }
    .stMetric [data-testid="stMetricLabel"] {
        color: #475569;
        font-weight: 500;
    }
    
    /* Recommendation Card styling */
    .recommendation-card {
        background-color: #fffaf0;
        border: 1px solid #feebc8;
        border-left: 5px solid #dd6b20;
        padding: 15px;
        border-radius: 6px;
        margin-bottom: 12px;
        color: #2d3748;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
    }
    .recommendation-card h4 {
        margin: 0 0 5px 0;
        color: #b7791f;
        font-weight: 700;
    }
    .recommendation-card p {
        margin: 0 0 8px 0;
        font-size: 0.95rem;
    }
    
    /* Header Banner styling */
    .gov-header {
        background-color: #0b2f64;
        padding: 25px;
        border-radius: 8px;
        margin-bottom: 25px;
        color: #ffffff;
        border-bottom: 4px solid #dd6b20;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.05);
    }
    .gov-header-agency {
        font-size: 0.85rem;
        font-weight: 600;
        letter-spacing: 1.5px;
        color: #93c5fd;
        text-transform: uppercase;
    }
    .gov-header-title {
        font-size: 2.2rem;
        font-weight: 700;
        margin-top: 5px;
        margin-bottom: 5px;
    }
    .gov-header-subtitle {
        font-size: 1rem;
        color: #bfdbfe;
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
        
        # 4. Load Raw Transactions (latest 50)
        try:
            df_raw_tx = pd.read_sql("SELECT trans_id, card_id, bank, name, halte_name, tap_in_time FROM passenger_transactions ORDER BY created_at DESC LIMIT 50", conn)
        except Exception:
            df_raw_tx = pd.DataFrame(columns=["trans_id", "card_id", "bank", "name", "halte_name", "tap_in_time"])
            
        conn.close()
        return df_eta, df_density, df_recommendations, df_raw_tx, None
    except Exception as e:
        return None, None, None, None, str(e)

# Render Official Government Header Banner
st.markdown("""
    <div class="gov-header">
        <div class="gov-header-agency">Pemerintah Provinsi DKI Jakarta • Dinas Perhubungan</div>
        <div class="gov-header-title">Sistem Pemantauan ShelterEye</div>
        <div class="gov-header-subtitle">Portal Analisis Kepadatan Penumpang Real-Time & Manajemen Headway Transjakarta</div>
    </div>
""", unsafe_allow_html=True)

# Auto Refresh Control in Sidebar
st.sidebar.header("Kontrol & Informasi")
auto_refresh = st.sidebar.checkbox("Penyegaran Otomatis (Real-Time)", value=True)
refresh_rate = st.sidebar.slider("Interval Penyegaran (detik)", min_value=1, max_value=10, value=3)

# Load data
df_eta, df_density, df_recommendations, df_raw_tx, error_msg = load_data()

if error_msg:
    st.error(f"Gagal terhubung ke database. Pastikan container Docker berjalan! Error: {error_msg}")
else:
    # ------------------ SIDEBAR STATS ------------------
    st.sidebar.markdown("---")
    st.sidebar.subheader("Statistik Ringkas")
    
    total_active_buses = len(df_eta[df_eta['bus_id'].str.startswith('TJ-')]) if df_eta is not None else 0
    st.sidebar.metric("Armada Bus Aktif", total_active_buses)
    
    total_passengers = int(df_density['passenger_count'].sum()) if df_density is not None else 0
    st.sidebar.metric("Total Penumpang Menunggu", total_passengers)
    
    critical_count = len(df_recommendations[df_recommendations['status'] == 'CRITICAL_SEND_BACKUP']) if df_recommendations is not None else 0
    st.sidebar.metric("Peringatan Kritis", critical_count)

    # ------------------ MAIN BODY ------------------
    col_map, col_details = st.columns([2, 1])

    with col_map:
        st.subheader("Peta Spasial Halte (ETA & Telemetry)")
        
        # Initialize Folium Map centered on Central Jakarta (using CartoDB positron light tiles)
        m = folium.Map(location=[-6.195, 106.825], zoom_start=13, tiles="CartoDB positron")
        
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
            
            # Get incoming buses
            incoming_buses = df_eta[df_eta['halte_name'] == name].sort_values(by="eta_minutes")
            buses_html = ""
            if not incoming_buses.empty:
                buses_html = "<h4>Armada Bus Mendatang:</h4><table style='width:100%; border: 1px solid #cbd5e1; border-collapse: collapse; text-align: left; color: #2d3748;'>"
                buses_html += "<tr style='border-bottom: 1px solid #cbd5e1; background-color: #f1f5f9;'><th>ID Bus</th><th>ETA (Menit)</th><th>Okupansi</th></tr>"
                for idx, bus in incoming_buses.head(3).iterrows():
                    occ_style = "color:#dc2626; font-weight:bold;" if bus['occupancy_pct'] > 70 else "color:#16a34a; font-weight:bold;"
                    buses_html += f"<tr><td>{bus['bus_id']}</td><td>{bus['eta_minutes']:.1f}m</td><td style='{occ_style}'>{bus['occupancy_pct']:.0f}%</td></tr>"
                buses_html += "</table>"
            else:
                buses_html = "<p style='color:#d97706;'>Tidak ada bus menuju halte ini</p>"

            # Determine Color Code based on status
            color = "green"
            if rec_status == "CRITICAL_SEND_BACKUP" or status_text.startswith("Potensi Overload"):
                color = "red"
            elif pass_count > 100:
                color = "orange"
                
            # Popup HTML (styled for light background popup container)
            popup_html = f"""
            <div style="font-family: Arial, sans-serif; color: #1e293b; width: 250px;">
                <h3 style="margin: 0 0 5px 0; color:#0b2f64;">Halte {name}</h3>
                <p style="margin: 4px 0;"><b>Penumpang Menunggu:</b> {pass_count} ({status_text})</p>
                <p style="margin: 4px 0;"><b>Probabilitas Overload:</b> {overload_pct:.1f}%</p>
                <p style="margin: 4px 0;"><b>Status Peringatan:</b> <span style="color:{'#dc2626' if rec_status=='CRITICAL_SEND_BACKUP' else '#16a34a'}; font-weight:bold;">{rec_status}</span></p>
                {buses_html}
                <p style="margin-top: 10px; font-size:0.8rem; color:#64748b;"><i>Diperbarui: {datetime.now().strftime('%H:%M:%S')}</i></p>
            </div>
            """
            
            folium.Marker(
                location=[coords["lat"], coords["lon"]],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=f"Halte {name} ({pass_count} penumpang)",
                icon=folium.Icon(color=color, icon="bus", prefix="fa")
            ).add_to(m)
            
        # Draw Real-Time Bus GPS Markers (Tracking Navigation)
        real_buses = df_eta[df_eta['bus_id'].str.startswith('TJ-')]
        for idx, bus in real_buses.iterrows():
            if pd.notna(bus['bus_lat']) and pd.notna(bus['bus_lon']):
                bus_popup_html = f"""
                <div style="font-family: Arial, sans-serif; color: #1e293b; width: 200px;">
                    <h4 style="margin: 0 0 5px 0; color:#0b2f64;">Bus {bus['bus_id']}</h4>
                    <p style="margin: 4px 0;"><b>Halte Berikutnya:</b> {bus['halte_name']}</p>
                    <p style="margin: 4px 0;"><b>ETA:</b> {bus['eta_minutes']:.1f} menit</p>
                    <p style="margin: 4px 0;"><b>Kapasitas Terisi:</b> {bus['current_passenger_count']} penumpang ({bus['occupancy_pct']:.0f}%)</p>
                </div>
                """
                # Flat, modern circular navy badge with white bus SVG icon (Fleet Management Style)
                bus_icon_html = """
                <div style="
                    background-color: #0b2f64; 
                    border: 2px solid #ffffff; 
                    border-radius: 50%; 
                    width: 30px; 
                    height: 30px; 
                    display: flex; 
                    align-items: center; 
                    justify-content: center; 
                    box-shadow: 0 2px 4px rgba(0,0,0,0.25);
                ">
                    <svg viewBox="0 0 24 24" width="15" height="15" fill="#ffffff">
                        <path d="M18 11H6V6h12v5zm-1.5 4c-.83 0-1.5-.67-1.5-1.5s.67-1.5 1.5-1.5 1.5.67 1.5 1.5-.67 1.5-1.5 1.5zm-9 0c-.83 0-1.5-.67-1.5-1.5S8.17 12 9 12s1.5.67 1.5 1.5S9.83 15 7.5 15zm11.5-11H5c-1.1 0-2 .9-2 2v12c0 1.1.9 2 2 2h1v1.5c0 .83.67 1.5 1.5 1.5h1c.83 0 1.5-.67 1.5-1.5V18h6v1.5c0 .83.67 1.5 1.5 1.5h1c.83 0 1.5-.67 1.5-1.5V18h1c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2z"/>
                    </svg>
                </div>
                """
                folium.Marker(
                    location=[bus['bus_lat'], bus['bus_lon']],
                    popup=folium.Popup(bus_popup_html, max_width=250),
                    tooltip=f"Bus {bus['bus_id']} (Menuju Halte {bus['halte_name']})",
                    icon=DivIcon(
                        icon_size=(30, 30),
                        icon_anchor=(15, 15),
                        html=bus_icon_html
                    )
                ).add_to(m)
            
        # Display Folium Map in Streamlit
        folium_static(m, width=700, height=450)

    with col_details:
        st.subheader("Rekomendasi Sistem & Peringatan")
        
        # Filter for recommendations that need action
        critical_recs = df_recommendations[df_recommendations['status'] == 'CRITICAL_SEND_BACKUP']
        
        if not critical_recs.empty:
            for idx, r in critical_recs.iterrows():
                st.markdown(f"""
                    <div class="recommendation-card">
                        <h4>Halte {r['halte_name']} (Kritis)</h4>
                        <p>{r['recommendation_text']}</p>
                        <small style="color: #64748b;">Waktu: {r['created_at']}</small>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.success("Semua halte beroperasi dalam kapasitas normal. Tidak diperlukan armada bus bantuan saat ini.")

    # ------------------ CHARTS SECTION ------------------
    st.markdown("---")
    col_chart1, col_chart2 = st.columns([1, 1])

    with col_chart1:
        st.subheader("Kepadatan Penumpang Halte (Real-Time)")
        if not df_density.empty:
            fig_density = px.bar(
                df_density.sort_values(by="passenger_count", ascending=False),
                x="halte_name",
                y="passenger_count",
                color="predicted_overload_pct",
                color_continuous_scale=px.colors.sequential.OrRd,
                labels={"passenger_count": "Jumlah Penumpang Menunggu", "halte_name": "Nama Halte", "predicted_overload_pct": "Probabilitas Overload (%)"}
            )
            fig_density.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_density, use_container_width=True)
        else:
            st.info("Data kepadatan penumpang belum tersedia.")

    with col_chart2:
        st.subheader("Okupansi & Posisi Armada Bus Aktif")
        
        if not real_buses.empty:
            fig_buses = px.bar(
                real_buses.sort_values(by="occupancy_pct", ascending=False).head(10),
                x="bus_id",
                y="occupancy_pct",
                color="occupancy_pct",
                color_continuous_scale=px.colors.sequential.Viridis,
                labels={"occupancy_pct": "Persentase Okupansi (%)", "bus_id": "ID Bus"}
            )
            fig_buses.update_layout(height=350, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig_buses, use_container_width=True)
        else:
            st.info("Belum ada armada bus tersimulasi yang aktif.")

    # ------------------ DETAILED TABLES ------------------
    st.markdown("---")
    st.subheader("Data Tabular Real-Time Database")
    tab1, tab2, tab3 = st.tabs(["Koordinat & ETA Bus Live", "Kepadatan Halte & Prediksi AI", "Log Transaksi Penumpang Real-Time (Tap-In)"])
    
    with tab1:
        st.dataframe(df_eta.sort_values(by="last_updated", ascending=False), use_container_width=True)
        
    with tab2:
        st.dataframe(df_density.sort_values(by="last_updated", ascending=False), use_container_width=True)
        
    with tab3:
        st.dataframe(df_raw_tx, use_container_width=True)

# Auto refresh logic using st.rerun()
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
