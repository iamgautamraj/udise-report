import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.express as px

# 1. PAGE CONFIGURATION
st.set_page_config(
    page_title="Vidya-Setu: Education Intelligence",
    page_icon="🇮🇳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. LOAD DATA
@st.cache_data
def load_data():
    df = pd.read_parquet("data/final/fragility_index.parquet")
    df['school_id'] = df['school_id'].astype(str)
    return df

try:
    df = load_data()
except FileNotFoundError:
    st.error("❌ Data not found! Please run the ETL script first.")
    st.stop()

# 3. SIDEBAR - VISUAL SETTINGS
st.sidebar.title("🎨 Presentation Controls")

# Map Theme
map_theme = st.sidebar.radio("Map Theme", ["Dark Mode", "Light Mode"], index=0)
if map_theme == "Dark Mode":
    map_style = "mapbox://styles/mapbox/dark-v10"
    desert_color = [255, 50, 50, 200]    # Red
    zombie_color = [255, 215, 0, 200]    # Gold/Yellow
    normal_color = [0, 255, 255, 100]    # Cyan/White
else:
    map_style = "mapbox://styles/mapbox/streets-v11"
    desert_color = [200, 30, 0, 200]     # Dark Red
    zombie_color = [255, 165, 0, 200]    # Orange
    normal_color = [0, 100, 255, 100]    # Blue
# B. Layer Type
layer_type = st.sidebar.selectbox("Visualization Layer", ["Scatterplot (Individual)", "Heatmap (Density)"])

# 4. SIDEBAR - DATA FILTERS
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔍 Data Filters")

# State & District
states = ["All"] + sorted(df['state_name'].astype(str).unique().tolist())
selected_state = st.sidebar.selectbox("Filter State", states)

if selected_state != "All":
    districts = sorted(df[df['state_name'] == selected_state]['district_name'].unique().tolist())
else:
    districts = sorted(df['district_name'].unique().tolist())
    
selected_district = st.sidebar.selectbox("Filter District", ["All"] + districts)

categories = ["All"] + sorted(df['school_category'].astype(str).unique().tolist())
selected_category = st.sidebar.selectbox("Filter School Category", categories)
# Metrics
st.sidebar.markdown("### 🛠️ Risk Metrics")
show_deserts = st.sidebar.checkbox("🚩 Education Deserts (>5km)", value=True)
show_zombies = st.sidebar.checkbox("⚠️ Zombie Schools (Excess Teachers)", value=False)
ptr_range = st.sidebar.slider("PTR Range", 0, 100, (0, 100))

# 5. FILTERING ENGINE
filtered_df = df.copy()

if selected_state != "All":
    filtered_df = filtered_df[filtered_df['state_name'] == selected_state]
if selected_district != "All":
    filtered_df = filtered_df[filtered_df['district_name'] == selected_district]
if selected_category != "All":
    filtered_df = filtered_df[filtered_df['school_category'] == selected_category]

# Apply Sliders
filtered_df = filtered_df[(filtered_df['PTR'] >= ptr_range[0]) & (filtered_df['PTR'] <= ptr_range[1])]

# Logic for Deserts/Zombies
if show_deserts and not show_zombies:
    filtered_df = filtered_df[filtered_df['is_desert'] == True]
elif show_zombies and not show_deserts:
    filtered_df = filtered_df[filtered_df['is_zombie'] == True]
elif show_deserts and show_zombies:
    filtered_df = filtered_df[(filtered_df['is_desert'] == True) | (filtered_df['is_zombie'] == True)]

# 6. MAIN DASHBOARD
st.title("🇮🇳 Vidya-Setu: National School Fragility Index")

with st.expander("ℹ️ About the Metrics & Definitions (Click to Expand)"):
    st.markdown("""
    **1. 🚩 Education Desert (Access Risk):**
    * **Definition:** A Primary/Middle school located unusually far from the nearest High School.
    * **Formula:** `Distance to nearest High School > 5 km`
    * **Impact:** High risk of student dropouts after Grade 8 due to lack of access.

    **2. ⚠️ Zombie School (Efficiency Risk):**
    * **Definition:** A school that is overstaffed but has very low enrollment.
    * **Formula:** `Total Teachers > 2` AND `PTR (Pupil-Teacher Ratio) < 10`
    * **Impact:** Wastage of government resources (salaries) on empty classrooms.

    **3. 📊 Fragility Score:**
    * **Definition:** A composite score (0-100) ranking the urgency of intervention.
    * **Formula:** Weighted sum of Isolation Distance and Zero-Teacher instances.
    """)

st.markdown(f"**Targeting {len(filtered_df):,} Critical Schools** | {selected_state} / {selected_district}")

# Metrics/KPI Row
m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Schools", f"{len(filtered_df):,}", border=True)
m2.metric("Avg Isolation", f"{filtered_df['distance_km'].mean():.1f} km", border=True)
m3.metric("Avg PTR", f"{filtered_df['PTR'].mean():.1f}", border=True)
m4.metric("Fragility Score", f"{filtered_df['fragility_score'].mean():.0f}/100", border=True)

# 7. THE MAP (Fixed: Flat & Clear Boundaries)
st.subheader("📍 Geospatial Intelligence")

# 1. REMOVE PITCH (Make it flat)
# 2. Add 'Normal' schools to the visualization so the map isn't empty
#    (We sample Normal schools to keep the app fast)
normal_schools = df[
    (df['is_desert'] == False) & 
    (df['is_zombie'] == False) & 
    (df['district_name'] == selected_district if selected_district != "All" else True)
].sample(frac=0.1) # Show only 10% of normal schools to reduce clutter

# Combine Data
viz_data = pd.concat([
    filtered_df, # The Red/Yellow dots (All of them)
    normal_schools # The Blue dots (Sampled)
])

# Define Colors
def get_color_fast(row):
    if row['is_desert']: return [255, 50, 50, 200]   # RED (Desert)
    if row['is_zombie']: return [255, 215, 0, 200]   # YELLOW (Zombie)
    return [0, 150, 255, 100]                        # BLUE (Normal)

viz_data['R'] = 0
viz_data['G'] = 150
viz_data['B'] = 255
viz_data.loc[viz_data['is_desert'], ['R','G','B']] = [255, 50, 50]
viz_data.loc[viz_data['is_zombie'], ['R','G','B']] = [255, 215, 0]

# VIEW STATE (Flat pitch)
view_state = pdk.ViewState(
    latitude=viz_data['latitude'].mean() if not viz_data.empty else 20.59,
    longitude=viz_data['longitude'].mean() if not viz_data.empty else 78.96,
    zoom=9 if selected_district != "All" else 4,
    pitch=0, # <--- FLAT VIEW (No bending)
)

# LAYER
layer = pdk.Layer(
    "ScatterplotLayer",
    viz_data,
    get_position=['longitude', 'latitude'],
    get_fill_color=['R', 'G', 'B', 180],
    get_radius=200 if selected_district != "All" else 800,
    pickable=True,
)

# RENDER (Force Light Style for Boundaries)
st.pydeck_chart(pdk.Deck(
    map_style="mapbox://styles/mapbox/streets-v11", # Explicit Streets Style
    initial_view_state=view_state,
    layers=[layer],
    tooltip={"text": "{school_name}\nSchool Category: {school_category}\nDistance: {distance_km}km"}
))

# 8. CHARTS & DATA
st.subheader("📊 Analytics Deep Dive")
c1, c2 = st.columns(2)

with c1:
    fig_dist = px.histogram(filtered_df, x="distance_km", nbins=50, 
                            title="Isolation Distribution (Distance to High School)",
                            color_discrete_sequence=['#FF4B4B'],
                            labels={"distance_km": "Distance (km)"})
    st.plotly_chart(fig_dist, use_container_width=True)

with c2:
    fig_ptr = px.histogram(filtered_df, x="PTR", nbins=50, 
                           title="PTR Distribution (Teacher Efficiency)",
                           color_discrete_sequence=['#FFA500'],
                           labels={"PTR": "Pupil-Teacher Ratio"})
    fig_ptr.add_vline(x=30, line_dash="dash", line_color="green", annotation_text="Standard (30:1)")
    st.plotly_chart(fig_ptr, use_container_width=True)

# 9. DATA TABLE (Full Width)
st.subheader("📋 Priority Intervention List")
st.dataframe(
    filtered_df[['school_name', 'state_name', 'district_name', 'school_category', 'distance_km', 'PTR', 'total_students', 'total_teachers', 'fragility_score']]
    .sort_values(by='fragility_score', ascending=False)
    .head(100)
    .style.format({
        "distance_km": "{:.2f}", 
        "PTR": "{:.1f}", 
        "fragility_score": "{:.0f}"
    })
    .background_gradient(subset=['fragility_score'], cmap="Reds"),
    use_container_width=True
)