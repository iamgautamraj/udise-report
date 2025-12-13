import pandas as pd
import numpy as np
from sklearn.neighbors import BallTree
import os

def process_gold_layer_optimized():
    print("🚀 Starting Optimized Geospatial Engine (Pandas + Sklearn)...")
    
    input_file = "data/raw/udise_plus.csv"
    if not os.path.exists(input_file):
        print(f"❌ Error: File not found at {input_file}")
        return

    print("📂 Loading Data...")
    # ADDED 'state_name' HERE
    use_cols = [
        'udise_school_code', 'school_name', 'state_name', 'district_name', 'latitude', 'longitude', 
        'class_to', 'school_category', 'total_teachers', 
        'class_students'
    ]
    
    try:
        df = pd.read_csv(input_file, usecols=lambda c: c in use_cols)
    except ValueError:
        print("⚠️ Warning: Column name mismatch. Reading all columns...")
        df = pd.read_csv(input_file)

    df.rename(columns={'udise_school_code': 'school_id', 'class_students': 'total_students'}, inplace=True)
    print(f"📊 Rows loaded: {len(df):,}")

    # CLEAN & PREPARE
    print("🛠️ processing Coordinates...")
    df = df.dropna(subset=['latitude', 'longitude'])
    df = df[(df['latitude'] != 0) & (df['longitude'] != 0)]
    
    df['lat_rad'] = np.radians(df['latitude'])
    df['lon_rad'] = np.radians(df['longitude'])

    # LOGIC
    df['is_high_school'] = np.where(
        (df['class_to'] >= 9) | (df['school_category'].astype(str).str.contains('Secondary', case=False, na=False)), 
        1, 0
    )

    df_primary = df[df['is_high_school'] == 0].copy()
    df_secondary = df[df['is_high_school'] == 1].copy()
    
    print("⏳ Building Spatial Index (BallTree)...")
    tree = BallTree(df_secondary[['lat_rad', 'lon_rad']].values, metric='haversine')
    
    print("🚀 Querying Nearest Neighbors...")
    distances, indices = tree.query(df_primary[['lat_rad', 'lon_rad']].values, k=1)
    df_primary['distance_km'] = distances * 6371
    
    # FRAGILITY METRICS
    print("📈 Calculating Fragility Index...")
    df_primary['PTR'] = np.where(df_primary['total_teachers'] > 0, 
                                 df_primary['total_students'] / df_primary['total_teachers'], 0)
    
    df_primary['is_desert'] = np.where(df_primary['distance_km'] > 5.0, True, False)
    df_primary['is_zombie'] = np.where((df_primary['PTR'] < 10) & (df_primary['total_teachers'] > 2), True, False)
    
    df_primary['fragility_score'] = (
        np.where(df_primary['distance_km'] > 5, 50, 0) + 
        np.where(df_primary['total_teachers'] == 0, 50, 0)
    )

    # SAVE
    output_dir = "data/final"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/fragility_index.csv"
    
    print(f"💾 Saving Gold Layer to: {output_path}")
    
    # ADDED 'state_name' and 'PTR' TO FINAL OUTPUT
    final_cols = ['school_id', 'school_name', 'state_name', 'district_name', 'total_students', 
                  'total_teachers', 'latitude', 'longitude', 'distance_km', 
                  'PTR', 'is_desert', 'is_zombie', 'fragility_score','school_category']
    
    df_primary[final_cols].to_csv(output_path, index=False)
    print("\n✅ SUCCESS! Analysis Complete.")

if __name__ == "__main__":
    process_gold_layer_optimized()