from config import get_spark_session
from pyspark.sql.functions import col, when, count
from pyspark.sql.types import DoubleType, IntegerType

def process_bronze_to_silver(input_path, output_path):
    spark = get_spark_session("VidyaSetu_Ingestion")

    print(f"📂 Reading Raw Data from: {input_path}")
    
    # 1. READ CSV (Bronze)
    # inferSchema=True is okay for 1.3M rows (takes ~1 min)
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    initial_count = df.count()
    print(f"📊 Total Raw Rows: {initial_count:,}")

    # 2. CLEANING (Silver Logic)
    # Standardize column names (optional, but good practice)
    # Ensure Lat/Long are Double Type
    df_clean = df.withColumn("latitude", col("latitude").cast(DoubleType())) \
                 .withColumn("longitude", col("longitude").cast(DoubleType())) \
                 .withColumn("total_teachers", col("total_teachers").cast(IntegerType()))

    # 3. FILTERING "GHOST" SCHOOLS
    # We cannot perform Spatial Analysis without Lat/Long.
    # We drop rows where Lat OR Long is Null OR Zero (0,0 is off the coast of Africa).
    df_valid_geo = df_clean.filter(
        (col("latitude").isNotNull()) & 
        (col("longitude").isNotNull()) & 
        (col("latitude") != 0) & 
        (col("longitude") != 0)
    )

    valid_count = df_valid_geo.count()
    dropped_count = initial_count - valid_count
    
    print(f"🧹 Dropped {dropped_count:,} rows with missing/invalid coordinates.")
    print(f"✅ Valid 'Geospatial' Schools: {valid_count:,}")

    # 4. WRITING TO PARQUET (Silver)
    # Mode 'overwrite' replaces the file if you run this script again
    print(f"💾 Saving to Parquet: {output_path}")
    df_valid_geo.write.mode("overwrite").parquet(output_path)
    
    print("🚀 ETL Job Complete!")

if __name__ == "__main__":
    # USER: Verify your CSV filename inside data/raw/
    INPUT_FILE = "data/raw/udise_plus.csv"  # <--- RENAME THIS IF YOUR CSV IS NAMED DIFFERENTLY
    OUTPUT_FOLDER = "data/processed/schools_silver"
    
    process_bronze_to_silver(INPUT_FILE, OUTPUT_FOLDER)