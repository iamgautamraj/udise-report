import os
import sys
from sedona.spark import * # <--- The Modern Import

# --- HARDCODED PATHS (Keep these) ---
JAVA_PATH = r"C:\Program Files\Java\jdk1.8.0_301"
HADOOP_HOME_PATH = r"D:\balsheet\VidyaSetu_Analytics\hadoop"

os.environ['JAVA_HOME'] = JAVA_PATH
os.environ['HADOOP_HOME'] = HADOOP_HOME_PATH
os.environ['PATH'] += os.pathsep + os.path.join(HADOOP_HOME_PATH, 'bin')
# ------------------------------------

def get_spark_session(app_name="VidyaSetu_ETL"):
    """
    Initializes Spark using SedonaContext (Modern Way).
    This automatically registers ST_KNN and the Optimizers.
    """
    sedona_version = "1.5.1"
    geotools_wrapper_version = "1.5.1-28.2"
    
    print(f"⚡ Starting Spark Session '{app_name}' (via SedonaContext)...")
    
    # 1. Configure the Builder
    config = SedonaContext.builder() \
        .appName(app_name) \
        .master("local[*]") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config('spark.jars.packages',
               f'org.apache.sedona:sedona-spark-shaded-3.5_2.12:{sedona_version},'
               f'org.datasyslab:geotools-wrapper:{geotools_wrapper_version}') \
        .config("spark.jars.repositories", "https://artifacts.unidata.ucar.edu/repository/unidata-all") \
        .getOrCreate()
    
    # 2. Create the Context (This registers ST_KNN and Extensions automatically)
    spark = SedonaContext.create(config)
    
    # Suppress Logs
    spark.sparkContext.setLogLevel("ERROR")
    
    print("✅ Spark Session Active (Optimizer Hooks Registered)")
    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    # Test ST_KNN (This would fail before)
    print("Testing SQL Optimizer...")
    try:
        # Create dummy points to test KNN syntax
        spark.sql("SELECT ST_Point(0.0, 0.0) as geom").createOrReplaceTempView("p")
        spark.sql("SELECT ST_Point(1.0, 1.0) as geom").createOrReplaceTempView("s")
        
        # If this runs, ST_KNN is fixed!
        spark.sql("SELECT * FROM p, s WHERE ST_KNN(p.geom, s.geom, 1)").show()
        print("🎉 ST_KNN is WORKING!")
    except Exception as e:
        print(f"❌ Error: {e}")