import os
import sys

# ==========================================
# USER INPUT: PASTE YOUR JAVA PATH HERE
# Use 'r' before the string to handle backslashes
# Example: r"C:\Program Files\Java\jdk1.8.0_301"
JAVA_PATH = r"C:\Program Files\Java\jdk1.8.0_301" 
# ==========================================

def run_diagnostics():
    print("--- DIAGNOSTIC DOCTOR STARTED ---")
    
    # 1. CHECK JAVA
    if not os.path.exists(JAVA_PATH):
        print(f"❌ ERROR: Java path does not exist: {JAVA_PATH}")
        print("   Action: Go to C:\\Program Files\\Java, open the jdk folder, and copy the path again.")
        return
    else:
        print(f"✅ Java Found: {JAVA_PATH}")

    # 2. CHECK HADOOP (WINUTILS)
    current_dir = os.path.abspath(os.getcwd())
    hadoop_home = os.path.join(current_dir, 'hadoop')
    winutils_path = os.path.join(hadoop_home, 'bin', 'winutils.exe')
    
    if not os.path.exists(winutils_path):
        print(f"❌ ERROR: winutils.exe not found at: {winutils_path}")
        print("   Action: Check if 'VidyaSetu_Analytics/hadoop/bin/winutils.exe' exists.")
        return
    else:
        print(f"✅ Hadoop Winutils Found: {winutils_path}")

    # 3. SET ENVIRONMENT VARIABLES MANUALLY
    os.environ['JAVA_HOME'] = JAVA_PATH
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')
    
    print("\n--- TRYING TO IMPORT PYSPARK ---")
    try:
        from pyspark.sql import SparkSession
        print("✅ PySpark Import Successful")
        
        print("--- ATTEMPTING TO START SPARK SESSION ---")
        spark = SparkSession.builder \
            .master("local[1]") \
            .appName("Test") \
            .getOrCreate()
        
        print("✅ SUCCESS! Spark Session Started.")
        print("   (If you see this, the paths are 100% correct)")
        spark.stop()
        
    except Exception as e:
        print(f"❌ SPARK CRASHED: {e}")

if __name__ == "__main__":
    run_diagnostics()