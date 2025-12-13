import pandas as pd

# Path to your RAW CSV
csv_path = "data/raw/udise_plus.csv"

print(f"🔍 Inspecting columns for: {csv_path}")

# Read ONLY the header (index_col=False ensures we don't mistake ID for index)
df = pd.read_csv(csv_path, nrows=0)

# Print them as a clean list
print(f"✅ Found {len(df.columns)} columns:")
print(list(df.columns))

# OPTIONAL: Check specific keywords
print("\n🔎 Searching for 'student' columns:")
for col in df.columns:
    if "student" in col.lower():
        print(f" -> {col}")