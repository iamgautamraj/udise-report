VidyaSetu_Analytics/
│
├── data/                          # LOCAL DATA LAKE (Simulating S3)
│   ├── raw/                       # Place your udise.csv here
│   ├── processed/                 # Parquet files (Silver layer)
│   └── final/                     # Aggregated data for Dashboard (Gold layer)
│
├── src/                           # SOURCE CODE
│   ├── __init__.py
│   ├── config.py                  # Spark Session & Sedona Config (CRITICAL)
│   ├── etl_bronze_silver.py       # Cleaning & Typing script
│   ├── etl_silver_gold.py         # The Geospatial Logic (Sedona)
│   └── analysis_engine.py         # Anomaly detection & Fragility logic
│
├── notebooks/                     # JUPYTER NOTEBOOKS
│   └── exploration.ipynb          # For quick testing before writing scripts
│
├── app/                           # DASHBOARD
│   └── main_dashboard.py          # Streamlit App
│
├── .env                           # AWS Credentials (ACCESS_KEY, SECRET_KEY)
├── .gitignore                     # Ignore data/ and .env
├── README.md
└── requirements.txt               # Dependencies