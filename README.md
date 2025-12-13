# Vidya-Setu: National School Fragility Index

A Geospatial Intelligence Engine optimizing education access for 1.37 Million schools in India.

## 📖 Overview

Vidya-Setu (Bridge of Knowledge) is a data engineering and analytics project designed to identify systemic gaps in India's education infrastructure. Moving beyond simple enrollment statistics, this project introduces the School Fragility Index—a composite metric that uses geospatial algorithms to pinpoint "Education Deserts" (areas with no secondary schools) and "Zombie Schools" (resource wastage).

Using a dataset of 1.37 million schools, this pipeline processes raw dirty data into actionable intelligence, visualized through an interactive Streamlit Command Center and a Power BI Executive Dashboard.

## 🏗️ Architecture & Tech Stack

The project follows a Bronze --> Silver --> Gold Medallion Architecture.

    graph TD
        A[Raw Data (CSV)] -->|Ingestion| B(Bronze Layer)
        B -->|Sanitation & Type Casting| C{Silver Layer}
        C -->|BallTree Spatial Indexing| D[Gold Layer]
        D -->|Aggregation & Scoring| E(Fragility Index)
        E -->|Serving| F[Streamlit App]
        E -->|BI Reporting| G[Power BI Dashboard]

| Component | Tech Stack | Justification |
| --------- | ---------- | ------------- |
| ETL Engine | Python (Pandas + Numpy) | Initially architected with PySpark, but refactored to vectorized Pandas for efficient single-node processing of 1.3M rows. |
| Geospatial Logic | Scikit-Learn (BallTree) | Switched from Apache Sedona to BallTree algorithms to perform Nearest Neighbor searches in O(N log N) complexity without cluster overhead. |
| Visualization | Streamlit + PyDeck | Chosen for its ability to render high-density scatterplots (1M+ points) and geospatial heatmaps interactively. |
| Business Intel | Power BI + DAX | Used for executive reporting, utilizing the VertiPaq engine to handle large datasets and creating dynamic measures for Fragility Scores. |
| Version Control | Git & GitHub | Standard CI/CD practice with strict .gitignore policies for large dataset management. |

## ⚙️ The Data Pipeline Lifecycle

1. Ingestion (Bronze)

    - Source: UDISE+ Dataset (Government of India).

    - Volume: 1.37 Million Records.

    - Challenge: Raw CSVs contained mixed types, shifting columns (e.g., "Balasore" appearing as a State), and massive outliers.

2. Sanitation (Silver)

    We implemented a robust cleaning module clean_raw_data() that:

    - Fixes Schema Shifts: Detects and drops rows where State/District names are numeric or swapped.

    - Handles Outliers: Caps PTR (Pupil-Teacher Ratio) at 150 to remove data entry errors (e.g., 1000 students for 1 teacher).

    - Coordinates: Filters out null or zero-value latitudes/longitudes.

3. Enrichment (Gold) - The "Jaw-Dropping" Logic

    This is where we mathematically prove isolation.

    - Problem: Calculating the distance from every primary school to every high school is a Cartesian Product (N * M).

    - 1.1M (Primary) * 0.25M (Secondary) = 286 Billion Comparisons.

    Solution: We used a BallTree Spatial Index.

    - It organizes coordinates into a tree structure.

    - Allows identifying the nearest high school for every primary school in seconds.

    - Result: Processing time dropped from Hours (Brute Force) to <30 Seconds.

## 📊 Key Metrics & Definitions

The dashboard visualizes three critical custom metrics:

| Metric | Definition | Logic / Formula |
| ------ | ---------- | --------------- |
| 🚩 Education Desert | A Primary school critically isolated from higher education. | Distance to High School > 5 km |
| 🧟 Zombie School | A school draining resources: overstaffed but empty. | Total Teachers > 2 AND PTR < 10 |
| 📉 Fragility Score | A composite risk score (0-100) for intervention priority. | Weighted Sum: (Desert * 50) + (Zero_Teachers * 50) |

## ⚔️ Challenges & Engineering Decisions

1. The Spark vs. Pandas Pivot

    - Initial Plan: Use PySpark + Apache Sedona for distributed geospatial joins.

    - Roadblock: While powerful, Spark's JVM overhead on a local machine (8GB RAM) caused bottlenecks for a dataset of this size (1.3M rows is "medium" data, not "big" data). Databricks Free Edition also removed support for custom JARs (Sedona).

    - Resolution: We pivoted to Scikit-Learn's BallTree. This allowed us to keep the algorithmic efficiency (O(N log N)) while running purely in Python, reducing infrastructure complexity by 90%.

2. Data Quality Nightmares

    - Issue: The raw data contained districts named "134" and States named "Balasore" due to CSV parsing errors.

    - Resolution: Implemented a strict Sanitation Layer (src/etl_silver_gold_pandas.py) that uses regex to validate names and logic checks to enforce State != District.

3. Visualization Limits

    - Issue: Plotting 1.3 million points crashes most browser-based maps (like standard Folium).

    - Resolution: Used PyDeck (Deck.gl) in Streamlit, which uses WebGL to render millions of points smoothly using the GPU. In Power BI, we aggregated data to the District Level to create performant Heatmaps.

## 🚀 How to Run This Project

### Prerequisites

- Python 3.10+

- VS Code (Recommended)

### 1. Clone & Setup

    git clone [https://github.com/YOUR_USERNAME/VidyaSetu-Analytics.git](https://github.com/YOUR_USERNAME/VidyaSetu-Analytics.git)
    cd VidyaSetu-Analytics

    # Create Virtual Environment
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate

    # Install Dependencies
    pip install -r requirements.txt

### 2. Run the ETL Pipeline

Note: You must place the udise_plus.csv file in data/raw/ first.

    # This cleans the data and generates the Fragility Index
    python src/etl_silver_gold_pandas.py

### 3. Launch the Dashboard

    streamlit run app/main_dashboard.py

## 📂 Repository Structure

    VidyaSetu-Analytics/
    │
    ├── app/
    │   └── main_dashboard.py          # Streamlit Command Center Application
    │
    ├── data/
    │   ├── raw/                       # (Ignored) Place your udise_plus.csv here
    │   └── final/
    │       └── fragility_index.csv    # The Processed Gold Layer (Generated by ETL)
    │
    ├── src/
    │   ├── etl_silver_gold_pandas.py  # The Core Logic (Sanitation + BallTree)
    │   ├── etl_silver_gold_spark.py   # (Legacy) The Spark/Sedona implementation
    │   └── config.py                  # Spark Configuration Utilities
    │
    ├── .gitignore                     # Prevents large data upload
    ├── requirements.txt               # Project dependencies
    └── README.md                      # Project Documentation

Thanks.
