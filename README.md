# Latin America Air Quality - Machine Learning Pipeline

This project is an end-to-end data pipeline and machine learning system that fetches, cleans, processes, and models air quality data across Latin American countries.

It collects real-time air quality metrics, combines them with geospatial attributes (such as population density and proximity to industrial areas), and runs a predictive model pipeline to classify the **Air Quality Category** (Good, Moderate, Poor, or Hazardous).

---

## Project Structure

```text
├── scripts/
│   └── fetch_data.py               # WAQI API data collector & scraper script
├── daily_updates/                  # Target directory for daily scraped air quality data CSVs
├── air_quality_pipeline.ipynb      # Main Jupyter Notebook (data cleaning, feature engineering, ML modeling)
├── updated_pollution_dataset.csv   # Post-processed dataset for verification/analysis
├── pyproject.toml                  # Python package and dependency configurations (uv compatible)
├── requirements.txt                # Pip requirements file
└── README.md                       # This documentation file
```

---

## Installation & Setup

Ensure you have Python 3.12 or newer installed. You can set up the environment and install dependencies in one of two ways:

### Option A: Using `uv` (Recommended)
This project is configured for [uv](https://github.com/astral-sh/uv) to ensure fast and deterministic builds.

1. **Install dependencies and create virtual environment:**
   ```bash
   uv sync
   ```
2. **Activate the environment:**
   ```bash
   source .venv/bin/activate
   ```

### Option B: Using standard `pip`
If you prefer not to use `uv`, you can install dependencies via `pip`:

1. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
2. **Install requirements:**
   ```bash
   pip install -r requirements.txt
   ```

---

## Running the Project

The project workflow consists of two main parts: data scraping/collection, and the data pipeline/model training.

### Part 1: Scraping Daily Air Quality Data (Optional)
The script [fetch_data.py](file:///home/migzavila/Downloads/SemII/ML/big_data_final_project/scripts/fetch_data.py) queries the World Air Quality Index (WAQI) API for stations in Latin America, approximates local population density and proximity to industrial areas using OpenStreetMap API (Overpass/Nominatim), and dumps a CSV into `daily_updates/`.

1. **Get a WAQI Token:**
   Sign up for a free token at [WAQI API](https://aqicn.org/data-platform/token/).
   
2. **Export the token as an environment variable:**
   ```bash
   export WAQI_TOKEN="your_waqi_api_token_here"
   ```
   
3. **Execute the script:**
   ```bash
   python scripts/fetch_data.py
   ```
   *(Or `uv run scripts/fetch_data.py` if using `uv`)*

*Note: The project already contains historical daily update CSV files from December 2025 through June 2026, so you can skip this step and go straight to the ML pipeline.*

---

### Part 2: Running the Machine Learning Pipeline
The Jupyter Notebook [air_quality_pipeline.ipynb](file:///home/migzavila/Downloads/SemII/ML/big_data_final_project/air_quality_pipeline.ipynb) contains the core data logic. It:
1. Dynamically merges all daily CSVs inside `daily_updates/`.
2. Cleans raw data and handles missing values using KNN Imputation.
3. Performs Feature Engineering (temporal features, pollutant z-score indexes, ratio features, temperature/humidity interactions).
4. Trains and evaluates **8 Machine Learning models**:
   * Random Forest
   * Support Vector Machine (SVM)
   * K-Nearest Neighbors (KNN)
   * Decision Tree
   * Logistic Regression
   * Naive Bayes
   * AdaBoost
   * Bagging Classifier
5. Performs hyperparameter tuning (GridSearchCV) on the top-performing models and compares their final accuracy and Macro F1 scores.

#### To run the notebook:
1. **Launch Jupyter:**
   ```bash
   jupyter notebook
   ```
   *(Or `uv run jupyter notebook` if using `uv`)*
2. **Open the notebook:**
   Select [air_quality_pipeline.ipynb](file:///home/migzavila/Downloads/SemII/ML/big_data_final_project/air_quality_pipeline.ipynb) from the Jupyter interface and run all cells.
