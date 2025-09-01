# 🌦️ Istanbul Weather Data Pipeline

This is a small ETL project that fetches historical weather data for Istanbul from the [Open-Meteo API](https://open-meteo.com/), processes it with **pandas**, and stores it in a **PostgreSQL** database.

## How it works
1. **Extract** – The script pulls the last 10 days of daily weather data (temperature, precipitation, sunrise/sunset, etc.) from the Open-Meteo API.  
2. **Transform** – The raw JSON is converted into a pandas DataFrame, timestamps are formatted, and missing values are handled.  
3. **Load** – A PostgreSQL table is created if it doesn’t exist, and new rows are inserted. Duplicate dates are ignored with `ON CONFLICT DO NOTHING`.

## Files
- `get_data.py` – main script that fetches and loads the data  
- `database.py` – a simple PostgresDatabase class for connecting and executing SQL commands  
- `creds.py` – stores local database credentials (ignored in git)  
- `.gitignore` – ensures sensitive files like creds.py are not uploaded  

## Usage
Run the pipeline with:
```bash
python get_data.py
