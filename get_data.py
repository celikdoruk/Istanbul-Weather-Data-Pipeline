import requests
import pandas as pd
from database import PostgresDatabase
import datetime
import creds

def get_data_last_10_days():
    # API endpoint
    url = "https://archive-api.open-meteo.com/v1/archive"

    todays_date = datetime.date.today()
    ten_days_ago = todays_date - datetime.timedelta(days=10)

    todays_date = todays_date.strftime("%Y-%m-%d")
    ten_days_ago = ten_days_ago.strftime("%Y-%m-%d")

    # Query parameters
    params = {
        "latitude": 41.01,       # Istanbul latitude
        "longitude": 28.97,      # Istanbul longitude
        "start_date": f"{ten_days_ago}",
        "end_date": f"{todays_date}",
        "daily": ["temperature_2m_max", "temperature_2m_min", "apparent_temperature_max", "apparent_temperature_min",
                "sunrise", "sunset", "daylight_duration", "sunshine_duration", "uv_index_max", "rain_sum", "showers_sum",
                "snowfall_sum", "precipitation_sum", "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant"],
        "timezone": "Europe/Istanbul"}

    # Send request
    response = requests.get(url, params=params)
    data = response.json()

    df = pd.DataFrame(data["daily"])
    df["time"] = pd.to_datetime(df["time"])
    df["sunrise"] = pd.to_datetime(df["sunrise"]).dt.time
    df["sunset"] = pd.to_datetime(df["sunset"]).dt.time
    df = df.where(pd.notnull(df), None)

    return df

def write_to_sql(df: pd.DataFrame, db: PostgresDatabase):
    try:
        db.connect()
        create_query = """
                CREATE TABLE IF NOT EXISTS weather (
                    time DATE PRIMARY KEY,
                    temperature_2m_max REAL,
                    temperature_2m_min REAL,
                    apparent_temperature_max REAL,
                    apparent_temperature_min REAL,
                    sunrise TIME,
                    sunset TIME,
                    daylight_duration REAL,
                    sunshine_duration REAL,
                    uv_index_max REAL,
                    rain_sum REAL,
                    showers_sum REAL,
                    snowfall_sum REAL,
                    precipitation_sum REAL,
                    wind_speed_10m_max REAL,
                    wind_gusts_10m_max REAL,
                    wind_direction_10m_dominant REAL);"""
        db.execute(create_query)
        db.commit()

        insert_query = """
                INSERT INTO weather (
                time, temperature_2m_max, temperature_2m_min, apparent_temperature_max, apparent_temperature_min,
                sunrise, sunset, daylight_duration, sunshine_duration, uv_index_max, rain_sum, showers_sum,
                snowfall_sum, precipitation_sum, wind_speed_10m_max, wind_gusts_10m_max, wind_direction_10m_dominant)
                VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time) DO NOTHING;"""
        
        for _, row in df.iterrows():
            db.execute(query=insert_query, params=tuple(row))
        db.commit()
        print(f"A total of {df.shape[0]} rows have been appended to the weather table.")
        
    except Exception as error:
        print(error)
    
    finally:
        db.close()
 

def main():
    df = get_data_last_10_days()
    db = PostgresDatabase(host=creds.host, database=creds.database, user=creds.user, password=creds.password)
    write_to_sql(df=df, db=db)


if __name__ == "__main__":
    main()