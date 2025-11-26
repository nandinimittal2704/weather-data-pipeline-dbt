# src/extract.py
import requests, time, json, os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env when present
load_dotenv()

# Weather API key should be provided via environment variable.
# For backward compatibility, a default key is kept but it's recommended
# to set `WEATHER_API_KEY` in `.env` or your environment.
DEFAULT_WEATHER_API_KEY = "WEATHER_API_KEY"
API_KEY = os.getenv("WEATHER_API_KEY", DEFAULT_WEATHER_API_KEY)
if API_KEY == DEFAULT_WEATHER_API_KEY:
    print("Warning: Using default Weather API key. Set WEATHER_API_KEY in .env for production.")

CONFIG = {
    "api_key": API_KEY, 
    "cities": ["Delhi", "Mumbai", "Bangalore", "Chennai", "Kolkata"]
}

OUT_PATH = Path("data/sample_weather.json")

def fetch_weather(city, api_key):
    url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

def main():
    rows = []
    for city in CONFIG["cities"]:
        try:
            data = fetch_weather(city, CONFIG["api_key"])
            row = {
                "city": city,
                "timestamp": data["location"]["localtime_epoch"],
                "temp_c": data["current"]["temp_c"],
                "humidity": data["current"]["humidity"],
                "weather_main": data["current"]["condition"]["text"],
                "raw": data
            }
            rows.append(row)
        except Exception as e:
            print(f"Error for {city}: {e}")
        time.sleep(1)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, 'w') as f:
        json.dump(rows, f, indent=2)

    df = pd.DataFrame(rows)
    print("Extracted rows:", len(df))
    return df

if __name__ == "__main__":
    main()
