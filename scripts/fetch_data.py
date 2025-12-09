import requests
import pandas as pd
from datetime import datetime
from geopy.distance import geodesic
import time
import os

# -----------------------------
# CONFIG
# -----------------------------
WAQI_TOKEN = os.environ["WAQI_TOKEN"]
OUTPUT_DIR = "daily_updates"

LATAM_COUNTRIES = [
    "Mexico", "Belize", "Guatemala", "Honduras", "El Salvador",
    "Nicaragua", "Costa Rica", "Panama", "Cuba",
    "Colombia", "Venezuela", "Ecuador", "Peru", "Bolivia", "Brazil", 
    "Paraguay", "Uruguay", "Chile", "Argentina"
]

# Ensure output folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------
def categorize_aqi(aqi):
    try: 
        if aqi is None:
            return None
        if aqi <= 50: return "Good"
        if aqi <= 100: return "Moderate"
        if aqi <= 250: return "Poor"
        return "Hazardous"
    except:
        return None


def find_closest_industrial_area(lat, lon, radius_km=50):
    url = (
        "https://nominatim.openstreetmap.org/search?"
        f"q=industrial&format=json&limit=20&"
        f"viewbox={lon-radius_km/100},{lat-radius_km/100},"
        f"{lon+radius_km/100},{lat+radius_km/100}"
    )

    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}).json()
    except:
        return None

    if not resp:
        return None

    min_dist = float("inf")
    for item in resp:
        ind_lat = float(item["lat"])
        ind_lon = float(item["lon"])
        dist = geodesic((lat, lon), (ind_lat, ind_lon)).km
        if dist < min_dist:
            min_dist = dist

    return round(min_dist, 2)


def fetch_city_data(city, country):
    url = f"https://api.waqi.info/feed/{city}/?token={WAQI_TOKEN}"
    print(f"Fetching: {city}, {country}")

    try:
        resp = requests.get(url).json()
    except:
        return None

    if resp.get("status") != "ok":
        return None

    d = resp["data"]
    iaqi = d.get("iaqi", {})
    lat, lon = d.get("city", {}).get("geo", [None, None])

    return {
        "City": city,
        "Country": country,
        "Latitude": lat,
        "Longitude": lon,
        "Temperature": iaqi.get("t", {}).get("v"),
        "Humidity": iaqi.get("h", {}).get("v"),
        "PM2.5": iaqi.get("pm25", {}).get("v"),
        "PM10": iaqi.get("pm10", {}).get("v"),
        "NO2": iaqi.get("no2", {}).get("v"),
        "SO2": iaqi.get("so2", {}).get("v"),
        "CO": iaqi.get("co", {}).get("v"),
        "AQI": d.get("aqi"),
        "Air_Quality_Category": categorize_aqi(d.get("aqi")),
        "Timestamp": datetime.now().isoformat()
    }


# -----------------------------
# MAIN FUNCTION
# -----------------------------
def fetch_all_latam_stations():
    all_rows = []

    for country in LATAM_COUNTRIES:
        search_url = f"https://api.waqi.info/search/?token={WAQI_TOKEN}&keyword={country}"

        try:
            resp = requests.get(search_url).json()
        except:
            continue

        if resp.get("status") != "ok":
            continue

        for station in resp.get("data", []):
            city = station.get("station", {}).get("name")

            if city:
                row = fetch_city_data(city, country)
                if row:
                    all_rows.append(row)

                time.sleep(1)  # avoid rate limit

    df = pd.DataFrame(all_rows)

    # Save file with today's date
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{OUTPUT_DIR}/latam_air_quality_{today}.csv"

    df.to_csv(filename, index=False)
    print(f"Saved: {filename}")

    return df


# -----------------------------
# RUN
# -----------------------------
df = fetch_all_latam_stations()
print(df.head())
