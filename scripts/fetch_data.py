import requests
import pandas as pd
from datetime import datetime
from geopy.distance import geodesic
import time
import json
import os

# -----------------------------
# CONFIG
# -----------------------------
WAQI_TOKEN = os.environ["WAQI_TOKEN"]
CACHE_FILE = "waqi_cache.json"
LATAM_COUNTRIES = [
    "Mexico", "Belize", "Guatemala", "Honduras", "El Salvador",
    "Nicaragua", "Costa Rica", "Panama", "Cuba",
    "Colombia", "Venezuela", "Ecuador", "Peru", "Bolivia", "Brazil", 
    "Paraguay", "Uruguay", "Chile", "Argentina"
]

# Load cache
if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, "r") as f:
        waqi_cache = json.load(f)
else:
    waqi_cache = {}

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
def get_population_density(lat, lon):
    key = f"pop_{lat}_{lon}"
    if key in waqi_cache:
        return waqi_cache[key]

    url = f"https://api.worldpop.org/v1/services?dataset=pop&year=2020&lon={lon}&lat={lat}"
    try:
        resp = requests.get(url).json()
        density = resp["data"]["pop"]
    except:
        density = None

    waqi_cache[key] = density
    with open(CACHE_FILE, "w") as f:
        json.dump(waqi_cache, f, indent=2)

    return density

def find_closest_industrial_area(lat, lon, radius_km=50):
    key = f"industry_{lat}_{lon}"
    if key in waqi_cache:
        return waqi_cache[key]

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
        waqi_cache[key] = None
        return None

    min_dist = float("inf")
    for item in resp:
        ind_lat = float(item["lat"])
        ind_lon = float(item["lon"])
        dist = geodesic((lat, lon), (ind_lat, ind_lon)).km
        if dist < min_dist:
            min_dist = dist

    min_dist = round(min_dist, 2)
    waqi_cache[key] = min_dist
    with open(CACHE_FILE, "w") as f:
        json.dump(waqi_cache, f, indent=2)

    return min_dist

def fetch_city_data(city, country):
    key = f"{city},{country}"
    if key in waqi_cache:
        return waqi_cache[key]

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

    data = {
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
        # "Population_Density": get_population_density(lat, lon),
        # "Proximity_to_Industrial_Areas": find_closest_industrial_area(lat, lon),
        "Timestamp": datetime.now().isoformat()
    }

    waqi_cache[key] = data
    with open(CACHE_FILE, "w") as f:
        json.dump(waqi_cache, f, indent=2)

    return data

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
                time.sleep(1)  # avoid API rate limit

    df = pd.DataFrame(all_rows)
    df.to_csv("latam_air_quality_real.csv", index=False)
    print("Saved: latam_air_quality_real.csv")
    return df

# -----------------------------
# RUN
# -----------------------------
df = fetch_all_latam_stations()
print(df.head())
