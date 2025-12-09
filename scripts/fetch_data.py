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
        if aqi <= 50:
            return "Good"
        if aqi <= 100:
            return "Moderate"
        if aqi <= 250:
            return "Poor"
        return "Hazardous"
    except:
        return None


def get_population_density(lat, lon, radius_km=5):
    """
    Approximate population density using OpenStreetMap Overpass API.
    Returns people per km².
    """
    # Convert radius to degrees roughly (1 deg ~ 111 km)
    delta = radius_km / 111
    bbox = (lat - delta, lon - delta, lat + delta, lon + delta)  # min_lat, min_lon, max_lat, max_lon

    overpass_url = "http://overpass-api.de/api/interpreter"
    query = f"""
    [out:json][timeout:25];
    (
      node["place"~"city|town"]["population"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      way["place"~"city|town"]["population"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
      relation["place"~"city|town"]["population"]({bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]});
    );
    out center;
    """

    try:
        resp = requests.get(overpass_url, params={'data': query}, timeout=15).json()
        elements = resp.get("elements", [])
        if not elements:
            return None

        # Take the largest population in the area as approximation
        populations = []
        for el in elements:
            pop = el.get("tags", {}).get("population")
            if pop:
                try:
                    populations.append(int(pop.replace(',', '')))
                except:
                    continue

        if not populations:
            return None

        pop_max = max(populations)
        # Approximate area in km² (circle with radius_km)
        area_km2 = 3.14159 * radius_km**2
        density = pop_max / area_km2
        return round(density, 2)
    except:
        return None



def find_closest_industrial_area(lat, lon, radius_km=50):
    """
    Uses OpenStreetMap Nominatim search to find the closest industrial area within radius.
    Returns distance in km.
    """
    url = (
        "https://nominatim.openstreetmap.org/search?"
        f"q=industrial&format=json&limit=20&"
        f"viewbox={lon-radius_km/100},{lat-radius_km/100},"
        f"{lon+radius_km/100},{lat+radius_km/100}"
    )

    try:
        resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10).json()
    except:
        return None

    if not resp:
        return None

    min_dist = float("inf")
    for item in resp:
        try:
            ind_lat = float(item["lat"])
            ind_lon = float(item["lon"])
            dist = geodesic((lat, lon), (ind_lat, ind_lon)).km
            if dist < min_dist:
                min_dist = dist
        except:
            continue

    return round(min_dist, 2) if min_dist != float("inf") else None


def fetch_city_data(city, country):
    url = f"https://api.waqi.info/feed/{city}/?token={WAQI_TOKEN}"
    print(f"Fetching: {city}, {country}")

    try:
        resp = requests.get(url, timeout=10).json()
    except:
        return None

    if resp.get("status") != "ok":
        return None

    d = resp["data"]
    iaqi = d.get("iaqi", {})
    lat, lon = d.get("city", {}).get("geo", [None, None])

    # Pull extra fields if coordinates are available
    population_density = get_population_density(lat, lon) if lat and lon else None
    proximity_industry = find_closest_industrial_area(lat, lon) if lat and lon else None

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
        "Population_Density": population_density,
        "Proximity_to_Industrial_Areas": proximity_industry,
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
            resp = requests.get(search_url, timeout=10).json()
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
    filename = f"{OUTPUT_DIR}/daily_update_air_quality_{today}.csv"

    df.to_csv(filename, index=False)
    print(f"Saved: {filename}")

    return df


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    df = fetch_all_latam_stations()
    print(df.head())
