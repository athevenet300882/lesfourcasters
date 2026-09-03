"""
Extract ERA5 data from Open-Meteo API and load into BigQuery.
Production version - Batch 20 communes per API call
"""

import requests
import json
import hashlib
import time
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

# ============================================
# LOAD ENV VARIABLES
# ============================================
load_dotenv()

GCP_PROJECT = os.getenv("GCP_PROJECT")
GCP_KEY_PATH = os.getenv("GCP_KEY_PATH")
OPEN_METEO_URL = os.getenv("OPEN_METEO_BASE_URL", "https://archive-api.open-meteo.com/v1/archive")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 4))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 20))

credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
client = bigquery.Client(credentials=credentials)

RAW_TABLE = f"{GCP_PROJECT}.lesfourcasters_raw.raw_open_meteo"
COMMUNES_TABLE = f"{GCP_PROJECT}.lesfourcasters_raw.raw_communes_referentiel"

# ============================================
# COMPUTE HASH
# ============================================

def compute_hash(row):
    """Hash on date + commune (business key only)"""
    date_str = row["time"].split("T")[0]
    data = json.dumps({"date": date_str, "nom_poi": row["nom_poi"]}, sort_keys=True)
    return hashlib.md5(data.encode()).hexdigest()

# ============================================
# FETCH BATCH
# ============================================

def fetch_batch(communes, start_date, end_date):
    """Fetch 20 communes at once"""
    for attempt in range(MAX_RETRIES):
        try:
            params = {
                "latitude": ",".join(str(c["latitude"]) for c in communes),
                "longitude": ",".join(str(c["longitude"]) for c in communes),
                "start_date": start_date,
                "end_date": end_date,
                "hourly": [
                    "temperature_2m",
                    "relative_humidity_2m",
                    "precipitation",
                    "wind_speed_10m",
                    "pressure_msl",
                    "sunshine_duration"
                ]
            }
            
            r = requests.get(OPEN_METEO_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            
            # Debug: print response structure
            if attempt == 0:
                print(f"   Response type: {type(data)}")
                if isinstance(data, dict):
                    print(f"   Response keys: {list(data.keys())[:5]}")
            
            return data
        
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt
                print(f"⚠️  Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

# ============================================
# BUILD ROWS
# ============================================

def build_rows(data, communes, start_date, end_date):
    """Build rows from API response"""
    rows = []
    
    # Handle response format
    print(f"   Parsing response (type: {type(data).__name__})")
    
    if isinstance(data, list):
        print(f"   → Response is list with {len(data)} items")
        results = data
    elif isinstance(data, dict):
        if "results" in data:
            print(f"   → Response has 'results' key with {len(data.get('results', []))} items")
            results = data["results"]
        elif "hourly" in data:
            print(f"   → Response has 'hourly' key (single location)")
            results = [data]
        else:
            print(f"   → Unexpected dict structure: {list(data.keys())[:5]}")
            return rows
    else:
        print(f"   → Unexpected type: {type(data)}")
        return rows
    
    if not results:
        print("   ⚠️  No results")
        return rows
    
    print(f"   Processing {len(results)} results for {len(communes)} communes")
    
    # Process each commune
    for comm_idx, commune in enumerate(communes):
        if comm_idx >= len(results):
            print(f"   ⚠️  Missing result for {commune['code_insee']}")
            continue
        
        result = results[comm_idx]
        
        # Handle if result is a list
        if isinstance(result, list):
            print(f"   ⚠️  Result {comm_idx} is a list, skipping")
            continue
        
        hourly = result.get("hourly", {})
        
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        humid = hourly.get("relative_humidity_2m", [])
        precip = hourly.get("precipitation", [])
        wind = hourly.get("wind_speed_10m", [])
        press = hourly.get("pressure_msl", [])
        sun = hourly.get("sunshine_duration", [])
        
        # Build daily rows
        curr = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        while curr <= end_dt:
            for j, t in enumerate(times):
                if t.startswith(str(curr)):
                    row = {
                        "time": f"{curr}T00:00:00Z",
                        "nom_poi": commune["code_insee"],
                        "temperature_2m_mean": temps[j] if j < len(temps) else None,
                        "relative_humidity_2m_mean": humid[j] if j < len(humid) else None,
                        "precipitation_sum": precip[j] if j < len(precip) else None,
                        "wind_speed_10m_mean": wind[j] if j < len(wind) else None,
                        "pressure_msl_mean": press[j] if j < len(press) else None,
                        "sunshine_duration": sun[j] if j < len(sun) else None,
                        "weather_code": 0,
                        "ville": commune.get("ville"),
                        "latitude_poi": commune["latitude"],
                        "longitude_poi": commune["longitude"],
                        "numero_departement": commune.get("numero_departement"),
                        "inserted_at": datetime.utcnow().isoformat()
                    }
                    rows.append(row)
                    break
            curr += timedelta(days=1)
    
    return rows

# ============================================
# GET COMMUNES
# ============================================

def get_communes():
    """Fetch communes from BigQuery"""
    q = f"""
    SELECT 
        `code INSEE` as code_insee,
        Commune as ville,
        Numero_Departement as numero_departement,
        Latitude as latitude,
        Longitude as longitude
    FROM `{COMMUNES_TABLE}`
    WHERE `code INSEE` IS NOT NULL
    """
    communes = {}
    for row in client.query(q):
        communes[row["code_insee"]] = {
            "code_insee": row["code_insee"],
            "ville": row["ville"],
            "numero_departement": row["numero_departement"],
            "latitude": row["latitude"],
            "longitude": row["longitude"]
        }
    print(f"✅ Loaded {len(communes)} communes")
    return communes

# ============================================
# GET HASHES
# ============================================

def get_hashes():
    """Fetch existing hashes"""
    q = f"""
    SELECT DISTINCT 
        MD5(CONCAT(CAST(DATE(time) as STRING), nom_poi)) as h
    FROM `{RAW_TABLE}`
    WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    return {row["h"] for row in client.query(q)}

# ============================================
# LOAD ROWS
# ============================================

def load_rows(rows):
    """Load rows to BigQuery"""
    if not rows:
        print("⚠️  No rows to load")
        return
    jc = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_APPEND")
    client.load_table_from_json(rows, RAW_TABLE, job_config=jc).result()
    print(f"✅ Loaded {len(rows)} rows")

# ============================================
# MAIN
# ============================================

def main():
    """Main pipeline"""
    print("=" * 60)
    print("🌍 ERA5 Pipeline")
    print("=" * 60)
    
    communes_list = list(get_communes().values())
    hashes = get_hashes()
    print(f"✅ Found {len(hashes)} existing hashes")
    
    end = (datetime.utcnow() - timedelta(days=5)).date()
    start = end - timedelta(days=10)
    print(f"📅 Fetching {start} to {end}")
    
    rows = []
    total = len(communes_list)
    
    for i in range(0, total, BATCH_SIZE):
        batch = communes_list[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"\n📦 Batch {batch_num}/{total_batches} ({len(batch)} communes)")
        
        try:
            data = fetch_batch(batch, str(start), str(end))
            batch_rows = build_rows(data, batch, str(start), str(end))
            
            new = 0
            for r in batch_rows:
                h = compute_hash(r)
                if h not in hashes:
                    r["hash"] = h
                    rows.append(r)
                    hashes.add(h)
                    new += 1
            
            print(f"   → {len(batch_rows)} total, {new} new")
        
        except Exception as e:
            print(f"❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
            raise SystemExit(f"Pipeline stopped: {e}")
    
    if rows:
        load_rows(rows)
    else:
        print("⚠️  No new rows")
    
    print("\n" + "=" * 60)
    print("✅ Complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()