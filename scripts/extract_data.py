"""
Combined Data Extraction Pipeline
- ERA5 weather data (Open-Meteo API)
- Odissé health data (Santé Publique France API)
Production version v1.1 - Fixed Odissé pagination (limit max 100)
"""

import requests
import json
import hashlib
import time
from datetime import datetime, timedelta
from typing import List, Dict
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import os

load_dotenv()

GCP_PROJECT = os.getenv("GCP_PROJECT")
GCP_KEY_PATH = os.getenv("GCP_KEY_PATH")
OPEN_METEO_URL = os.getenv("OPEN_METEO_BASE_URL", "https://archive-api.open-meteo.com/v1/archive")
ODISSE_API = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets"
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 4))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 10))

credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
client = bigquery.Client(credentials=credentials)

RAW_TABLE_ERA5 = f"{GCP_PROJECT}.lesfourcasters_raw.raw_open_meteo"
COMMUNES_TABLE = f"{GCP_PROJECT}.lesfourcasters_raw.raw_communes_referentiel"
RAW_DATASET = f"{GCP_PROJECT}.lesfourcasters_raw"

# ============================================
# HELPER FUNCTIONS
# ============================================

def fetch_with_retry(url: str, params: dict, timeout: int = 60) -> dict:
    """Fetch URL with exponential backoff retries"""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt
                print(f"   ⚠️  Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"   ❌ ERROR: {e}")
                return {}

def load_to_bigquery(table_name: str, rows: List[Dict]):
    """Load rows to BigQuery with autodetect"""
    if not rows:
        print(f"   ⚠️  No rows to load")
        return
    
    table_id = f"{RAW_DATASET}.{table_name}"
    
    try:
        jc = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND"
        )
        client.load_table_from_json(rows, table_id, job_config=jc).result()
        print(f"   ✅ Loaded {len(rows)} rows to {table_name}")
    except Exception as e:
        print(f"   ❌ Error loading to {table_name}: {e}")

def get_existing_records(table_name: str) -> set:
    """Fetch existing (code_dept, annee) pairs"""
    try:
        q = f"""
        SELECT DISTINCT 
            code_departement,
            annee
        FROM `{RAW_DATASET}.{table_name}`
        """
        existing = set()
        for row in client.query(q):
            existing.add((row["code_departement"], row["annee"]))
        print(f"   ✅ Found {len(existing)} existing records in {table_name}")
        return existing
    except Exception as e:
        print(f"   ℹ️  Table {table_name} may not exist yet")
        return set()

# ============================================
# ERA5 EXTRACTION
# ============================================

def fetch_batch_era5(communes, start_date, end_date):
    """Fetch batch of communes from ERA5 API"""
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
    return fetch_with_retry(OPEN_METEO_URL, params, timeout=60)

def build_era5_rows(data, communes, start_date, end_date):
    """Build rows from ERA5 response"""
    rows = []
    
    if isinstance(data, list):
        results = data
    elif isinstance(data, dict):
        if "results" in data:
            results = data["results"]
        elif "hourly" in data:
            results = [data]
        else:
            return rows
    else:
        return rows
    
    if not results:
        return rows
    
    for comm_idx, commune in enumerate(communes):
        if comm_idx >= len(results):
            continue
        
        result = results[comm_idx]
        
        if isinstance(result, list):
            continue
        
        hourly = result.get("hourly", {})
        
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        humid = hourly.get("relative_humidity_2m", [])
        precip = hourly.get("precipitation", [])
        wind = hourly.get("wind_speed_10m", [])
        press = hourly.get("pressure_msl", [])
        sun = hourly.get("sunshine_duration", [])
        
        curr = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        while curr <= end_dt:
            for j, t in enumerate(times):
                if t.startswith(str(curr)):
                    row = {
                        "time": f"{curr}T00:00:00Z",
                        "nom_poi": commune["code_insee"],
                        "temperature_2m_mean": float(temps[j]) if j < len(temps) and temps[j] is not None else None,
                        "relative_humidity_2m_mean": float(humid[j]) if j < len(humid) and humid[j] is not None else None,
                        "precipitation_sum": float(precip[j]) if j < len(precip) and precip[j] is not None else None,
                        "wind_speed_10m_mean": float(wind[j]) if j < len(wind) and wind[j] is not None else None,
                        "pressure_msl_mean": float(press[j]) if j < len(press) and press[j] is not None else None,
                        "sunshine_duration": float(sun[j]) if j < len(sun) and sun[j] is not None else None,
                        "weather_code": 0.0,
                        "ville": commune.get("ville"),
                        "latitude_poi": commune["latitude"],
                        "longitude_poi": commune["longitude"],
                        "numero_departement": commune.get("numero_departement")
                    }
                    rows.append(row)
                    break
            curr += timedelta(days=1)
    
    return rows

def get_communes_era5():
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
    print(f"   ✅ Loaded {len(communes)} communes")
    return communes

def get_existing_era5():
    """Fetch existing (date, nom_poi) pairs from raw table"""
    q = f"""
    SELECT DISTINCT 
        DATE(time) as date,
        nom_poi
    FROM `{RAW_TABLE_ERA5}`
    WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    """
    existing = set()
    try:
        for row in client.query(q):
            existing.add((str(row["date"]), row["nom_poi"]))
        print(f"   ✅ Found {len(existing)} existing ERA5 records")
    except Exception as e:
        print(f"   ℹ️  ERA5 table may not exist yet")
    
    return existing

def extract_era5():
    """Main ERA5 extraction pipeline"""
    print("\n" + "=" * 60)
    print("🌍 ERA5 WEATHER DATA")
    print("=" * 60)
    
    communes_list = list(get_communes_era5().values())
    existing = get_existing_era5()
    
    end = (datetime.utcnow() - timedelta(days=5)).date()
    start = end - timedelta(days=10)
    print(f"   📅 Fetching {start} to {end}")
    
    rows = []
    total = len(communes_list)
    
    for i in range(0, total, BATCH_SIZE):
        batch = communes_list[i:i+BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE
        
        print(f"\n   📦 Batch {batch_num}/{total_batches} ({len(batch)} communes)")
        
        try:
            data = fetch_batch_era5(batch, str(start), str(end))
            batch_rows = build_era5_rows(data, batch, str(start), str(end))
            
            new = 0
            for r in batch_rows:
                date_str = r["time"].split("T")[0]
                key = (date_str, r["nom_poi"])
                if key not in existing:
                    rows.append(r)
                    existing.add(key)
                    new += 1
            
            print(f"      → {len(batch_rows)} total, {new} new")
        
        except Exception as e:
            print(f"      ❌ ERROR: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    if rows:
        load_to_bigquery("raw_open_meteo", rows)
    else:
        print("   ⚠️  No new ERA5 rows")
    
    return True

# ============================================
# ODISSÉ EXTRACTION (avec pagination limit=100)
# ============================================

def fetch_odisse_dataset_all(dataset_id: str) -> List[Dict]:
    """Fetch ALL records from Odissé dataset using pagination (max 100/page)"""
    url = f"{ODISSE_API}/{dataset_id}/records"
    all_records = []
    offset = 0
    page_size = 100  # HARD LIMIT imposed by Opendatasoft API
    
    while True:
        params = {"limit": page_size, "offset": offset}
        data = fetch_with_retry(url, params, timeout=30)
        
        if not data:
            break
        
        results = data.get("results", [])
        
        if not results:
            break
        
        all_records.extend(results)
        print(f"      📄 Page offset={offset}: +{len(results)} records (total: {len(all_records)})")
        
        if len(results) < page_size:
            # Last page reached
            break
        
        offset += page_size
        
        # Safety: Opendatasoft caps offset+limit at 10000
        if offset >= 10000:
            print(f"      ⚠️  Reached API offset cap (10000)")
            break
    
    print(f"      ✅ Retrieved {len(all_records)} total records")
    return all_records

def extract_odisse_canicule_jours():
    """Extract heat wave days by department"""
    print("\n   📊 Dataset 1: Jours de canicule par département")
    
    dataset_id = "canicules-nombres-de-jours-de-canicule-departement"
    table_name = "raw_odisse_canicule_jours"
    
    existing = get_existing_records(table_name)
    records = fetch_odisse_dataset_all(dataset_id)
    
    if not records:
        print("      No records retrieved")
        return
    
    new_rows = []
    for r in records:
        key = (r.get("code_departement"), r.get("annee"))
        
        if key not in existing:
            row = {
                "code_departement": r.get("code_departement"),
                "departement_nom": r.get("nom_departement"),
                "annee": r.get("annee"),
                "nombre_jours": r.get("nombre_de_jours")
            }
            new_rows.append(row)
            existing.add(key)
    
    print(f"      → {len(records)} total, {len(new_rows)} new")
    load_to_bigquery(table_name, new_rows)

def extract_odisse_deces_chaleur():
    """Extract deaths attributable to heat"""
    print("\n   📊 Dataset 2: Décès attribuables à la chaleur")
    
    dataset_id = "canicules-deces-attribuables-a-la-chaleur-pendant-lete-et-pendant-les-vagues-de-chaleur-france"
    table_name = "raw_odisse_deces_chaleur"
    
    existing = get_existing_records(table_name)
    records = fetch_odisse_dataset_all(dataset_id)
    
    if not records:
        print("      No records retrieved")
        return
    
    new_rows = []
    for r in records:
        key = (r.get("code_departement"), r.get("annee"))
        
        if key not in existing:
            row = {
                "code_departement": r.get("code_departement"),
                "departement_nom": r.get("nom_departement"),
                "annee": r.get("annee"),
                "deces_attribuables": r.get("nombre_de_deces_attribuables"),
                "fraction_deces": r.get("fraction_de_deces_attribuables")
            }
            new_rows.append(row)
            existing.add(key)
    
    print(f"      → {len(records)} total, {len(new_rows)} new")
    load_to_bigquery(table_name, new_rows)

def extract_odisse_syndrome(dataset_id: str, table_name: str, pathologie: str):
    """Extract weekly urgences/SOS Médecins data for a given winter pathology
    (grippe, bronchiolite, gastro-entérite). Loads ALL raw fields returned by
    the API (field names vary per dataset and aren't guessed), letting
    BigQuery autodetect the schema. Dedup is done via a hash of the full
    record content."""
    print(f"\n   📊 {pathologie}: Passages urgences + SOS Médecins")
    
    try:
        q = f"""
        SELECT DISTINCT record_hash
        FROM `{RAW_DATASET}.{table_name}`
        """
        existing = set()
        for row in client.query(q):
            existing.add(row["record_hash"])
        print(f"   ✅ Found {len(existing)} existing records in {table_name}")
    except Exception:
        print(f"   ℹ️  Table {table_name} may not exist yet")
        existing = set()
    
    records = fetch_odisse_dataset_all(dataset_id)
    
    if not records:
        print("      No records retrieved")
        return
    
    new_rows = []
    for r in records:
        record_hash = hashlib.md5(json.dumps(r, sort_keys=True, default=str).encode()).hexdigest()
        
        if record_hash not in existing:
            row = dict(r)  # copy all raw fields as returned by the API
            row["pathologie"] = pathologie
            row["record_hash"] = record_hash
            new_rows.append(row)
            existing.add(record_hash)
    
    print(f"      → {len(records)} total, {len(new_rows)} new")
    if new_rows:
        print(f"      🔑 Champs disponibles: {sorted(new_rows[0].keys())}")
    load_to_bigquery(table_name, new_rows)


def extract_odisse_grippe():
    """Extract flu (grippe) surveillance data"""
    extract_odisse_syndrome(
        "grippe-passages-aux-urgences-et-actes-sos-medecins-france",
        "raw_odisse_grippe",
        "Grippe"
    )


def extract_odisse_bronchiolite():
    """Extract bronchiolitis surveillance data"""
    extract_odisse_syndrome(
        "bronchiolite-passages-aux-urgences-et-actes-sos-medecins-france",
        "raw_odisse_bronchiolite",
        "Bronchiolite"
    )


def extract_odisse_gastro():
    """Extract acute gastroenteritis surveillance data"""
    extract_odisse_syndrome(
        "gastro-enterite-aigue-passages-aux-urgences-et-actes-sos-medecins-france",
        "raw_odisse_gastro_enterite",
        "Gastro-entérite aiguë"
    )

def extract_odisse():
    """Main Odissé extraction pipeline"""
    print("\n" + "=" * 60)
    print("🏥 ODISSÉ HEALTH DATA")
    print("=" * 60)
    
    try:
        extract_odisse_canicule_jours()
        extract_odisse_deces_chaleur()
        extract_odisse_grippe()
        extract_odisse_bronchiolite()
        extract_odisse_gastro()
        return True
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============================================
# MAIN
# ============================================

def main():
    """Main pipeline"""
    print("\n" + "=" * 60)
    print("📊 DATA EXTRACTION PIPELINE")
    print("=" * 60)
    
    era5_ok = extract_era5()
    odisse_ok = extract_odisse()
    
    print("\n" + "=" * 60)
    if era5_ok and odisse_ok:
        print("✅ DATA EXTRACTION COMPLETE!")
    else:
        print("⚠️  DATA EXTRACTION COMPLETED WITH ERRORS")
    print("=" * 60)

if __name__ == "__main__":
    main()