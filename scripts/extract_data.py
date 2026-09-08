"""
Combined Data Extraction Pipeline (REFACTORED)
- ERA5 weather data (Open-Meteo API)
- Odissé health data (Santé Publique France API)
Production version v1.3 - Factorized for maintainability
"""

import requests
import json
import hashlib
import time
from datetime import datetime, timedelta
from typing import List, Dict, Callable, Optional, Tuple
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
# CORE HELPER FUNCTIONS
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

def load_to_bigquery(table_name: str, rows: List[Dict]) -> bool:
    """Load rows to BigQuery with autodetect"""
    if not rows:
        print(f"   ⚠️  No rows to load")
        return False
    
    table_id = f"{RAW_DATASET}.{table_name}"
    try:
        jc = bigquery.LoadJobConfig(autodetect=True, write_disposition="WRITE_APPEND")
        client.load_table_from_json(rows, table_id, job_config=jc).result()
        print(f"   ✅ Loaded {len(rows)} rows to {table_name}")
        return True
    except Exception as e:
        print(f"   ❌ Error loading to {table_name}: {e}")
        return False

def get_existing_records(table_name: str, key_fields: List[str] = None) -> set:
    """Fetch existing records (generic for any key_fields)"""
    if key_fields is None:
        key_fields = ["code_departement", "annee"]
    
    fields_select = ", ".join(key_fields)
    try:
        q = f"SELECT DISTINCT {fields_select} FROM `{RAW_DATASET}.{table_name}`"
        existing = set()
        for row in client.query(q):
            key = tuple(row[field] for field in key_fields)
            existing.add(key)
        print(f"   ✅ Found {len(existing)} existing records in {table_name}")
        return existing
    except Exception:
        print(f"   ℹ️  Table {table_name} may not exist yet")
        return set()

def print_section(title: str, emoji: str = "📊"):
    """Print a formatted section header"""
    print(f"\n   {emoji} {title}")

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
            "temperature_2m", "relative_humidity_2m", "precipitation",
            "wind_speed_10m", "pressure_msl", "sunshine_duration"
        ]
    }
    return fetch_with_retry(OPEN_METEO_URL, params, timeout=60)

def build_era5_rows(data, communes, start_date, end_date):
    """Build rows from ERA5 response"""
    rows = []
    results = data.get("results", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
    
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
        
        curr = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        while curr <= end_dt:
            for j, t in enumerate(times):
                if t.startswith(str(curr)):
                    row = {
                        "time": f"{curr}T00:00:00Z",
                        "nom_poi": commune["code_insee"],
                        "temperature_2m_mean": float(hourly.get("temperature_2m", [])[j]) if j < len(hourly.get("temperature_2m", [])) else None,
                        "relative_humidity_2m_mean": float(hourly.get("relative_humidity_2m", [])[j]) if j < len(hourly.get("relative_humidity_2m", [])) else None,
                        "precipitation_sum": float(hourly.get("precipitation", [])[j]) if j < len(hourly.get("precipitation", [])) else None,
                        "wind_speed_10m_mean": float(hourly.get("wind_speed_10m", [])[j]) if j < len(hourly.get("wind_speed_10m", [])) else None,
                        "pressure_msl_mean": float(hourly.get("pressure_msl", [])[j]) if j < len(hourly.get("pressure_msl", [])) else None,
                        "sunshine_duration": float(hourly.get("sunshine_duration", [])[j]) if j < len(hourly.get("sunshine_duration", [])) else None,
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
    SELECT `code INSEE` as code_insee, Commune as ville, Numero_Departement as numero_departement, Latitude as latitude, Longitude as longitude
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
    """Fetch existing ERA5 records"""
    q = f"SELECT DISTINCT DATE(time) as date, nom_poi FROM `{RAW_TABLE_ERA5}` WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)"
    existing = set()
    try:
        for row in client.query(q):
            existing.add((str(row["date"]), row["nom_poi"]))
        print(f"   ✅ Found {len(existing)} existing ERA5 records")
    except Exception:
        print(f"   ℹ️  ERA5 table may not exist yet")
    
    return existing

def extract_era5():
    """Main ERA5 extraction pipeline"""
    print("\n" + "=" * 60)
    print("🌍 ERA5 WEATHER DATA")
    print("=" * 60)
    
    try:
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
        
        if rows:
            load_to_bigquery("raw_open_meteo", rows)
        else:
            print("   ⚠️  No new ERA5 rows")
        
        return True
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False

# ============================================
# ODISSÉ EXTRACTION (FACTORIZED)
# ============================================

def fetch_odisse_dataset_all(dataset_id: str) -> List[Dict]:
    """Fetch ALL records from Odissé dataset using pagination (max 100/page)"""
    url = f"{ODISSE_API}/{dataset_id}/records"
    all_records = []
    offset = 0
    page_size = 100
    
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
        
        if len(results) < page_size or offset >= 10000:
            break
        
        offset += page_size
    
    print(f"      ✅ Retrieved {len(all_records)} total records")
    return all_records

def extract_odisse_dataset(
    dataset_id: str,
    table_name: str,
    dataset_name: str,
    row_mapper: Optional[Callable[[Dict], Dict]] = None,
    dedup_key: Optional[Tuple[str, str]] = None
):
    """
    GENERIC function to extract any Odissé dataset
    
    Args:
        dataset_id: ID de l'API Odissé
        table_name: Nom de la table BigQuery
        dataset_name: Nom affiché (pour les logs)
        row_mapper: Fonction optionnelle pour transformer chaque row
        dedup_key: Tuple (field1, field2) pour dédupe, ou None pour hash-based
    """
    print_section(dataset_name)
    
    try:
        existing = set()
        if dedup_key:
            existing = get_existing_records(table_name, list(dedup_key))
        
        records = fetch_odisse_dataset_all(dataset_id)
        
        if not records:
            print("      No records retrieved")
            return
        
        new_rows = []
        for r in records:
            # Appliquer le mapper si fourni
            row = row_mapper(r) if row_mapper else dict(r)
            
            # Gérer la déduplication
            if dedup_key:
                key = (r.get(dedup_key[0]), r.get(dedup_key[1]))
                if key not in existing:
                    new_rows.append(row)
                    existing.add(key)
            else:
                # Hash-based dedup (pour syndromes)
                record_hash = hashlib.md5(json.dumps(r, sort_keys=True, default=str).encode()).hexdigest()
                if record_hash not in existing:
                    row["record_hash"] = record_hash
                    new_rows.append(row)
                    existing.add(record_hash)
        
        print(f"      → {len(records)} total, {len(new_rows)} new")
        if new_rows and "record_hash" not in new_rows[0]:
            print(f"      🔑 Champs: {sorted(new_rows[0].keys())}")
        
        load_to_bigquery(table_name, new_rows)
    
    except Exception as e:
        print(f"      ❌ ERROR: {e}")

# ===== ROW MAPPERS (transformation des données) =====

def map_canicule_jours(r: Dict) -> Dict:
    """Transform Canicule Jours raw data"""
    return {
        "code_departement": r.get("dep"),
        "departement_nom": r.get("libgeo"),
        "region_code": r.get("reg"),
        "region_nom": r.get("reglib"),
        "annee": r.get("annee"),
        "nombre_jours": r.get("nb_j_can")
    }

def map_syndrome(pathologie_name: str):
    """Factory pour créer un mapper de syndrome"""
    def mapper(r: Dict) -> Dict:
        row = dict(r)
        row["pathologie"] = pathologie_name
        return row
    return mapper

# ===== EXTRACTION FUNCTIONS =====

def extract_odisse():
    """Main Odissé extraction pipeline - FACTORIZED"""
    print("\n" + "=" * 60)
    print("🏥 ODISSÉ HEALTH DATA")
    print("=" * 60)
    
    try:
        # Canicule Jours
        extract_odisse_dataset(
            "canicules-nombres-de-jours-de-canicule-departement",
            "raw_odisse_canicule_jours",
            "Dataset 1: Jours de canicule par département",
            row_mapper=map_canicule_jours,
            dedup_key=("dep", "annee")
        )
        
        # Décès Chaleur
        extract_odisse_dataset(
            "canicules-deces-attribuables-a-la-chaleur-pendant-lete-et-pendant-les-vagues-de-chaleur-france",
            "raw_odisse_deces_chaleur",
            "Dataset 2: Décès attribuables à la chaleur"
        )
        
        # Syndromes (Grippe, Bronchiolite, Gastro)
        syndromes = [
            ("grippe-passages-aux-urgences-et-actes-sos-medecins-france", "raw_odisse_grippe", "Grippe"),
            ("bronchiolite-passages-aux-urgences-et-actes-sos-medecins-france", "raw_odisse_bronchiolite", "Bronchiolite"),
            ("gastro-enterite-aigue-passages-aux-urgences-et-actes-sos-medecins-france", "raw_odisse_gastro_enterite", "Gastro-entérite aiguë"),
        ]
        
        for dataset_id, table_name, pathologie in syndromes:
            extract_odisse_dataset(
                dataset_id,
                table_name,
                f"Dataset: {pathologie}",
                row_mapper=map_syndrome(pathologie)
            )
        
        return True
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        return False

# ============================================
# MAIN
# ============================================

def main():
    """Main pipeline"""
    print("\n" + "=" * 60)
    print("📊 DATA EXTRACTION PIPELINE (REFACTORED)")
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