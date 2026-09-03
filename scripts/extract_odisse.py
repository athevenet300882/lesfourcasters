"""
Extract Odissé data from Santé Publique France API and load into BigQuery.
Production version v2.0 - Canicules + Épidémies hivernales
"""

import requests
import json
import time
from datetime import datetime
from typing import List, Dict
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
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 4))

credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
client = bigquery.Client(credentials=credentials)

ODISSE_API = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets"
RAW_DATASET = f"{GCP_PROJECT}.lesfourcasters_raw"

# ============================================
# FETCH DATASET
# ============================================

def fetch_dataset(dataset_id: str, limit: int = 10000) -> List[Dict]:
    """Fetch dataset from Odissé API with retries"""
    url = f"{ODISSE_API}/{dataset_id}/records"
    
    for attempt in range(MAX_RETRIES):
        try:
            print(f"📥 Fetching {dataset_id} (Attempt {attempt + 1}/{MAX_RETRIES})...")
            
            params = {"limit": limit}
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            records = [r.get("record", {}).get("fields", {}) for r in data.get("results", [])]
            
            print(f"   ✅ Retrieved {len(records)} records")
            return records
        
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = 2 ** attempt
                print(f"   ⚠️  Attempt {attempt + 1} failed. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"   ❌ ERROR: {e}")
                return []

# ============================================
# GET EXISTING RECORDS
# ============================================

def get_existing_records(table_name: str) -> set:
    """Fetch existing (code_dept, annee) pairs"""
    q = f"""
    SELECT DISTINCT 
        code_departement,
        annee
    FROM `{RAW_DATASET}.{table_name}`
    """
    
    existing = set()
    try:
        for row in client.query(q):
            existing.add((row["code_departement"], row["annee"]))
        print(f"   ✅ Found {len(existing)} existing records in {table_name}")
    except Exception as e:
        print(f"   ℹ️  Table {table_name} may not exist yet: {e}")
    
    return existing

# ============================================
# LOAD ROWS
# ============================================

def load_rows(table_name: str, rows: List[Dict]):
    """Load rows to BigQuery with autodetect"""
    if not rows:
        print(f"   ⚠️  No new rows to load")
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

# ============================================
# DATASET 1: JOURS DE CANICULE
# ============================================

def extract_canicule_jours():
    """Extract heat wave days by department"""
    print("\n" + "=" * 60)
    print("📊 DATASET 1: Jours de canicule par département")
    print("=" * 60)
    
    dataset_id = "canicules-nombres-de-jours-de-canicule-departement"
    table_name = "raw_odisse_canicule_jours"
    
    existing = get_existing_records(table_name)
    records = fetch_dataset(dataset_id)
    
    if not records:
        print("   No records retrieved")
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
    
    print(f"   → {len(records)} total, {len(new_rows)} new")
    load_rows(table_name, new_rows)

# ============================================
# DATASET 2: DÉCÈS ATTRIBUABLES À LA CHALEUR
# ============================================

def extract_deces_chaleur():
    """Extract deaths attributable to heat"""
    print("\n" + "=" * 60)
    print("📊 DATASET 2: Décès attribuables à la chaleur")
    print("=" * 60)
    
    dataset_id = "canicules-deces-attribuables-a-la-chaleur-pendant-lete-et-pendant-les-vagues-de-chaleur-france"
    table_name = "raw_odisse_deces_chaleur"
    
    existing = get_existing_records(table_name)
    records = fetch_dataset(dataset_id)
    
    if not records:
        print("   No records retrieved")
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
    
    print(f"   → {len(records)} total, {len(new_rows)} new")
    load_rows(table_name, new_rows)

# ============================================
# DATASET 3: ÉPIDÉMIES HIVERNALES
# ============================================

def extract_epidemies_hivernales():
    """Extract winter epidemics data"""
    print("\n" + "=" * 60)
    print("📊 DATASET 3: Épidémies hivernales")
    print("=" * 60)
    
    dataset_id = "epidemies-hivernales-activite-des-urgences-au-cours-des-epidemies-hivernales-france-metropolitaine"
    table_name = "raw_odisse_epidemies_hivernales"
    
    existing = get_existing_records(table_name)
    records = fetch_dataset(dataset_id)
    
    if not records:
        print("   No records retrieved")
        return
    
    new_rows = []
    for r in records:
        # Use region + saison as key
        region = r.get("region", "UNKNOWN")
        saison = r.get("saison", "UNKNOWN")
        key = (region, saison)
        
        if key not in existing:
            row = {
                "region": region,
                "saison": saison,
                "annee": r.get("annee"),
                "passage_urgences": r.get("passage_urgences"),
                "passage_urgences_4ans": r.get("passage_urgences_4ans"),
                "taux_passage_urgences": r.get("taux_passage_urgences")
            }
            new_rows.append(row)
            existing.add(key)
    
    print(f"   → {len(records)} total, {len(new_rows)} new")
    load_rows(table_name, new_rows)

# ============================================
# MAIN
# ============================================

def main():
    """Main pipeline"""
    print("\n" + "=" * 60)
    print("🌍 ODISSÉ EXTRACTION PIPELINE")
    print("=" * 60)
    
    try:
        extract_canicule_jours()
        extract_deces_chaleur()
        extract_epidemies_hivernales()
        
        print("\n" + "=" * 60)
        print("✅ ODISSÉ EXTRACTION COMPLETE!")
        print("=" * 60)
    
    except Exception as e:
        print(f"\n❌ PIPELINE ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise SystemExit(f"Pipeline stopped: {e}")

if __name__ == "__main__":
    main()