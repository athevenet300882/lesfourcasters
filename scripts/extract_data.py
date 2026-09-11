"""
Combined Data Extraction Pipeline
- ERA5 weather data (Open-Meteo API) - agregats journaliers
- Odisse health data (Sante Publique France API)
Production version v2.0
"""

import requests
import json
import hashlib
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Callable, Optional, Tuple, Any
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

# Fenetre de collecte ERA5 (jours)
ERA5_LAG_DAYS = int(os.getenv("ERA5_LAG_DAYS", 5))
ERA5_WINDOW_DAYS = int(os.getenv("ERA5_WINDOW_DAYS", 10))
ERA5_DEDUP_LOOKBACK_DAYS = int(os.getenv("ERA5_DEDUP_LOOKBACK_DAYS", 60))

credentials = service_account.Credentials.from_service_account_file(GCP_KEY_PATH)
client = bigquery.Client(credentials=credentials, project=GCP_PROJECT)

RAW_DATASET = f"{GCP_PROJECT}.lesfourcasters_raw"
RAW_TABLE_ERA5 = f"{RAW_DATASET}.raw_open_meteo"
COMMUNES_TABLE = f"{RAW_DATASET}.raw_communes_referentiel"

# Variables journalieres demandees a Open-Meteo
ERA5_DAILY_VARS = [
    "weather_code",
    "temperature_2m_mean",
    "temperature_2m_min",
    "temperature_2m_max",
    "apparent_temperature_mean",
    "apparent_temperature_max",
    "relative_humidity_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_mean",
    "wind_gusts_10m_max",
    "pressure_msl_mean",
    "sunshine_duration",
]


# ============================================
# HELPERS
# ============================================

def normalize_insee(value: Any) -> Optional[str]:
    """
    Normalise un code INSEE en 5 caracteres.
    - 1034 -> 01034
    - 2A004 -> 2A004 (Corse inchangee)
    - None / vide -> None
    Meme logique que stg_communes.sql (LPAD).
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s[:2].upper() in ("2A", "2B") and len(s) == 5:
        return s.upper()
    if s.isdigit():
        return s.zfill(5)
    return s


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
                print(f"   [!] Tentative {attempt + 1} echouee. Nouvel essai dans {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"   [X] ERREUR: {e}")
                return {}
    return {}


def load_to_bigquery(table_name: str, rows: List[Dict]) -> bool:
    """Load rows to BigQuery"""
    if not rows:
        print("   [!] Aucune ligne a charger")
        return False

    table_id = f"{RAW_DATASET}.{table_name}"
    try:
        jc = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition="WRITE_APPEND",
        )
        # Force le type des champs identifiants sur la table ERA5
        if table_name == "raw_open_meteo":
            jc.autodetect = False
            table = client.get_table(table_id)
            jc.schema = table.schema

        client.load_table_from_json(rows, table_id, job_config=jc).result()
        print(f"   [OK] {len(rows)} lignes chargees dans {table_name}")
        return True
    except Exception as e:
        print(f"   [X] Erreur de chargement vers {table_name}: {e}")
        time.sleep(1)
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
            existing.add(tuple(row[field] for field in key_fields))
        print(f"   [OK] {len(existing)} enregistrements existants dans {table_name}")
        return existing
    except Exception:
        print(f"   [i] La table {table_name} n'existe peut-etre pas encore")
        return set()


def print_section(title: str):
    print(f"\n   >> {title}")


# ============================================
# ERA5 EXTRACTION
# ============================================

def get_communes_era5() -> Dict[str, Dict]:
    """Fetch communes from BigQuery (codes INSEE normalises)"""
    q = f"""
    SELECT
        `code INSEE`        AS code_insee,
        Commune             AS ville,
        Numero_Departement  AS numero_departement,
        Latitude            AS latitude,
        Longitude           AS longitude
    FROM `{COMMUNES_TABLE}`
    WHERE `code INSEE` IS NOT NULL
      AND Latitude IS NOT NULL
      AND Longitude IS NOT NULL
    """
    communes = {}
    for row in client.query(q):
        code = normalize_insee(row["code_insee"])
        if not code:
            continue
        communes[code] = {
            "code_insee": code,
            "ville": row["ville"],
            "numero_departement": row["numero_departement"],
            "latitude": row["latitude"],
            "longitude": row["longitude"],
        }
    print(f"   [OK] {len(communes)} communes chargees")
    return communes


def get_existing_era5() -> set:
    """
    Cles (date, code_insee) deja presentes.
    nom_poi est normalise cote SQL pour couvrir les anciens chargements
    ou il contenait un code non pade ou un nom de ville.
    """
    q = f"""
    SELECT DISTINCT
        DATE(time) AS date,
        CASE
            WHEN REGEXP_CONTAINS(CAST(nom_poi AS STRING), r'^[0-9]+$')
                THEN LPAD(CAST(nom_poi AS STRING), 5, '0')
            ELSE CAST(nom_poi AS STRING)
        END AS code_insee
    FROM `{RAW_TABLE_ERA5}`
    WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {ERA5_DEDUP_LOOKBACK_DAYS} DAY)
    """
    existing = set()
    try:
        for row in client.query(q):
            existing.add((str(row["date"]), row["code_insee"]))
        print(f"   [OK] {len(existing)} enregistrements ERA5 existants")
    except Exception:
        print("   [i] La table ERA5 n'existe peut-etre pas encore")
    return existing


def fetch_batch_era5(communes: List[Dict], start_date: str, end_date: str) -> dict:
    """Fetch batch of communes from ERA5 API (agregats journaliers)"""
    params = {
        "latitude": ",".join(str(c["latitude"]) for c in communes),
        "longitude": ",".join(str(c["longitude"]) for c in communes),
        "start_date": start_date,
        "end_date": end_date,
        "daily": ERA5_DAILY_VARS,
        "timezone": "Europe/Paris",
    }
    return fetch_with_retry(OPEN_METEO_URL, params, timeout=90)


def build_era5_rows(data: Any, communes: List[Dict]) -> List[Dict]:
    """Build rows from ERA5 daily response"""
    rows: List[Dict] = []

    if isinstance(data, dict):
        results = data.get("results") or [data]
    elif isinstance(data, list):
        results = data
    else:
        return rows

    if not results:
        return rows

    def val(arr, i):
        if not arr or i >= len(arr) or arr[i] is None:
            return None
        try:
            return float(arr[i])
        except (TypeError, ValueError):
            return None

    for idx, commune in enumerate(communes):
        if idx >= len(results):
            continue

        result = results[idx]
        if not isinstance(result, dict):
            continue

        daily = result.get("daily") or {}
        times = daily.get("time") or []

        for i, t in enumerate(times):
            row = {
                "time": f"{t}T00:00:00Z",
                "nom_poi": str(commune["code_insee"]),
                "ville": commune.get("ville"),
                "latitude_poi": commune["latitude"],
                "longitude_poi": commune["longitude"],
                "numero_departement": commune.get("numero_departement"),
            }
            for var in ERA5_DAILY_VARS:
                row[var] = val(daily.get(var), i)
            rows.append(row)

    return rows


def extract_era5() -> bool:
    """Main ERA5 extraction pipeline"""
    print("\n" + "=" * 60)
    print("ERA5 WEATHER DATA")
    print("=" * 60)

    try:
        communes_list = list(get_communes_era5().values())
        if not communes_list:
            print("   [X] Aucune commune, extraction annulee")
            return False

        existing = get_existing_era5()

        end = (datetime.now(timezone.utc) - timedelta(days=ERA5_LAG_DAYS)).date()
        start = end - timedelta(days=ERA5_WINDOW_DAYS)
        print(f"   [i] Periode: {start} -> {end}")

        rows: List[Dict] = []
        skipped = 0
        total = len(communes_list)
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        for i in range(0, total, BATCH_SIZE):
            batch = communes_list[i:i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1

            print(f"\n   [{batch_num}/{total_batches}] {len(batch)} communes")

            data = fetch_batch_era5(batch, str(start), str(end))
            batch_rows = build_era5_rows(data, batch)

            new = 0
            for r in batch_rows:
                date_str = r["time"].split("T")[0]
                key = (date_str, r["nom_poi"])
                if key not in existing:
                    rows.append(r)
                    existing.add(key)
                    new += 1
                else:
                    skipped += 1

            print(f"       {len(batch_rows)} recues, {new} nouvelles")

        print(f"\n   [i] Total: {len(rows)} nouvelles, {skipped} deja presentes")

        if not rows:
            print("   [!] Aucune nouvelle ligne ERA5")
            return True

        # Controle qualite avant chargement
        missing_max = sum(1 for r in rows if r.get("temperature_2m_max") is None)
        if missing_max:
            pct = 100 * missing_max / len(rows)
            print(f"   [!] ATTENTION: {missing_max} lignes ({pct:.1f}%) sans temperature_2m_max")
            if pct > 50:
                print("   [X] Plus de 50% de valeurs manquantes - chargement annule")
                print("       Verifier la reponse de l'API avant de reessayer.")
                return False

        return load_to_bigquery("raw_open_meteo", rows)

    except Exception as e:
        print(f"   [X] ERREUR: {e}")
        return False


# ============================================
# ODISSE EXTRACTION
# ============================================

def fetch_odisse_dataset_all(dataset_id: str) -> List[Dict]:
    """Fetch ALL records from Odisse dataset using pagination (max 100/page)"""
    url = f"{ODISSE_API}/{dataset_id}/records"
    all_records: List[Dict] = []
    offset = 0
    page_size = 100

    while True:
        data = fetch_with_retry(url, {"limit": page_size, "offset": offset}, timeout=30)
        if not data:
            break

        results = data.get("results", [])
        if not results:
            break

        all_records.extend(results)
        print(f"       page offset={offset}: +{len(results)} (total: {len(all_records)})")

        if len(results) < page_size or offset >= 10000:
            break

        offset += page_size

    print(f"       [OK] {len(all_records)} enregistrements recuperes")
    return all_records


def extract_odisse_dataset(
    dataset_id: str,
    table_name: str,
    dataset_name: str,
    row_mapper: Optional[Callable[[Dict], Dict]] = None,
    dedup_key: Optional[Tuple[str, str]] = None,
) -> bool:
    """Extraction generique d'un dataset Odisse"""
    print_section(dataset_name)

    try:
        existing = set()
        if dedup_key:
            existing = get_existing_records(table_name, list(dedup_key))

        records = fetch_odisse_dataset_all(dataset_id)
        if not records:
            print("       [!] Aucun enregistrement")
            return False

        new_rows: List[Dict] = []
        for r in records:
            row = row_mapper(r) if row_mapper else dict(r)

            if dedup_key:
                key = (r.get(dedup_key[0]), r.get(dedup_key[1]))
                if key not in existing:
                    new_rows.append(row)
                    existing.add(key)
            else:
                record_hash = hashlib.md5(
                    json.dumps(r, sort_keys=True, default=str).encode()
                ).hexdigest()
                if record_hash not in existing:
                    row["record_hash"] = record_hash
                    new_rows.append(row)
                    existing.add(record_hash)

        print(f"       {len(records)} recus, {len(new_rows)} nouveaux")
        return load_to_bigquery(table_name, new_rows)

    except Exception as e:
        print(f"       [X] ERREUR: {e}")
        return False


def map_canicule_jours(r: Dict) -> Dict:
    return {
        "code_departement": r.get("dep"),
        "departement_nom": r.get("libgeo"),
        "region_code": r.get("reg"),
        "region_nom": r.get("reglib"),
        "annee": r.get("annee"),
        "nombre_jours": r.get("nb_j_can"),
    }


def map_syndrome(pathologie_name: str) -> Callable[[Dict], Dict]:
    def mapper(r: Dict) -> Dict:
        row = dict(r)
        row["pathologie"] = pathologie_name
        return row
    return mapper


def extract_odisse() -> bool:
    """Main Odisse extraction pipeline"""
    print("\n" + "=" * 60)
    print("ODISSE HEALTH DATA")
    print("=" * 60)

    ok = True

    ok &= extract_odisse_dataset(
        "canicules-nombres-de-jours-de-canicule-departement",
        "raw_odisse_canicule_jours",
        "Jours de canicule par departement",
        row_mapper=map_canicule_jours,
        dedup_key=("dep", "annee"),
    )

    ok &= extract_odisse_dataset(
        "canicules-deces-attribuables-a-la-chaleur-pendant-lete-et-pendant-les-vagues-de-chaleur-france",
        "raw_odisse_deces_chaleur",
        "Deces attribuables a la chaleur",
    )

    syndromes = [
        ("grippe-passages-aux-urgences-et-actes-sos-medecins-france",
         "raw_odisse_grippe", "Grippe"),
        ("bronchiolite-passages-aux-urgences-et-actes-sos-medecins-france",
         "raw_odisse_bronchiolite", "Bronchiolite"),
        ("gastro-enterite-aigue-passages-aux-urgences-et-actes-sos-medecins-france",
         "raw_odisse_gastro_enterite", "Gastro-enterite aigue"),
    ]

    for dataset_id, table_name, pathologie in syndromes:
        ok &= extract_odisse_dataset(
            dataset_id,
            table_name,
            pathologie,
            row_mapper=map_syndrome(pathologie),
        )

    return ok


# ============================================
# MAIN
# ============================================

def main() -> int:
    print("\n" + "=" * 60)
    print("DATA EXTRACTION PIPELINE v2.0")
    print("=" * 60)

    era5_ok = extract_era5()
    odisse_ok = extract_odisse()

    print("\n" + "=" * 60)
    if era5_ok and odisse_ok:
        print("[OK] EXTRACTION TERMINEE")
        code = 0
    else:
        print("[X] EXTRACTION TERMINEE AVEC ERREURS")
        print(f"    ERA5: {'OK' if era5_ok else 'ECHEC'}")
        print(f"    Odisse: {'OK' if odisse_ok else 'ECHEC'}")
        code = 1
    print("=" * 60)
    return code


if __name__ == "__main__":
    raise SystemExit(main())