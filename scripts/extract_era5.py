import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import hashlib
import io

GCP_PROJECT = 'newfourcasters'
TABLE_ID = f"{GCP_PROJECT}.lesfourcasters_raw.raw_open_meteo"

LAG_DAYS = 5
WINDOW_DAYS = 10

BATCH_SIZE = 50        # communes par requete
PAUSE_SECONDS = 2      # pause entre deux requetes
MAX_RETRIES = 3


def compute_hash(row):
    """Hash SHA256 de la cle metier : une commune n'a qu'une mesure par jour."""
    key = f"{row['time']}_{row['nom_poi']}"
    return hashlib.sha256(key.encode()).hexdigest()


def get_communes_from_bigquery(client):
    query = """
    SELECT DISTINCT Commune, Latitude, Longitude, Numero_Departement
    FROM `newfourcasters.lesfourcasters_raw.raw_communes_referentiel`
    WHERE Commune IS NOT NULL
      AND Latitude IS NOT NULL
      AND Longitude IS NOT NULL
    ORDER BY Commune
    """
    results = client.query(query).to_dataframe()
    return [
        {
            'name': row['Commune'],
            'lat': row['Latitude'],
            'lon': row['Longitude'],
            'dept': str(row['Numero_Departement']).zfill(2),
        }
        for _, row in results.iterrows()
    ]


def get_existing_hashes(client, start_date, end_date):
    query = f"""
    SELECT DISTINCT
      FORMAT_TIMESTAMP('%Y-%m-%d 00:00:00', time) AS time_str,
      nom_poi
    FROM `{TABLE_ID}`
    WHERE DATE(time) BETWEEN '{start_date}' AND '{end_date}'
    """
    results = client.query(query).to_dataframe()
    return {
        compute_hash({'time': row['time_str'], 'nom_poi': row['nom_poi']})
        for _, row in results.iterrows()
    }


def fetch_batch(batch, start_date, end_date):
    """Une seule requete pour tout un lot de communes."""
    lats = ",".join(str(c['lat']) for c in batch)
    lons = ",".join(str(c['lon']) for c in batch)
    url = (
        "https://archive-api.open-meteo.com/v1/era5"
        f"?latitude={lats}&longitude={lons}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=temperature_2m_mean,temperature_2m_min,temperature_2m_max,"
        "relative_humidity_2m_mean"
        "&timezone=Europe/Paris"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=90)
            resp.raise_for_status()
            payload = resp.json()
            # avec plusieurs coordonnees l'API renvoie une liste
            return payload if isinstance(payload, list) else [payload]
        except Exception as e:
            print(f"  tentative {attempt}/{MAX_RETRIES} echouee: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(5 * attempt)
    return []


def main():
    end_date = (datetime.now() - timedelta(days=LAG_DAYS)).date().isoformat()
    start_date = (datetime.now() - timedelta(days=LAG_DAYS + WINDOW_DAYS)).date().isoformat()
    print(f"Collecte ERA5 du {start_date} au {end_date}", flush=True)

    client = bigquery.Client(project=GCP_PROJECT)

    communes = get_communes_from_bigquery(client)
    print(f"{len(communes)} communes", flush=True)

    existing = get_existing_hashes(client, start_date, end_date)
    print(f"{len(existing)} lignes deja presentes sur la fenetre", flush=True)

    new_rows = []
    for i in range(0, len(communes), BATCH_SIZE):
        batch = communes[i:i + BATCH_SIZE]
        print(f"Lot {i // BATCH_SIZE + 1} ({len(batch)} communes)", flush=True)

        results = fetch_batch(batch, start_date, end_date)

        for commune, data in zip(batch, results):
            daily = data.get('daily')
            if not daily:
                continue
            for j, date_str in enumerate(daily['time']):
                if daily['temperature_2m_mean'][j] is None:
                    continue
                row = {
                    'time': f"{date_str} 00:00:00",
                    'nom_poi': commune['name'],
                    'numero_departement': commune['dept'],
                    'latitude_poi': commune['lat'],
                    'longitude_poi': commune['lon'],
                    'temperature_2m_mean': daily['temperature_2m_mean'][j],
                    'temperature_2m_min': daily['temperature_2m_min'][j],
                    'temperature_2m_max': daily['temperature_2m_max'][j],
                    'relative_humidity_2m_mean': daily['relative_humidity_2m_mean'][j],
                }
                if compute_hash(row) not in existing:
                    new_rows.append(row)

        time.sleep(PAUSE_SECONDS)

    if not new_rows:
        print("Aucune nouvelle ligne a inserer", flush=True)
        return

    df = pd.DataFrame(new_rows)
    print(f"{len(df)} nouvelles lignes", flush=True)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition='WRITE_APPEND',
    )
    json_data = df.to_json(orient='records', date_format='iso', lines=True)
    load_job = client.load_table_from_file(
        io.StringIO(json_data), TABLE_ID, job_config=job_config
    )
    load_job.result()
    print(f"{load_job.output_rows} lignes ajoutees a BigQuery", flush=True)


if __name__ == '__main__':
    main()