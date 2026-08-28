import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import hashlib
import io

GCP_PROJECT = 'newfourcasters'
TABLE_ID = f"{GCP_PROJECT}.lesfourcasters_raw.raw_open_meteo"

# ERA5 a un délai de ~5 jours : on va chercher une fenêtre glissante
LAG_DAYS = 5
WINDOW_DAYS = 10


def compute_hash(row):
    """Hash SHA256 de la cle metier : une commune n'a qu'une mesure par jour."""
    key = f"{row['time']}_{row['nom_poi']}"
    return hashlib.sha256(key.encode()).hexdigest()


def get_communes_from_bigquery(client):
    query = """
    SELECT DISTINCT Commune, Latitude, Longitude, Numero_Departement
    FROM `newfourcasters.lesfourcasters_raw.raw_communes_referentiel`
    WHERE Commune IS NOT NULL
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
    """Hashs deja presents dans BigQuery sur la fenetre."""
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


def get_era5(lat, lon, city, dept, start_date, end_date):
    url = (
        "https://archive-api.open-meteo.com/v1/era5"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=temperature_2m_mean,temperature_2m_min,temperature_2m_max,"
        "relative_humidity_2m_mean"
        "&timezone=Europe/Paris"
    )
    try:
        data = requests.get(url, timeout=15).json()
        if 'daily' not in data:
            return []

        rows = []
        for i, date_str in enumerate(data['daily']['time']):
            if data['daily']['temperature_2m_mean'][i] is None:
                continue
            rows.append({
                'time': f"{date_str} 00:00:00",
                'nom_poi': city,
                'numero_departement': dept,
                'latitude_poi': lat,
                'longitude_poi': lon,
                'temperature_2m_mean': data['daily']['temperature_2m_mean'][i],
                'temperature_2m_min': data['daily']['temperature_2m_min'][i],
                'temperature_2m_max': data['daily']['temperature_2m_max'][i],
                'relative_humidity_2m_mean': data['daily']['relative_humidity_2m_mean'][i],
            })
        return rows
    except Exception as e:
        print(f"Erreur {city}: {e}")
        return []


def main():
    end_date = (datetime.now() - timedelta(days=LAG_DAYS)).date().isoformat()
    start_date = (datetime.now() - timedelta(days=LAG_DAYS + WINDOW_DAYS)).date().isoformat()
    print(f"Collecte ERA5 du {start_date} au {end_date}")

    client = bigquery.Client(project=GCP_PROJECT)

    communes = get_communes_from_bigquery(client)
    print(f"{len(communes)} communes")

    existing = get_existing_hashes(client, start_date, end_date)
    print(f"{len(existing)} lignes deja presentes sur la fenetre")

    new_rows = []
    for idx, c in enumerate(communes):
        for row in get_era5(c['lat'], c['lon'], c['name'], c['dept'], start_date, end_date):
            if compute_hash(row) not in existing:
                new_rows.append(row)
        if (idx + 1) % 50 == 0:
            print(f"{idx + 1}/{len(communes)} communes traitees")

    if not new_rows:
        print("Aucune nouvelle ligne a inserer")
        return

    df = pd.DataFrame(new_rows)
    print(f"{len(df)} nouvelles lignes")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition='WRITE_APPEND',
    )
    json_data = df.to_json(orient='records', date_format='iso', lines=True)
    load_job = client.load_table_from_file(
        io.StringIO(json_data), TABLE_ID, job_config=job_config
    )
    load_job.result()
    print(f"{load_job.output_rows} lignes ajoutees a BigQuery")


if __name__ == '__main__':
    main()