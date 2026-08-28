import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
import hashlib
import io

GCP_PROJECT = 'newfourcasters'

def get_communes_from_bigquery():
    """Récupère toutes les communes depuis BigQuery"""
    client = bigquery.Client(project=GCP_PROJECT)
    
    query = """
    SELECT DISTINCT Commune, Latitude, Longitude, Numero_Departement
    FROM `newfourcasters.lesfourcasters_raw.raw_communes_referentiel`
    ORDER BY Commune
    """
    
    results = client.query(query).to_dataframe()
    
    communes = []
    for _, row in results.iterrows():
        communes.append({
            'name': row['Commune'],
            'lat': row['Latitude'],
            'lon': row['Longitude'],
            'dept': str(row['Numero_Departement']).zfill(2)
        })
    
    return communes

def get_era5_historical(lat, lon, city, dept, start_date, end_date):
    """Collecte ERA5 historique"""
    
    url = f"https://archive-api.open-meteo.com/v1/era5?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&daily=temperature_2m_mean,temperature_2m_min,temperature_2m_max,relative_humidity_2m_mean&timezone=Europe/Paris"
    
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        if 'daily' not in data:
            return []
        
        rows = []
        for i, date_str in enumerate(data['daily']['time']):
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
        print(f"❌ Erreur {city}: {e}")
        return []

def compute_hash(row):
    """Hash pour déduplication"""
    key_str = f"{row['time']}_{row['nom_poi']}_{row['latitude_poi']}_{row['longitude_poi']}"
    return hashlib.sha256(key_str.encode()).hexdigest()

def main():
    print("🌍 Backfill ERA5 Seamless (01/08 - 23/08) - TOUTES LES COMMUNES...")
    
    # Récupère toutes les communes depuis BigQuery
    communes = get_communes_from_bigquery()
    print(f"✅ {len(communes)} communes trouvées")
    
    start_date = '2026-08-01'
    end_date = '2026-08-23'
    
    all_rows = []
    for idx, commune in enumerate(communes):
        rows = get_era5_historical(commune['lat'], commune['lon'], commune['name'], commune['dept'], start_date, end_date)
        for row in rows:
            row['row_hash'] = compute_hash(row)
            all_rows.append(row)
        
        if (idx + 1) % 50 == 0:
            print(f"✅ {idx + 1}/{len(communes)} communes traitées")
    
    if not all_rows:
        print("❌ Aucune donnée collectée")
        return
    
    df = pd.DataFrame(all_rows)
    df = df.drop(columns=['row_hash'])
    
    print(f"✅ {len(df)} lignes au total")
    
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        table_id = f"{GCP_PROJECT}.lesfourcasters_raw.raw_open_meteo"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition='WRITE_APPEND'
        )
        
        json_data = df.to_json(orient='records', date_format='iso', lines=True)
        
        load_job = client.load_table_from_file(
            io.StringIO(json_data),
            table_id,
            job_config=job_config
        )
        
        load_job.result()
        print(f"✅ {load_job.output_rows} lignes ajoutées à BigQuery")
        
    except Exception as e:
        print(f"❌ Erreur BigQuery: {e}")

if __name__ == '__main__':
    main()