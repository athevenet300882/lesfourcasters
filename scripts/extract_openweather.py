import os
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
import hashlib
import json
import io

# Config
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
GCP_PROJECT = os.getenv('GCP_PROJECT_ID', 'newfourcasters')

# Liste des communes (exemple simplifié)
COMMUNES = [
    {'name': 'Paris', 'lat': 48.8566, 'lon': 2.3522, 'dept': '75'},
    {'name': 'Marseille', 'lat': 43.2965, 'lon': 5.3698, 'dept': '13'},
    {'name': 'Lyon', 'lat': 45.7640, 'lon': 4.8357, 'dept': '69'},
    {'name': 'Toulouse', 'lat': 43.6047, 'lon': 1.4442, 'dept': '31'},
    {'name': 'Nice', 'lat': 43.7102, 'lon': 7.2620, 'dept': '06'},
]

def get_weather(lat, lon, city, dept):
    """Collecte données OpenWeather"""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=metric"
    
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        
        return {
            'date': datetime.now().date().isoformat(),
            'time': datetime.now().time().isoformat(),
            'nom_poi': city,
            'numero_departement': dept,
            'latitude_poi': lat,
            'longitude_poi': lon,
            'temperature_2m_mean': data.get('main', {}).get('temp'),
            'temperature_2m_min': data.get('main', {}).get('temp_min'),
            'temperature_2m_max': data.get('main', {}).get('temp_max'),
            'relative_humidity_2m_mean': data.get('main', {}).get('humidity'),
            'weather_code': data.get('weather', [{}])[0].get('id'),
            'loaded_at': datetime.now().isoformat(),
        }
    except Exception as e:
        print(f"❌ Erreur {city}: {e}")
        return None

def compute_hash(row):
    """Crée hash SHA256 pour déduplication"""
    key_str = f"{row['date']}_{row['nom_poi']}_{row['latitude_poi']}_{row['longitude_poi']}"
    return hashlib.sha256(key_str.encode()).hexdigest()

def main():
    print("🌍 Collecte OpenWeather...")
    
    if not OPENWEATHER_API_KEY:
        print("❌ OPENWEATHER_API_KEY manquante")
        return
    
    # Collecter données
    rows = []
    for commune in COMMUNES:
        row = get_weather(commune['lat'], commune['lon'], commune['name'], commune['dept'])
        if row:
            row['row_hash'] = compute_hash(row)
            rows.append(row)
            print(f"✅ {commune['name']}")
    
    if not rows:
        print("❌ Aucune donnée collectée")
        return
    
    df = pd.DataFrame(rows)
    print(f"✅ {len(df)} communes collectées")
    
    # Upload BigQuery
    try:
        client = bigquery.Client(project=GCP_PROJECT)
        table_id = f"{GCP_PROJECT}.lesfourcasters_raw.raw_open_meteo"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition='WRITE_APPEND'
        )
        
        # Convertir en JSON lines
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
        raise

if __name__ == '__main__':
    main()