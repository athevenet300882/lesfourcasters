#!/usr/bin/env python3
"""
Script de collecte Odissé - Santé publique France
Récupère canicules, décès, épidémies hivernales
"""

import logging
import requests
from datetime import datetime
from typing import List, Dict
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

PROJECT_ID = "newfourcasters"
DATASET_ID = "lesfourcasters_raw"

# API Odissé (OpenDataSoft v2.1)
ODISSE_API = "https://odisse.santepubliquefrance.fr/api/explore/v2.1/catalog/datasets"

class OdisseCollector:
    """Collecteur données Odissé"""
    
    def __init__(self):
        self.client = bigquery.Client(project=PROJECT_ID)
        self.session = requests.Session()
    
    def fetch_dataset(self, dataset_id: str, limit: int = 10000) -> List[Dict]:
        """Récupère un dataset Odissé"""
        url = f"{ODISSE_API}/{dataset_id}/records"
        params = {'limit': limit}
        
        try:
            logger.info(f"Fetching {dataset_id}...")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            records = [r.get('record', {}).get('fields', {}) for r in data.get('results', [])]
            logger.info(f"✅ Récupéré {len(records)} records")
            
            return records
        
        except Exception as e:
            logger.error(f"❌ Erreur: {e}")
            return []
    
    def create_table(self, table_name: str, schema: List):
        """Crée table BigQuery"""
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        try:
            self.client.get_table(table_id)
            logger.info(f"✅ Table {table_name} existe")
        except:
            logger.info(f"Création table {table_name}...")
            table = bigquery.Table(table_id, schema=schema)
            self.client.create_table(table)
            logger.info(f"✅ Table créée")
    
    def load_to_bigquery(self, table_name: str, records: List[Dict]):
        """Charge records dans BigQuery"""
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        
        if not records:
            logger.warning("Aucun record")
            return
        
        try:
            logger.info(f"Chargement {len(records)} records dans {table_name}...")
            errors = self.client.insert_rows_json(table_id, records)
            
            if errors:
                logger.error(f"❌ Erreurs: {errors}")
            else:
                logger.info(f"✅ {len(records)} records chargés")
        
        except Exception as e:
            logger.error(f"❌ Erreur: {e}")
    
    def run(self):
        """Lance collecte complète"""
        logger.info("=" * 60)
        logger.info("DÉMARRAGE COLLECTE ODISSÉ")
        logger.info("=" * 60)
        
        # Dataset 1: Jours de canicule
        logger.info("\n📊 Dataset 1: Jours de canicule par département")
        schema1 = [
            bigquery.SchemaField("code_departement", "STRING"),
            bigquery.SchemaField("departement_nom", "STRING"),
            bigquery.SchemaField("annee", "INTEGER"),
            bigquery.SchemaField("nombre_jours", "INTEGER"),
            bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="NULLABLE"),
        ]
        self.create_table("raw_odisse_canicule_jours", schema1)
        
        records1 = self.fetch_dataset("canicules-nombres-de-jours-de-canicule-departement")
        if records1:
            # Transforme
            transformed1 = []
            for r in records1:
                transformed1.append({
                    'code_departement': r.get('code_departement'),
                    'departement_nom': r.get('nom_departement'),
                    'annee': r.get('annee'),
                    'nombre_jours': r.get('nombre_de_jours'),
                    'inserted_at': datetime.utcnow().isoformat() + 'Z'
                })
            self.load_to_bigquery("raw_odisse_canicule_jours", transformed1)
        
        # Dataset 2: Décès attribuables
        logger.info("\n📊 Dataset 2: Décès attribuables à la chaleur")
        schema2 = [
            bigquery.SchemaField("code_departement", "STRING"),
            bigquery.SchemaField("departement_nom", "STRING"),
            bigquery.SchemaField("annee", "INTEGER"),
            bigquery.SchemaField("deces_attribuables", "INTEGER"),
            bigquery.SchemaField("fraction_deces", "FLOAT64"),
            bigquery.SchemaField("inserted_at", "TIMESTAMP", mode="NULLABLE"),
        ]
        self.create_table("raw_odisse_deces_chaleur", schema2)
        
        records2 = self.fetch_dataset("canicules-deces-attribuables-a-la-chaleur-pendant-lete-et-pendant-les-vagues-de-chaleur-france")
        if records2:
            transformed2 = []
            for r in records2:
                transformed2.append({
                    'code_departement': r.get('code_departement'),
                    'departement_nom': r.get('nom_departement'),
                    'annee': r.get('annee'),
                    'deces_attribuables': r.get('nombre_de_deces_attribuables'),
                    'fraction_deces': r.get('fraction_de_deces_attribuables'),
                    'inserted_at': datetime.utcnow().isoformat() + 'Z'
                })
            self.load_to_bigquery("raw_odisse_deces_chaleur", transformed2)
        
        logger.info("=" * 60)
        logger.info("✅ COLLECTE TERMINÉE")
        logger.info("=" * 60)


if __name__ == "__main__":
    collector = OdisseCollector()
    collector.run()
