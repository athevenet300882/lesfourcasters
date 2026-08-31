# lesfourcasters

Analyse de l'impact des vagues de chaleur sur la santé publique en France.

Projet de fin de formation Wild Code School (Data Analyst, cohort mai–novembre 2026) combinant data engineering, analytics et health data science.

---

## Vue d'ensemble

lesfourcasters collecte, transforme et analyse les données de vagues de chaleur et leurs impacts sanitaires en France, utilisant une infrastructure cloud GCP/BigQuery avec orchestration dbt et visualisation BI.

### Objectifs
- Modéliser les tendances de chaleur extrême (ERA5 data, Open-Meteo)
- Quantifier l'impact sanitaire (décès, hospitalisations via API Odissé)
- Analyser les disparités régionales (communes, départements, régions)
- Fournir un dashboard exécutif pour les décideurs de santé publique

### Personas clés
- Alisson : Chef des opérations SDIS (pompiers) – besoins opérationnels temps réel
- Data analysts, chercheurs en santé publique

---

## Architecture

```
SOURCES EXTERNES
│
├─ Open-Meteo ERA5 (weather: temp, precip, wind, etc.)
├─ Santé publique France / API Odissé (health impact)
└─ Géolocalisation (communes ↔ regions via INSEE codes)
│
↓
GitHub Actions (Daily @ 6h UTC)
│ .github/workflows/daily-pipeline.yml
│
↓
GCP Cloud Storage (Raw)
│ lesfourcasters_raw/
│   ├─ era5/
│   ├─ odisse/
│   └─ geo/
│
↓
BigQuery Dataset: RAW (lesfourcasters_raw)
│ stg_*.sql (staging, deduplication)
│
↓
dbt (lesfourcasters_dbt/)
│ models/staging/
│ models/mart/
│ tests/
│ macros/
│
↓
BigQuery Dataset: DBT (lesfourcasters_dbt)
│ 3.5M+ rows – ready for BI
│
↓
Power BI / Looker Studio
│ Executive Dashboard
```

---

## Stack Technique

| Composant | Technologie |
|-----------|-------------|
| Cloud | Google Cloud Platform (GCP) |
| Data Warehouse | BigQuery |
| ETL/Data Pipeline | dbt (Core 1.12.3+) |
| Orchestration | GitHub Actions |
| Data Sources | Open-Meteo, Santé publique France API |
| IaC / Config | YAML (profiles.yml, dbt_project.yml) |
| Version Control | Git / GitHub |
| Languages | Python 3, SQL, YAML |
| BI | Power BI / Looker Studio |

---

## Projets GCP

### newfourcasters (Production)
- Project ID: `newfourcasters`
- Project Number: `795647081240`
- Datasets:
  - `lesfourcasters_raw` – données brutes (staging)
  - `lesfourcasters_dbt` – données transformées (mart)
- Service Account: CI/CD GitHub Actions
- Location: `EU` (Europe)

---

## Installation et Setup Local

### Prérequis
- Python 3.9+
- git
- gcloud CLI (Google Cloud SDK)
- Compte GCP avec accès aux datasets
- zsh (ou bash)

### 1. Clone et environnement

```bash
# Clone le repo
git clone https://github.com/athevenet300882/lesfourcasters.git
cd lesfourcasters

# Crée un virtual env
python3 -m venv venv
source venv/bin/activate

# Installe les dépendances
pip install -r requirements.txt
```

### 2. Authentification GCP

```bash
# Authentifie localement (OAuth)
gcloud auth application-default login

# Vérifie l'accès BigQuery
bq ls --project_id=newfourcasters
```

### 3. dbt Profile local

Le fichier `~/.dbt/profiles.yml` doit contenir:

```yaml
lesfourcasters:
  outputs:
    dev:
      type: bigquery
      project: newfourcasters
      dataset: lesfourcasters_dbt_dev
      threads: 4
      timeout_seconds: 300
      location: EU
      method: oauth
    
    ci:
      type: bigquery
      project: newfourcasters
      dataset: lesfourcasters_dbt
      threads: 4
      timeout_seconds: 300
      location: EU
      method: service-account
      keyfile: /tmp/gcp-key.json
  
  target: dev
```

### 4. Test de connexion

```bash
cd ~/projects/lesfourcasters_dbt/lesfourcasters_dbt

# Test la connexion
dbt debug

# Affiche les datasets
dbt parse
```

---

## Pipeline ETL Quotidien

### Workflow GitHub Actions
Fichier: `.github/workflows/daily-pipeline.yml`

Planification: Tous les jours à 6h UTC (7h heure d'été France)

Etapes:
1. Fetch Open-Meteo ERA5 (20 communes par requête, limite de rate limiting)
2. Fetch Odissé API (health impact data)
3. Push data brute vers GCS / BigQuery lesfourcasters_raw
4. dbt run (transformations) vers lesfourcasters_dbt
5. dbt test (validation)
6. Logs vers BigQuery (dbt_logs table)

### Lancer manuellement

```bash
# Local
dbt run --select staging
dbt test

# Depuis GitHub (onglet Actions > manual trigger)
```

---

## Structure du Repo

```
lesfourcasters/
│
├── .github/
│   └── workflows/
│       └── daily-pipeline.yml
│
├── scripts/
│   ├── fetch_era5.py
│   ├── fetch_odisse.py
│   ├── backfill.py
│   └── utils.py
│
├── lesfourcasters_dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml (git-ignored)
│   ├── requirements.txt
│   │
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_era5.sql
│   │   │   ├── stg_odisse.sql
│   │   │   └── stg_communes.sql
│   │   │
│   │   └── mart/
│   │       ├── fact_heatwave.sql
│   │       ├── dim_commune.sql
│   │       ├── dim_date.sql
│   │       └── agg_health_impact.sql
│   │
│   ├── tests/
│   │   ├── generic/
│   │   │   ├── not_null.sql
│   │   │   └── unique.sql
│   │   └── specific/
│   │       └── test_*.sql
│   │
│   ├── macros/
│   │   ├── normalize_commune_name.sql
│   │   └── hash_row.sql
│   │
│   └── target/ (git-ignored)
│       ├── manifest.json
│       ├── run_results.json
│       └── ...
│
├── docs/
│   ├── architecture.md
│   ├── data_dictionary.md
│   ├── onboarding.md
│   └── cahier_des_charges.md
│
├── README.md (this file)
├── .gitignore
└── requirements.txt
```

---

## Données et Colonnes Clés

### Dataset lesfourcasters_raw (staging)

#### Table: stg_era5
Données météo brutes (Open-Meteo ERA5 reanalysis)

- `date` (DATE)
- `commune` (STRING) – nom normalisé
- `code_insee` (STRING)
- `temperature_2m` (FLOAT64) – Celsius
- `precipitation` (FLOAT64) – mm
- `wind_speed_10m` (FLOAT64) – km/h
- `relative_humidity_2m` (INT64) – pourcentage
- `_hash` (STRING) – SHA256(date + nom_poi) pour dédup
- `_loaded_at` (TIMESTAMP)

#### Table: stg_odisse
Données d'impact sanitaire (API Santé publique France)

- `date` (DATE)
- `commune` (STRING)
- `code_insee` (STRING)
- `deaths_heatwave` (INT64) – décès attribués à la canicule
- `hospitalizations` (INT64)
- `emergency_calls` (INT64)
- `_source_api` (STRING)
- `_loaded_at` (TIMESTAMP)

#### Table: stg_communes
Référentiel géographique (INSEE)

- `code_insee` (STRING) – clé primaire
- `commune_name` (STRING)
- `department_code` (STRING)
- `region_code` (STRING)
- `region_name` (STRING)
- `latitude` (FLOAT64)
- `longitude` (FLOAT64)

---

### Dataset lesfourcasters_dbt (production mart)

#### Table: fact_heatwave (3.5M+ rows)
Fact table centrale – grain: date x commune

- `fact_id` (STRING) – clé primaire
- `date` (DATE)
- `code_insee` (STRING) – FK vers dim_commune
- `temperature_2m` (FLOAT64)
- `precipitation` (FLOAT64)
- `deaths_heatwave` (INT64)
- `hospitalizations` (INT64)
- `emergency_calls` (INT64)
- `heat_wave_alert` (BOOLEAN)
- `created_at` (TIMESTAMP)
- `updated_at` (TIMESTAMP)

#### Table: dim_commune
Dimension communes

- `commune_key` (STRING) – clé primaire
- `code_insee` (STRING)
- `commune_name` (STRING)
- `department_code` (STRING)
- `department_name` (STRING)
- `region_code` (STRING)
- `region_name` (STRING)
- `latitude` (FLOAT64)
- `longitude` (FLOAT64)
- `population_estimate` (INT64)

#### Table: dim_date
Dimension temps

- `date_key` (DATE)
- `year` (INT64)
- `month` (INT64)
- `week_of_year` (INT64)
- `day_of_week` (INT64)
- `is_weekend` (BOOLEAN)

#### Table: agg_health_impact
Agrégation par period + région (pour rapports)

- `period_key` (STRING) – "YYYYMM"
- `region_code` (STRING)
- `total_deaths` (INT64)
- `total_hospitalizations` (INT64)
- `avg_temperature` (FLOAT64)
- `max_temperature` (FLOAT64)
- `alert_days` (INT64)

---

## Tests et Validation

### Lancer les tests dbt

```bash
cd lesfourcasters_dbt/lesfourcasters_dbt

# Tous les tests
dbt test

# Tests spécifiques
dbt test --select test_fact_heatwave_not_null

# Tests sur un modèle
dbt test --select fact_heatwave
```

### Assertions incluses
- Pas de lignes dupliquées (unique sur fact_id)
- Pas de NULLs sur colonnes clés
- Ranges valides (temp: -30C à +50C)
- Intégrité FK (communes existantes)
- Couverture géographique (plus de 350 communes)

---

## Génération de la Documentation dbt

```bash
cd lesfourcasters_dbt/lesfourcasters_dbt

# Génère la doc HTML
dbt docs generate

# Lance le serveur local (port 8000)
dbt docs serve

# Ouvre http://localhost:8000
```

---

## Secrets et Variables d'environnement

Fichier: `.env` (git-ignored)

```bash
# GCP
GCP_PROJECT_ID=newfourcasters
GCP_DATASET_RAW=lesfourcasters_raw
GCP_DATASET_DBT=lesfourcasters_dbt

# APIs
OPEN_METEO_BASE_URL=https://archive-api.open-meteo.com/v1/archive
ODISSE_API_URL=https://api.santefrancepublique.fr/odisse/v1
ODISSE_API_KEY=<your-key-here>
```

GitHub Secrets (à configurer dans Settings > Secrets):
- `GCP_SERVICE_ACCOUNT_KEY` – JSON keyfile pour CI
- `ODISSE_API_KEY` – API key Santé publique France
- `GITHUB_TOKEN` – auto-généré

---

## Equipe et Contributions

### Membres du projet
- Angel – Data Analyst (sanitary impact axis, cahier des charges individual)
- Christophe – Data Engineering
- Eddy – Business Analytics
- Loick – Infrastructure / DevOps

### Branch Policy
- `main` - production (merges via PR uniquement)
- `develop` - staging / feature testing
- `feature/*` - feature branches (fork from develop)

PR Workflow:
1. Feature branch vers PR sur develop
2. Tests + review
3. Merge + auto-deploy to staging
4. Validation puis PR develop vers main
5. Prod deploy + GitHub Actions trigger

---

## Axes Individuels (Cahier des Charges)

### Axe Angel: Impact Sanitaire
- Source: API Odissé (Santé publique France canicule datasets)
- Grain: date x commune x indicator (décès, hospitalisations, appels urgence)
- Livrable: Table stg_odisse + modèles dbt (staging + mart)
- KPIs:
  - Taux d'excès mortalité par région
  - Corrélation temp vs hospitalizations
  - Geographic disparities (urban vs rural)

### Axe Collectif: Weather Infrastructure
- Source: Open-Meteo ERA5 reanalysis
- Grain: date x commune x 29 variables (temp, precip, wind, humidity, etc.)
- Livrable: Table stg_era5 + déduplication robuste + backfill
- KPIs:
  - Couverture complète (360 communes)
  - 0 lignes dupliquées
  - Latency inférieure à 1h après fetch

---

## Troubleshooting

### Problème: Pipeline GitHub Actions échoue

Symptome: Workflow daily-pipeline.yml en echec

Diagnostique:
1. Vérifier les logs: GitHub > Actions > daily-pipeline > Latest Run
2. Vérifier BigQuery: SELECT * FROM lesfourcasters_raw.__dbt_internal_metadata
3. Vérifier les quotas GCP: GCP Console > BigQuery > Quotas

Solutions courantes:
- Timeout Open-Meteo: Augmenter batch_size (actuellement 20 communes)
- Deduplicate failure: Vérifier la logique dans stg_era5.sql
- Missing API key: Vérifier GitHub Secrets ODISSE_API_KEY

### Problème: dbt run échoue localement

Symptome: dbt run retourne une erreur SQL

```bash
# Debug mode
dbt run --debug

# Voir le SQL compilé
dbt compile --select fact_heatwave

# Voir les logs
cat logs/dbt.log
```

### Problème: Commune not found

Symptome: NULL values dans dim_commune join

Cause: Accent/apostrophe mismatch (ex: "Saint-Rémy-de-Provence" vs "Saint-Remy-de-Provence")

Solution: La macro normalize_commune_name() utilise NORMALIZE(..., NFD) + REGEXP_REPLACE() pour harmoniser. Vérifier stg_communes.sql pour la logique de normalisation.

---

## Documentation Complémentaire

- data_dictionary.md – Descriptions détaillées de toutes les colonnes
- cahier_des_charges.md – Requirements complets + axes
- onboarding.md – Guide pour nouveaux contributeurs
- dbt docs – Généré via dbt docs serve

---

## Calendrier et Jalons

| Date | Jalon | Statut |
|------|-------|--------|
| Mai 2026 | Kick-off projet | Termine |
| Juin 2026 | Infrastructure GCP + dbt setup | Termine |
| Juillet 2026 | API integrations + backfill | Termine |
| Aout 2026 | Production pipeline + tests | Termine |
| Septembre 2026 | BI dashboard | En cours |
| Octobre 2026 | Reporting + optimizations | Prevu |
| Novembre 2026 | Graduation + final delivery | Prevu |

---

## Contact et Support

- Slack: #lesfourcasters (Wild Code School workspace)
- Repo Issues: GitHub Issues (bug reports, features)
- Angel (Data Lead): [email/contact]
- GCP Admin: [TBD]

---

## Licence

Projet académique Wild Code School – données publiques (Open-Meteo, Santé publique France).

---

Last Updated: Septembre 2026 | Maintainers: lesfourcasters team