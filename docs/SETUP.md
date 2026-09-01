# Setup Local

## Prérequis

- Python 3.9+
- git
- gcloud CLI (Google Cloud SDK)
- Compte GCP avec accès datasets newfourcasters
- zsh (ou bash)

## 1. Clone et environnement

```bash
# Clone le repo
git clone https://github.com/athevenet300882/lesfourcasters.git
cd lesfourcasters

# Crée virtual env
python3 -m venv venv
source venv/bin/activate

# Installe dépendances
pip install -r requirements.txt
```

## 2. Authentification GCP

```bash
# Authentifie (OAuth)
gcloud auth application-default login

# Vérifie accès BigQuery
bq ls --project_id=newfourcasters

# Tu devrais voir:
# datasetId: lesfourcasters_raw
# datasetId: lesfourcasters_dbt
```

## 3. Configuration dbt

Crée `~/.dbt/profiles.yml`:

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

**Alternative**: Stocker dans le projet (`profiles.yml` à la racine), puis:
```bash
echo "profiles.yml" >> .gitignore
dbt debug --profiles-dir .
dbt run --profiles-dir .
```

## 4. Test de connexion

```bash
cd ~/projects/lesfourcasters_dbt/lesfourcasters_dbt

dbt debug

# Devrait afficher: ✓ Connection test: OK
```

## 5. Premier dbt run

```bash
# Staging uniquement
dbt run --select staging

# Tous les modèles
dbt run

# Tests
dbt test
```

## 6. Explorer les données

```bash
# Affiche 5 lignes
dbt show --select stg_open_meteo --limit 5

# BigQuery direct
bq query --nouse_legacy_sql "
SELECT COUNT(*) as total, COUNT(DISTINCT date) as days
FROM `newfourcasters.lesfourcasters_dbt.fct_daily_heat_health`
"
```

## Variables d'environnement (.env)

Optionnel pour les clés API:

```bash
cat > .env << 'ENVEOF'
GCP_PROJECT_ID=newfourcasters
GCP_DATASET_RAW=lesfourcasters_raw
GCP_DATASET_DBT=lesfourcasters_dbt
ODISSE_API_KEY=<your-key>
ENVEOF

echo ".env" >> .gitignore
```

## Troubleshooting Setup

### "No such file or directory: /tmp/gcp-key.json"
Utilise `dev` (OAuth) en local, pas `ci` (service account):
```bash
dbt run  # ← Default: dev
```

### "Connection test: FAIL"
Réauthentifie:
```bash
gcloud auth application-default login
gcloud auth list
```

### "Could not find profiles.yml"
```bash
# Option 1: ~/.dbt/profiles.yml
mkdir -p ~/.dbt

# Option 2: À la racine du projet
dbt debug --profiles-dir .
```

## Commandes de base

```bash
# dbt
dbt run
dbt test
dbt docs generate && dbt docs serve

# BigQuery
bq ls lesfourcasters_raw
bq show lesfourcasters_raw.raw_open_meteo

# Git
git status
git add .
git commit -m "message"
git push origin main
```