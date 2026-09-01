# Architecture

## Vue d'ensemble

lesfourcasters analyse l'impact des vagues de chaleur sur la santé publique en France, utilisant une infrastructure cloud GCP/BigQuery avec orchestration dbt.

## Infrastructure GCP

| Composant | Détail |
|-----------|--------|
| Projet | newfourcasters (ID: 795647081240) |
| Datasets | lesfourcasters_raw, lesfourcasters_dbt |
| Buckets | gs://newfourcasters-raw-data, gs://newfourcasters-staging, gs://newfourcasters-scripts |
| Service Account | github-actions-697@newfourcasters.iam.gserviceaccount.com |
| Location | EU (Europe) |
| Crédit essai | 263 EUR, expiration 2 nov 2026 |

## Pipeline ETL

```
Sources Brutes (Open-Meteo, Odissé, Communes)
    ↓
Staging (nettoyage, renommage, formatage)
    ↓
Intermediate (jointures, enrichissement géographique)
    ↓
Mart (tables analytiques prêtes BI)
    ↓
BigQuery (analyse, dashboards)
```

## Modèles dbt

### Staging (nettoyage, renommage)
- **stg_open_meteo**: 3.5M lignes, 29 variables ERA5 renommées en français
- **stg_communes**: 360 lignes, référentiel INSEE avec codes, coordonnées, régions
- **stg_odisse_canicule**: 2.1K lignes, données sanitaires département-année
- **stg_odisse_hivernales**: 3.4K lignes (hors périmètre chaleur)

### Intermediate (jointures, enrichissement)
- **int_health_weather_join**: 3.5M lignes, jointure double sur département puis commune normalisée (NORMALIZE + REGEXP_REPLACE), déduplication par ROW_NUMBER()

### Mart (tables analytiques)
- **dim_commune**: 360 lignes, dimension communes (code INSEE, nom, région, coordonnées)
- **fct_daily_heat_health**: 3.5M+ lignes, fact table grains commune-jour (température, humidité, alertes, décès)

## Sources de données

### Open-Meteo ERA5
- **Endpoint**: archive-api.open-meteo.com/v1/archive
- **Fréquence**: Quotidienne
- **Délai**: ~5 jours
- **Variables**: 29 (températures, humidité, précipitations, vent, pression, ensoleillement, sol)
- **Communes**: 360
- **Sans clé API**: Public, gratuit

### Odissé (Santé publique France)
- **API**: Opendatasoft v2.1
- **Maille**: Département-Année (pas commune-jour)
- **Jeux de données**: 5 (canicules, jours, sévérité, population, décès)
- **Licence**: Licence Ouverte 2.0
- **Status**: Actuellement CSV manuel, collecte API à implémenter

### Référentiel communes
- **Format**: Excel converti
- **Lignes**: 360 communes
- **Colonnes**: Code INSEE, nom, département, région, coordonnées, SDIS

## Orchestration

### GitHub Actions Workflow
- **Fichier**: `.github/workflows/daily-pipeline.yml`
- **Fréquence**: 6h UTC (quotidien) + déclenche manuel possible
- **Étapes**:
  1. Checkout code
  2. Setup Python 3.12
  3. Installer dépendances
  4. Authentifier GCP (service account)
  5. Fetch ERA5 (Open-Meteo)
  6. Fetch Odissé (Santé publique)
  7. dbt run (transformations)
  8. dbt test (validation)

### Fenêtre glissante
- Collection: J-15 à J-5 (rétroactive)
- Rattrapage: 10 jours en cas d'échec
- Déduplication: SHA-256(date + commune)

## Qualité des données

| Contrôle | Statut | Détail |
|----------|--------|--------|
| Valeurs manquantes (colonnes clés) | ✓ OK | 0 nulls dans date, commune, température |
| Doublons | ✓ OK | 0 doublons après ROW_NUMBER() |
| Continuité temporelle | ✓ OK | 9,735 jours sans lacune (2000-2026) |
| Couverture quotidienne | ✓ OK | 360 communes chaque jour |
| Codes INSEE | ✓ OK | 360 communes = 360 codes |
| Bornes physiques | ✓ OK | Températures -30 à +45°C, humidité 0-100% |

## Environnement local

- **Poste**: iMac macOS
- **Python**: 3.12.x
- **Shell**: zsh
- **Repo local**: ~/projects/lesfourcasters_dbt/lesfourcasters_dbt
- **dbt version**: 1.12.3+

## GitHub

- **Repo**: athevenet300882/lesfourcasters
- **Branch principale**: main
- **Strategy**: Feature branches + Pull Requests
- **Protected branches**: main (require PR review)