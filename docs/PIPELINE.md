# Pipeline Documentation

## Exécution quotidienne

### GitHub Actions Workflow

**Fichier**: `.github/workflows/daily-pipeline.yml`

```yaml
name: Daily Pipeline

on:
  schedule:
    - cron: '0 6 * * *'  # 6h UTC, quotidien
  workflow_dispatch:      # Déclenchement manuel
```

### Étapes d'exécution

1. **Checkout** – Clone le repo
2. **Setup Python** – Version 3.12
3. **Install dépendances** – `pip install -r requirements.txt`
4. **Authentifier GCP** – Charger service account depuis secret GitHub
5. **Fetch ERA5** – Script `fetch_era5.py` (Open-Meteo)
6. **Fetch Odissé** – Script `fetch_odisse.py` (Santé publique)
7. **dbt run** – Transformations (`dbt run --target ci`)
8. **dbt test** – Validation qualité
9. **Log completion** – Confirmation exécution

## Collection de données

### Open-Meteo ERA5

**Fréquence**: Quotidienne à 6h UTC

**Script**: `scripts/fetch_era5.py`

**Fenêtre glissante**:
```python
window_start = today - 15 days
window_end = today - 5 days
```

**Limitation d'API**:
- Lot de 20 communes par requête
- Pause 15s entre lots
- Retry jusqu'à 4 fois (30s, 60s, 90s, 120s)

### Odissé (Santé publique)

**Status**: En cours d'implémentation

**5 jeux de données**:
- Jours de canicule par département
- Sévérité
- Population exposée
- Décès attribuables
- Excès de décès observés

**Maille**: Département-Année (pas commune-jour)

## Transformations dbt

### Performance

| Étape | Durée |
|-------|-------|
| Parse | ~1s |
| Compile | ~2s |
| Run | ~40s |
| Test | ~8s |
| **Total** | **~50s** |

### Tags

```bash
# Par tag
dbt run --select tag:staging       # stg_*
dbt run --select tag:intermediate  # int_*
dbt run --select tag:mart          # dim_*, fct_*
dbt run --select tag:weather       # ERA5
```

## Déduplication

### Clé de hachage
```
sha256(date + commune_name)
```

### Logique
1. Récupère hachages existants (fenêtre -15 à -5)
2. Calcule hachages des nouvelles lignes
3. Insère seulement si hash ∉ BigQuery
4. Cleanup: ROW_NUMBER() en cas de doublon

### Vérification quotidienne

```sql
SELECT COUNT(DISTINCT code_insee) as communes
FROM fct_daily_heat_health
WHERE DATE(date) = CURRENT_DATE() - 5
-- Doit retourner: 360
```

## Monitoring

### Logs
```bash
# GitHub Actions
# https://github.com/athevenet300882/lesfourcasters/actions

# BigQuery
bq query "SELECT * FROM lesfourcasters_raw.raw_open_meteo LIMIT 10"
```

### Alertes
GitHub envoie email si:
- Job fails
- Workflow cancelled
- Timeout (>30 min)

## Historique des exécutions

### Query

```sql
SELECT
  CAST(inserted_at AS DATE) as execution_date,
  COUNT(*) as rows_inserted,
  MIN(date) as min_date,
  MAX(date) as max_date,
  COUNT(DISTINCT code_insee) as communes
FROM `newfourcasters.lesfourcasters_dbt.fct_daily_heat_health`
GROUP BY execution_date
ORDER BY execution_date DESC
LIMIT 30
```

### Expected pattern

- 360 lignes/jour (une par commune)
- Sliding window -15 to -5 = deduplicate ~3,240 rows
- Net: +360 rows/day