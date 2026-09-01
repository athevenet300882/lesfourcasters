# Contributing Guide

Merci de contribuer à lesfourcasters!

## Avant de commencer

1. Lis **ARCHITECTURE.md** pour comprendre la structure
2. Lis **SETUP.md** pour configurer ton environnement
3. Familiarise-toi avec **DATA_DICTIONARY.md**

## Workflow Git

### 1. Fork et clone

```bash
git clone https://github.com/TON_USERNAME/lesfourcasters.git
cd lesfourcasters
git remote add upstream https://github.com/athevenet300882/lesfourcasters.git
```

### 2. Crée une feature branch

```bash
git fetch upstream
git checkout -b feature/ma-feature upstream/main

# Exemples:
# feature/optimize-era5
# feature/add-odisse-integration
# fix/dedup-communes
# docs/add-api-guide
```

### 3. Fais tes changements

```bash
dbt run
dbt test

git add .
git commit -m "feat: description claire"
```

**Convention commits**:
- `feat:` Nouvelle feature
- `fix:` Correction de bug
- `docs:` Documentation
- `refactor:` Code refactor
- `test:` Ajout tests
- `perf:` Optimisation performance
- `chore:` Maintenance

### 4. Push et crée Pull Request

```bash
git push origin feature/ma-feature
```

Puis sur GitHub: "New Pull Request"

**Description PR**:
- Quoi? (résumé)
- Pourquoi? (contexte)
- Comment? (approche)
- Avant/après? (si applicable)

### 5. Review et merge

- Maintainer review ta PR
- Apporte corrections si demandé
- Merge après approbation

---

## Standards de code

### SQL (dbt models)

✅ **Bon**:
```sql
{{ config(
    materialized = 'table',
    tags = ['staging', 'weather']
) }}

SELECT
  time AS date,
  temperature_2m_mean AS temperature,
  TRIM(IFNULL(Ville, nom_poi)) AS ville,
  CURRENT_TIMESTAMP() AS inserted_at
FROM {{ source('raw', 'raw_open_meteo') }}
WHERE time IS NOT NULL
  AND temperature_2m_mean IS NOT NULL
```

❌ **Mauvais**:
```sql
select time, temperature_2m_mean, Ville
from raw_open_meteo
where temperature_2m_mean is not null
```

**Rules**:
- Utilise `{{ ref() }}` et `{{ source() }}`
- Renomme colonnes en français
- Ajoute `config()` avec tags
- Commente logiques complexes
- Ligne max: 100 caractères

### Python

✅ **Bon**:
```python
def fetch_era5_data(start_date: date, end_date: date) -> dict:
    """Fetch ERA5 data from Open-Meteo API."""
    logger.info(f"Fetching {start_date} to {end_date}")
    
    try:
        return api.fetch(...)
    except RequestException as e:
        logger.error(f"API failed: {e}")
        raise
```

**Rules**:
- Type hints obligatoires
- Docstrings pour chaque fonction
- Logging (pas print)
- Gestion erreurs explicite

### Markdown

**Rules**:
- Titres hiérarchiques (#, ##, ###)
- Code blocks avec language
- Listes numérotées pour étapes
- Links actifs

---

## Tests obligatoires

### Avant commit

```bash
dbt test
dbt run
```

### Avant PR

```bash
dbt run
dbt test
dbt docs generate

# Vérifier data quality
bq query "SELECT COUNT(*) FROM fct_daily_heat_health"
```

---

## Code of Conduct

- Sois respectueux
- Accepte critiques constructives
- Focus sur qualité du code
- Aide les autres à apprendre