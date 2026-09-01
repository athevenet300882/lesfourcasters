# Data Dictionary

## Fact Table: fct_daily_heat_health

### Vue d'ensemble
- **Grain**: Commune-Jour
- **Lignes**: 3,503,520 (360 communes × 9,735 jours)
- **Couverture**: 1er janvier 2000 → 26 août 2026
- **Mise à jour**: Quotidienne (6h UTC)

### Colonnes

| Colonne | Type | Nullable | Description | Exemple |
|---------|------|----------|-------------|---------|
| **fact_id** | STRING | NO | UUID unique | `a1b2c3d4-e5f6...` |
| **date** | DATE | NO | Date mesure | `2024-08-15` |
| **code_insee** | STRING | NO | Code INSEE (5 digits) | `75056` |
| **temperature** | FLOAT64 | NO | Température moyenne (°C) | `22.5` |
| **humidite** | FLOAT64 | NO | Humidité relative (%) | `65.0` |
| **precipitations** | FLOAT64 | NO | Précipitations totales (mm) | `0.0` |
| **deaths** | INT64 | YES | Décès attribuables chaleur | `3` |
| **hospitalizations** | INT64 | YES | Hospitalisations | `12` |
| **heat_wave_alert** | BOOLEAN | YES | Alerte vague chaleur | `TRUE` |
| **updated_at** | TIMESTAMP | NO | Timestamp insertion | `2024-08-16 02:15:30 UTC` |

## Dimension: dim_commune

### Vue d'ensemble
- **Grain**: Commune unique
- **Lignes**: 360
- **Clé primaire**: code_insee

### Colonnes

| Colonne | Type | Description | Exemple |
|---------|------|-------------|---------|
| **code_insee** | STRING | Code INSEE (5 digits, PK) | `75056` |
| **commune_name** | STRING | Nom commune | `Paris` |
| **department_code** | STRING | Code département (2-3 digits) | `75` |
| **department_name** | STRING | Nom département | `Île-de-France` |
| **region_code** | STRING | Code région (2 digits) | `11` |
| **region_name** | STRING | Nom région | `Île-de-France` |
| **latitude** | FLOAT64 | Latitude WGS84 | `48.8566` |
| **longitude** | FLOAT64 | Longitude WGS84 | `2.3522` |
| **sdis_code** | STRING | SDIS rattaché | `SDIS75` |

## Staging Tables

### stg_open_meteo (3.5M lignes)
Données brutes ERA5 nettoyées et renommées

| Colonne | Type | Description |
|---------|------|-------------|
| date | DATE | Date mesure |
| temperature | FLOAT64 | Température 2m moyenne (°C) |
| humidite | FLOAT64 | Humidité relative 2m (%) |
| precipitations | FLOAT64 | Somme précipitations (mm) |
| ville | STRING | Nom commune (normalisé) |
| numero_departement | STRING | Code département (2 chars, zero-padded) |
| latitude | FLOAT64 | Latitude WGS84 |
| longitude | FLOAT64 | Longitude WGS84 |
| inserted_at | TIMESTAMP | Timestamp insertion |

### stg_communes (360 lignes)
Référentiel communes

| Colonne | Type | Description |
|---------|------|-------------|
| code_insee | STRING | Code INSEE (5 digits) |
| commune_name | STRING | Nom commune |
| department_code | STRING | Code département |
| department_name | STRING | Nom département |
| region_code | STRING | Code région |
| region_name | STRING | Nom région |
| latitude | FLOAT64 | Latitude |
| longitude | FLOAT64 | Longitude |

## Formats & Standards

### Date & Time
- Format: ISO 8601 (YYYY-MM-DD)
- Timezone: UTC
- Exemple: `2024-08-15T14:30:00Z`

### Nombres
- **Température**: Celsius, 1 décimale (-30 à +45°C)
- **Humidité**: Pourcentage (0-100)
- **Précipitations**: Millimètres (≥ 0)

### Codes
- **INSEE**: 5 digits, zero-padded (`01234`, `75056`)
- **Département**: 2-3 digits, zero-padded (`75`, `971`)
- **Région**: 2 digits, zero-padded (`11`, `24`)

### Géographie (WGS84)
- **Latitude**: -90 à +90
- **Longitude**: -180 à +180
- **Précision**: 4 décimales (~11 km)

## Règles de qualité

### Valeurs manquantes
- Colonnes clés (fact_id, date, code_insee): **0 nulls**
- Colonnes météo: **0 nulls**
- Colonnes santé: COALESCE à 0 si manquant

### Doublons
- **Clé métier**: date + code_insee (unique)
- **Enforcement**: ROW_NUMBER() PARTITION BY
- **Vérification**: Quotidienne via GitHub Actions

### Bornes physiques
- Température: -30 à +45°C
- Humidité: 0 à 100%
- Précipitations: ≥ 0 mm
- Décès/Hospitalisations: ≥ 0

## Transformations clés

### Normalisation commune
```sql
REGEXP_REPLACE(NORMALIZE(LOWER(TRIM(ville)), NFD), r"[\\pM''−\\s]", '')
```
Retire accents, apostrophes, tirets, espaces
- Exemple: `Digne-les-Bains` → `dgnelsbains`

### Déduplication
```sql
ROW_NUMBER() OVER (PARTITION BY date, code_insee ORDER BY inserted_at DESC)
```
Garde la dernière insertion en cas de doublon

### Formatage département
```sql
LPAD(numero_departement, 2, '0')
```
Force 2 digits: `5` → `05`