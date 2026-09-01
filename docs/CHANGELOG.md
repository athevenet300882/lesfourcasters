# Changelog

Format basé sur [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added
- CodeCarbon emissions tracking script
- Green IT audit and optimization checklist
- Complete documentation suite
- README.md with full architecture

### Changed
- Updated profiles.yml with dev/ci targets

### Fixed
- Deduplication logic for sliding window

---

## [v0.1.0] – 2026-09-01

### Added
- Initial project setup with GCP newfourcasters
- dbt models: 4 staging, 1 intermediate, 2 mart
  - stg_open_meteo (29 ERA5 variables, 3.5M rows)
  - stg_communes (360 communes reference)
  - stg_odisse_canicule (health data)
  - stg_odisse_hivernales (out of scope)
  - int_health_weather_join (double join + dedup)
  - dim_commune (dimension table, 360 rows)
  - fct_daily_heat_health (fact table, 3.5M rows)
- GitHub Actions daily workflow (6h UTC)
- Python scripts
  - fetch_era5.py (Open-Meteo API)
  - backfill_era5.py (historical data)
  - mesures_emissions.py (CodeCarbon tracking)
- BigQuery infrastructure
  - lesfourcasters_raw dataset (sources)
  - lesfourcasters_dbt dataset (transformations)
  - 3 Cloud Storage buckets
- dbt tests (not_null, unique)
- Complete documentation
  - cahier des charges
  - risk register
  - EDA notebook

### Fixed
- 1.2M missing departments (format incompatibility → LPAD CAST)
- 3M duplicate rows from cartesian product → ROW_NUMBER()
- Deduplication using float coordinates → date + commune key
- Two communes with divergent orthography → NORMALIZE REGEXP_REPLACE
- 25 empty columns from partial API → extended to 29 variables
- API timeouts from GitHub runners → batch requests + retry

### Performance
- dbt run: 40 seconds
- dbt test: 8 seconds
- Total pipeline: ~50s
- Data quality: 0 nulls, 0 duplicates
- 9,735 days without gaps

---

## Historical Milestones

### 28 August 2026
- Rebuilt infrastructure from scratch (project → newfourcasters)
- Created 7 dbt models
- Resolved 1.2M missing department values
- Fixed 3M duplicate rows

### 29 August 2026
- Workflow first autonomous execution
- 360 rows inserted (one per commune)
- Deduplication validated

### 30 August 2026
- Second autonomous workflow run
- Data quality audit passed

### 31 August 2026
- Documentation complete
- Cahier des charges + risk register
- EDA notebook
- Added INSEE codes and regions

### 1 September 2026
- Green IT audit integrated
- CodeCarbon emissions tracking
- Optimization checklist created
- Complete documentation suite
- All codes reference PDF

---

## Known Issues

### Limitation d'API
- Open-Meteo applique rate-limiting depuis GitHub
- **Workaround**: Batch requests (20 communes/call), pause 15s
- **Status**: En production, fonctionne correctement

### Maille Odissé
- Données sanitaires à maille département-année (pas commune)
- **Impact**: Croisement météo-santé limité
- **Status**: Design décision

### Validation année en cours
- Indicateur sanitaire 2025 (dernière année complète)
- Météo 2026 (données courantes)
- **Status**: Expected

### Mode matérialisation inefficace
- dbt reconstruit 3.5M lignes/jour
- **Optimisation**: Mode incremental (-97% volume)
- **Status**: Phase 3, identifié dans GREEN_IT.md

---

## Versioning

Nous utilisons [Semantic Versioning](https://semver.org/):
- **MAJOR**: Changements incompatibles (restructure données)
- **MINOR**: Nouvelles features (modèles, APIs)
- **PATCH**: Bug fixes (requêtes SQL, formules)

---

## Release Process

1. Update CHANGELOG.md
2. Tag: `git tag v0.1.0`
3. Push: `git push origin main --tags`
4. Create GitHub Release