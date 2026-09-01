# Green IT & Optimizations

## Impact environnemental actuel

### Empreinte carbone

| Métrique | Valeur |
|----------|--------|
| Par exécution | ~0.15 kg CO2 |
| Par jour | ~0.15 kg CO2 |
| Par mois | ~4.5 kg CO2 |
| Par an | ~55 kg CO2 |
| Équivalent km voiture | ~550 km/an |

### Variables collectées vs utilisées

- **Collectées**: 29 variables ERA5
- **Utilisées**: 6 (temperature, humidité, précipitations, localisation)
- **Gaspillage**: 69% des colonnes inutiles

### Infrastructure traitement

- BigQuery: ~40s compute/jour
- Cloud Storage: 3 buckets, ~50GB
- GitHub Actions: ~50s job time/jour

---

## Gaspillages identifiés

### 1. Colonnes inutiles (72% d'overhead)

**Problème**: `stg_open_meteo` collecte 29 variables, n'utilise que 6

**Impact**: Traitement 23 colonnes superflues

**Gain potentiel**: Réduire à 6 colonnes = **-72% volume**

### 2. Dark Data

**Backfill temporaires**: Fichiers d'export d'exploration non supprimés

**À nettoyer**:
```bash
# Cloud Storage
gsutil rm -r gs://newfourcasters-staging/backfill_*

# BigQuery
bq rm newfourcasters:lesfourcasters_raw.temp_*
```

**Gain**: ~280 MB

### 3. Matérialisation inefficace

**Problème**: dbt reconstruit 3.5M lignes à chaque exécution

**Volume traité**: ~900 MB quotidien

**Gain potentiel**: Mode incremental = **-97% volume**

---

## Checklist d'optimisations (5 phases)

### Phase 0: Setup monitoring (30 min)
- [ ] Installer CodeCarbon: `pip install codecarbon`
- [ ] Lancer script: `python3 scripts/mesures_emissions.py`
- [ ] Créer table BigQuery: `emissions_log`
- [ ] Configurer alertes seuil CO2

### Phase 1: Nettoyage (30 min)
- [ ] Supprimer backfill: `gsutil rm backfill_*/`
- [ ] Supprimer exports: `bq rm temp_*`
- [ ] Auditer Cloud Storage: `gsutil du -s`
- **Gain**: ~280 MB stockage

### Phase 2: Optimisation SQL (2h)
- [ ] Réduire colonnes stg_open_meteo (29→6)
- [ ] Supprimer intermediate si redondante
- **Gain**: -20-30% compute dbt

### Phase 3: Mode incremental (2h)
- [ ] Passer à `materialized = 'incremental'`
- [ ] Ajouter `unique_key` et filtres
- **Gain**: -97% volume traité = **-40% CO2/jour**

### Phase 4: Tests validation (1h)
- [ ] Vérifier intégrité données
- [ ] Comparer résultats avant/après
- [ ] Relancer mesures_emissions.py
- [ ] Logger baseline

### Phase 5: Documentation (1h)
- [ ] Mettre à jour ce doc
- [ ] Documenter dans CHANGELOG.md
- [ ] Ajouter notes dans modèles dbt
- [ ] Créer rapport gains

---

## Monitoring continu

### Métriques quotidiennes

```sql
SELECT
  CAST(inserted_at AS DATE) as day,
  COUNT(*) as rows_processed,
  SUM(bytes_billed) / 1024 / 1024 / 1024 as gb_billed
FROM `project.region.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE job_type = 'QUERY'
  AND creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY day
ORDER BY day DESC
```

### Mesurer CO2

```bash
python3 scripts/mesures_emissions.py
```

Résultats dans `pipeline_emissions_YYYYMMDD_HHMMSS.json`

---

## Estimations de gain

### Avant optimisation
- CO2/jour: 0.15 kg
- Volume compute: 900 MB
- Stockage: 50 GB

### Après Phase 3 (Mode incremental)
- CO2/jour: 0.09 kg (40% reduction)
- Volume compute: 27 MB (-97%)
- Stockage: 50 GB (inchangé)

### ROI
- Temps implémentation: 5-6h
- Gain CO2/an: ~22 kg
- Économies BigQuery: ~15 EUR/mois