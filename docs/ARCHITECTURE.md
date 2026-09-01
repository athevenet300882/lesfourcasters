# Architecture

## Vue d'ensemble

lesfourcasters analyse l'impact des vagues de chaleur sur la santé publique en France.

## Infrastructure GCP

| Composant | Détail |
|-----------|--------|
| Projet | newfourcasters (ID: 795647081240) |
| Datasets | lesfourcasters_raw, lesfourcasters_dbt |
| Service Account | github-actions-697@newfourcasters.iam |

## Pipeline ETL

Sources → Staging → Intermediate → Mart → BigQuery
