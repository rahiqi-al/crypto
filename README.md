# Pipeline d'Analyse du Marché des Cryptomonnaies

## Objectifs

Ce projet vise à construire un pipeline ETL pour collecter, stocker, transformer et analyser les données de prix et de volume des cryptomonnaies.

### Étapes du pipeline :
1. **Ingestion** : Récupération quotidienne des données .
2. **Stockage** : Enregistrement des données brutes dans HDFS.
3. **Transformation & Agrégation** :
   - Nettoyage des données.
   - Calcul des métriques (prix moyen, prix min/max, volume moyen, etc.).
4. **Chargement** : Insertion des données transformées dans HBase pour des requêtes rapides.
5. **Orchestration** : Utilisation d'Apache Airflow pour automatiser le pipeline.

## Architecture
```
               ┌───────────────────────┐
               │         yfinance      │
               └───────────┬───────────┘
                           │
           ┌───────────────▼──────────────┐
           │  Ingestion (Airflow + HDFS)  │
           └───────────────┬──────────────┘
                           │
                      Zone Brute (HDFS)
                           ▼
           ┌───────────────▼──────────────┐
           │Traitement(MapReduce+Airflow) │
           └───────────────┬──────────────┘
                           │
                     Zone Traité (HDFS)
                            ▼
               ┌────────────────────────┐
               │         HBase          │
               └────────────────────────┘
                            │
                            ▼
                      Analytics / BI
```

## Technologies Utilisées
- **Sources de données** : yfinance
- **Stockage** : HDFS
- **Traitement** : MapReduce (Python)
- **Base de données** : HBase
- **Orchestration** : Apache Airflow

## Installation & Exécution
1. Installer Hadoop, HBase et Airflow.
2. Configurer Airflow et ajouter les DAGs.
3. Exécuter le pipeline via Airflow.

## Auteur
Ali Rahiqi
