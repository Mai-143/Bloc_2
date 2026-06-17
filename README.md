# JobTech Data Lake – Traitement Massif de Données avec Spark, Kafka et PostgreSQL

## Présentation du projet

JobTech Data Lake est une plateforme de traitement de données massives permettant de collecter, transformer, stocker et exposer des données provenant de plusieurs sources liées au marché de l'emploi et à l'écosystème du développement logiciel.

Le projet met en œuvre une architecture Big Data complète reposant sur Apache Spark, Apache Kafka, PostgreSQL, FastAPI et Docker afin de répondre aux problématiques de traitement batch et quasi temps réel.

---

## Objectifs

* Collecter des données provenant de plusieurs sources.
* Concevoir une architecture Data Lake multicouche.
* Réaliser des traitements massifs avec Apache Spark.
* Mettre en œuvre un pipeline de streaming avec Kafka et Spark Structured Streaming.
* Construire un Data Warehouse décisionnel.
* Exposer les données via une API REST.
* Conteneuriser l'ensemble de la solution.
* Mettre en place des tests automatisés.

---

## Sources de données

### Adzuna API

Collecte d'offres d'emploi Data Engineer, Data Analyst et Data Scientist.

Données récupérées :

* titre du poste
* entreprise
* localisation
* salaire
* catégorie

### GitHub API

Collecte de données sur les dépôts populaires :

* nom du dépôt
* langage principal
* nombre de stars
* activité

### StackOverflow Survey

Analyse des données publiques du questionnaire annuel StackOverflow :

* pays
* technologies utilisées
* expérience
* salaires

### GitHub Events API (Streaming)

Collecte en quasi temps réel des événements publics GitHub :

* PushEvent
* CreateEvent
* DeleteEvent
* PullRequestEvent
* IssuesEvent

---

# Architecture générale

```text
                     ┌───────────────┐
                     │ Adzuna API    │
                     └───────┬───────┘
                             │
                     ┌───────▼───────┐
                     │ GitHub API    │
                     └───────┬───────┘
                             │
                     ┌───────▼──────────┐
                     │ StackOverflow    │
                     └───────┬──────────┘
                             │
                             ▼

                      BRONZE LAYER
                 (Données brutes collectées)

                             ▼

                      SILVER LAYER
            (Nettoyage et standardisation Spark)

                             ▼

                       GOLD LAYER
                (Agrégations et indicateurs)

                             ▼

                      POSTGRESQL
                    (Data Warehouse)

                             ▼

                         FASTAPI
                   (Exposition REST)
```

---

# Architecture Streaming

```text
GitHub Events API
        │
        ▼
Kafka Producer
        │
        ▼
Kafka Topic (github_events)
        │
        ▼
Spark Structured Streaming
        │
        ▼
Silver Streaming Layer
        │
        ▼
PostgreSQL
        │
        ▼
FastAPI
```

---

# Structure du projet

```text
Bloc_2/
│
├── api/
│   └── main.py
│
├── kafka/
│   └── producer.py
│
├── spark/
│   ├── common/
│   └── jobs/
│       ├── 01_bronze_to_silver.py
│       ├── 02_silver_to_gold.py
│       ├── 03_load_to_postgres.py
│       └── 04_github_events_streaming.py
│
├── warehouse/
│   └── schema.sql
│
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── tests/
│
├── docker-compose.yml
│
└── requirements.txt
```

---

# Data Lake

## Bronze Layer

Stockage des données brutes collectées.

Objectifs :

* historisation
* traçabilité
* conservation des données d'origine

## Silver Layer

Transformation avec Apache Spark :

* suppression des doublons
* gestion des valeurs manquantes
* normalisation des salaires
* harmonisation des localisations
* standardisation des catégories

## Gold Layer

Création d'indicateurs métier :

* salaires moyens par catégorie
* répartition géographique des offres
* popularité des langages GitHub
* salaire moyen par pays

---

# Data Warehouse

## Tables de dimensions

* dim_company
* dim_category
* dim_location
* dim_source

## Table de faits

* fact_jobs

## Tables analytiques

* gold_salary_by_category
* gold_jobs_by_location
* gold_developer_salary_by_country
* gold_github_language_popularity

## Tables streaming

* github_events_stream
* github_event_type_stats
* github_repo_activity

---

# API REST

Documentation Swagger :

```text
http://localhost:8000/docs
```

Documentation ReDoc :

```text
http://localhost:8000/redoc
```

Principaux endpoints :

### Jobs

```text
GET /jobs
```

### Analytics

```text
GET /analytics/jobs-by-location
GET /analytics/salary-by-category
GET /analytics/github-languages
GET /analytics/developer-salary-by-country
```

### Streaming

```text
GET /streaming/github/events
GET /streaming/github/event-types
GET /streaming/github/repos
```

### Data Warehouse

```text
GET /warehouse/companies
GET /warehouse/locations
GET /warehouse/categories
GET /warehouse/sources
```

---

# Déploiement

## Démarrage

```bash
docker compose up -d
```

## Vérification

```bash
docker ps
```

## API

```bash
http://localhost:8000/docs
```

---

# Tests

Exécution des tests :

```bash
docker exec -it jobtech_api pytest -v
```

Résultat attendu :

```text
10 passed
```

---

# Technologies utilisées

* Python
* Apache Spark
* Spark Structured Streaming
* Apache Kafka
* PostgreSQL
* FastAPI
* Docker
* Pytest
* GitHub Actions

---

# Résultats obtenus

* Architecture Data Lake complète Bronze / Silver / Gold.
* Traitement batch distribué avec Apache Spark.
* Intégration d'un pipeline Kafka pour le streaming.
* Construction d'un Data Warehouse décisionnel.
* Exposition des données via une API REST documentée.
* Déploiement conteneurisé avec Docker.
* Validation automatique par tests unitaires.

---

# Perspectives d'amélioration

* Intégration d'Airflow pour l'orchestration.
* Mise en place d'un monitoring avec Prometheus et Grafana.
* Déploiement sur Kubernetes.
* Création de dashboards Power BI connectés au Data Warehouse.
* Mise en œuvre d'une architecture Lakehouse avec Delta Lake.

```
```
