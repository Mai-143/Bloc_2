# JobTech Data Lake – Bloc 2

## Objectif du projet

Ce projet met en place une architecture Data Engineering complète permettant de collecter, traiter, stocker et exposer des données liées au marché de l’emploi tech.

Le projet combine deux types de traitements :

- un pipeline batch pour les données historiques ;
- un pipeline streaming pour les événements GitHub en temps quasi réel.

## Architecture

Sources batch
Adzuna / StackOverflow / GitHub Repositories
        ↓
Bronze Layer
        ↓
Spark Batch Processing
        ↓
Silver Layer
        ↓
Gold Layer
        ↓
PostgreSQL
        ↓
FastAPI

Source streaming
GitHub Events API
        ↓
Kafka Producer
        ↓
Kafka Topic github_events
        ↓
Spark Structured Streaming
        ↓
Silver Streaming
        ↓
PostgreSQL
        ↓
FastAPI