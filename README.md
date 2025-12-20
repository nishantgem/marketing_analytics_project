# marketing_analytics_ecommerce_project
Marketing data modeling and integration
📌 Overview
This repository contains an end‑to‑end marketing analytics workflow, including data ingestion, transformation, modeling, and visualization. The goal is to build a scalable analytics foundation for understanding marketing performance, customer acquisition efficiency, and long‑term customer value.

The project includes:

Automated data pipelines to ingest and integrate marketing data into a data warehouse

Cleaned and transformed data models for analysis

Trend analysis and forecasting

CAC (Customer Acquisition Cost) analysis

LTV (Customer Lifetime Value) modeling

Reusable SQL/dbt models for analytics engineering

Python notebooks for exploratory analysis and forecasting



marketing_project/
└── models/
    ├── staging/
    │   └── social/
    │       ├── sources.yml
    │       ├── stg_pinterest.sql
    │       ├── stg_reddit.sql
    │       ├── stg_meta_mx.sql
    │       └── stg_meta_non_mx.sql
    │
    ├── prep/
    │   └── prep_unified_social_metrics.sql
    │
    └── mart/
        └── fact_marketing_performance.sql


