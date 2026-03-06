# Medallion Architecture - Databricks Asset Bundle

Real-world medallion pipeline using Databricks Asset Bundles (DABs).

## Architecture
Bronze → Silver → Gold (fact + dim tables)

## Stack
- Databricks Free Edition
- Delta Lake
- PySpark
- GitHub Actions CI/CD

## CI/CD Flow
- Feature branch → PR to dev → tests run → approved → deploy to dev
- dev → PR to release → approved → deploy to prod
