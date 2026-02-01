# ☁️ Enterprise Cloud Migration

**Zero-downtime migration of legacy SQL Server data warehouse to Azure Synapse Analytics using Terraform IaC. Achieved 60% cost reduction and 3x performance improvement.**

![Azure](https://img.shields.io/badge/Azure-Synapse%20Analytics-blue.svg)
![Terraform](https://img.shields.io/badge/Terraform-IaC-purple.svg)
![Python](https://img.shields.io/badge/Python-3.9-yellow.svg)
![Synapse](https://img.shields.io/badge/Synapse-Dedicated%20SQL-orange.svg)

## 🎯 Project Overview

Migrated 10TB+ enterprise data warehouse from on-premises SQL Server 2016 to Azure Synapse Analytics, implementing modern data lakehouse architecture with Bronze-Silver-Gold medallion pattern.

**Key Achievements:**
- 60% infrastructure cost reduction vs on-prem maintenance
- 99.9% uptime during migration (zero business disruption)
- 3x faster query performance with columnstore indexing
- 100% automated infrastructure deployment via Terraform

## 🏗️ Architecture

**Legacy Stack:**
- SQL Server 2016 on Windows Server
- SSIS packages for ETL
- Limited scalability, high maintenance costs

**Modern Cloud Stack:**
- Azure Synapse Analytics (Dedicated SQL Pools)
- Azure Data Lake Gen2 (Bronze/Silver/Gold layers)
- Azure Data Factory (orchestration)
- Terraform (Infrastructure as Code)
