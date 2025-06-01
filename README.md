# IMDb End-to-End Data Pipeline

[![Project Status](https://img.shields.io/badge/Status-Active%20Development-green)]()
[![Docker](https://img.shields.io/badge/Docker-Compose-blue)]()
[![Airflow](https://img.shields.io/badge/Airflow-2.10.5-orange)]()
[![dbt](https://img.shields.io/badge/dbt-1.8.7-purple)]()
[![Testing](https://img.shields.io/badge/Testing-pytest-red)]()

## 1. Overview

This project implements a **production-ready ETL pipeline** that automates the download, ingestion, and transformation of IMDb datasets into analytics-ready models. Built with **Apache Airflow**, **PostgreSQL**, and **dbt**, it demonstrates modern data engineering practices including containerization, automated testing, and comprehensive monitoring.

### Key Features
- Automated Daily ETL with smart incremental loading
- Fully Dockerized development environment
- Complete dbt Transformation Layer (staging → intermediate → marts)
- Comprehensive Testing Suite with pytest and dbt tests
- Role-based Access Control with custom security manager
- Production-ready Architecture with proper logging and monitoring

> **Project Goals:** Demonstrate end-to-end data engineering skills including orchestration, bulk data processing, dimensional modeling, and automated testing in a cloud-ready architecture.

## 2. Pipeline Architecture

```plaintext
+----------------+       +------------------------+       +-----------------------+       +---------------------+
| IMDb Public    |       | Airflow DAGs (Python)  |       | PostgreSQL (raw data) |       | dbt (staging/models)|
| .tsv.gz Files  |  -->  | download + load tasks  |  -->  | imdb_* tables created |  -->  | transformations     |
+----------------+       +------------------------+       +-----------------------+       +---------------------+
        |                        |                              |                              |
        v                        v                              v                              v
Downloaded daily      Tables created via SQL        Data loaded using COPY         Transformations & cleaning
to `/data/files/`     from `/schemas/*.sql`         or executemany fallback        with sources, staging, models
```

## 3. Project Status & Roadmap

### COMPLETED (75% Complete)

| Component | Status | Details |
|-----------|--------|---------|
| **Infrastructure** | Complete | Docker Compose with Airflow, PostgreSQL, isolated services |
| **Data Ingestion** | Complete | Airflow DAG with smart incremental loading via `Last-Modified` headers |
| **Database Layer** | Complete | PostgreSQL with optimized `COPY` bulk loading and schema management |
| **Transformation Layer** | Complete | Full dbt project with 7 staging, 5 intermediate, and 4 mart models |
| **Testing Framework** | Partial | pytest for Airflow functions (31% coverage), dbt testing configured |
| **Documentation** | Complete | API docs, schema documentation, and model lineage |
| **Security** | Complete | Custom role-based access control and DAG permissions |

### IN PROGRESS (Sprint 4)

| Component | Status | Next Steps |
|-----------|--------|------------|
| **Unit Testing** | 🔄 Expanding | Adding more Airflow function tests and edge case coverage |
| **Integration Testing** | 📋 Planned | End-to-end pipeline testing with test datasets |
| **CI/CD Pipeline** | 📋 Planned | GitHub Actions for automated testing and deployment |

### 🎯 **PLANNED** (Sprint 5+)

| Component | Priority | Description |
|-----------|----------|-------------|
| **Cloud Deployment** | High | AWS/GCP deployment with managed services |
| **Data Quality Monitoring** | High | Great Expectations integration for data validation |
| **Dashboard Integration** | Medium | Grafana/Superset for analytics visualization |
| **Performance Optimization** | Medium | Query optimization and incremental dbt models |

## 4. Technical Implementation Status

### **Airflow ETL Pipeline** ✅ **PRODUCTION READY**
- **DAG**: `imdb_download_dag` - Orchestrates complete ETL workflow
- **Smart Downloads**: HTTP `Last-Modified` header checking for incremental updates
- **Bulk Loading**: PostgreSQL `COPY` command for optimal performance (7M+ rows in seconds)
- **Error Handling**: Comprehensive exception handling with detailed logging
- **Data Validation**: Row count verification and data integrity checks
- **Idempotency**: `TRUNCATE` and reload strategy ensures consistent state

### **Database Architecture** ✅ **OPTIMIZED**
- **Schema Management**: Version-controlled SQL schemas in `/schemas/` directory
- **Raw Data Tables**: 7 IMDb tables with proper data types and constraints
- **Connection Pooling**: Optimized PostgreSQL configuration for concurrent access
- **Security**: Role-based access with dedicated application user

### **dbt Transformation Layer** ✅ **COMPREHENSIVE**
- **Sources Layer**: 7 source definitions with freshness tests and documentation
- **Staging Layer**: Complete data cleaning and normalization (7 models)
  - Snake_case standardization, type casting, array parsing
  - Boolean conversion, null handling, data quality filters
- **Intermediate Layer**: Business logic and complex joins (5 models)
  - `int_title_with_ratings`, `int_title_with_genres`, `int_person_filmography`
  - `int_title_hierarchies`, `int_title_complete`
- **Marts Layer**: Analytics-ready aggregated tables (4 models)
  - `mart_top_titles`, `mart_genre_analytics`, `mart_person_career`, `mart_series_analytics`

### **Testing & Quality Assurance** ✅ **IMPLEMENTED**
- **Unit Tests**: pytest framework testing Airflow functions with mocking
  - Function isolation testing, error condition handling, SQL injection prevention
- **dbt Tests**: Comprehensive data quality tests
  - `not_null`, `unique`, `accepted_values`, `relationships`
  - Custom tests for data integrity and business logic validation
- **Coverage**: 31% code coverage with expanding test suite

## 5. Technology Stack & Architecture

### **Core Technologies**
- **🐳 Containerization**: Docker 24.0.x + Docker Compose 2.20.x
- **🌊 Orchestration**: Apache Airflow 2.10.5 (apache/airflow:latest-python3.8)
- **🗄️ Database**: PostgreSQL 12.x with optimized configuration
- **🔧 Transformation**: dbt-core 1.8.7 + dbt-postgres adapter
- **🐍 Runtime**: Python 3.8.x with production dependencies

### **Python Dependencies**
- **Data Processing**: `pandas`, `psycopg2-binary`, `numpy`
- **HTTP/API**: `requests` for IMDb data fetching
- **Cloud Ready**: `boto3` for future AWS integration
- **Testing**: `pytest`, `pytest-cov` for comprehensive testing
- **Development**: Full dev environment with debugging capabilities

### **Infrastructure Architecture**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Airflow Web   │    │  Airflow Sched. │    │   PostgreSQL    │
│   (Port 8080)   │    │   (Background)  │    │   (Port 5434)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
         ┌─────────────────────────────────────────────────┐
         │              Docker Network                     │
         │          (Isolated Services)                    │
         └─────────────────────────────────────────────────┘
```

## 6. Quick Start Guide

### **Prerequisites**
- Docker Desktop installed and running
- At least 4GB RAM available for containers
- Git for repository management

### **Environment Setup**
```bash
# 1. Clone and navigate to project
git clone <repository-url>
cd apache-airflow-dev-server

# 2. Start the complete environment
docker compose up --build

# 3. Wait for services to initialize (~2-3 minutes)
# Watch logs for "Airflow is ready" message
```

### **Access Points**
| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | `admin` / `admin` |
| **PostgreSQL** | `localhost:5434` | `airflow` / `airflow` |
| **Database Name** | `airflow` | Direct SQL access available |

### **Pipeline Execution**
1. **Enable DAG**: In Airflow UI, toggle `imdb_download_dag` to ON
2. **Monitor Progress**: Watch task execution in Graph/Grid view
3. **Verify Data**: Check PostgreSQL tables populated with IMDb data
4. **Run dbt**: Execute transformations for analytics models

### **dbt Commands** (Inside Container)
```bash
# Access dbt environment
docker compose exec webserver bash
cd /opt/airflow/dbt

# Run complete transformation pipeline
dbt run --profiles-dir .

# Execute data quality tests
dbt test --profiles-dir .

# Generate and serve documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8081
```

### **Testing Framework**
```bash
# Run unit tests for Airflow functions
pytest tests/unit/ -v

# Run with coverage reporting
pytest tests/ --cov=project_airflow --cov-report=term-missing -v
```

## 7. Project Structure

```
apache-airflow-dev-server/               # 🏗️ Root project directory
├── 📋 pytest.ini                        # Testing configuration
├── 📄 docker-compose.yml                # Service orchestration
├── 🐳 Dockerfile                        # Custom Airflow image
├── 📊 requirements.txt                  # Python dependencies
├── 📚 README.md                         # Project documentation
├── 📖 SCHEMA.md                         # Database schema reference
├── 🗂️ data/files/                       # 📦 Downloaded IMDb datasets
├── 🔧 project_airflow/                  # 🚀 Airflow application
│   ├── ⚙️ config/auth/                  # Security and permissions
│   ├── 📈 dags/imdb_download_dag.py     # Main ETL pipeline
│   └── 🔨 scripts/airflow-entrypoint.sh # Container initialization
├── 🏗️ dbt/                              # 🎯 Data transformation layer
│   ├── 📁 models/staging/               # Raw data cleaning (7 models)
│   ├── 📁 models/intermediate/          # Business logic (5 models)
│   ├── 📁 models/marts/                 # Analytics tables (4 models)
│   ├── 📋 profiles.yml                  # Database connections
│   ├── ⚙️ dbt_project.yml              # Project configuration
│   └── 📊 target/                       # Compiled SQL and docs
├── 🗃️ schemas/                          # 📋 Table definitions (7 SQL files)
├── 🧪 tests/                            # 🔬 Testing framework
│   ├── 🧪 unit/test_schema_setup.py     # Airflow function tests
│   ├── ⚙️ conftest.py                   # pytest configuration
│   └── 📁 integration/ (planned)        # End-to-end tests
└── 🔧 scripts/                          # 🛠️ Utility scripts
    ├── 🔍 database_check.sql            # Database validation
    └── 📚 README.md                     # Scripts documentation
```

### **Key Components Breakdown**

| Directory | Purpose | Status | Files |
|-----------|---------|--------|-------|
| `project_airflow/dags/` | ETL orchestration | ✅ Complete | 1 production DAG |
| `dbt/models/staging/` | Data cleaning | ✅ Complete | 7 staging models |
| `dbt/models/intermediate/` | Business logic | ✅ Complete | 5 intermediate models |
| `dbt/models/marts/` | Analytics tables | ✅ Complete | 4 mart models |
| `schemas/` | Database schemas | ✅ Complete | 7 table definitions |
| `tests/unit/` | Unit testing | 🔄 Expanding | 3 tests (31% coverage) |
| `tests/integration/` | E2E testing | 📋 Planned | Future development |

## 8. Key Technical Features

### Efficient IMDb Data Loading
- **Incremental Downloads**: Uses HTTP header `Last-Modified` to only download changed files
- **Bulk Loading**: PostgreSQL `COPY` command for optimal performance
- **Error Handling**: Detailed logging and validation of row counts
- **Data Cleaning**: Transformations in dbt staging models

### Custom Security
- Role-based authorization for DAG access
- Configurable through JSON permissions file
- Inheritance from AirflowSecurityManager

### Data Transformation
- Normalized nested arrays (directors, writers, genres)
- Data type conversions in staging models
- Boolean conversion from text ('0'/'1')

## 9. Next Steps

- [x] Add intermediate and mart layer dbt models for analytics
- [ ] Implement data quality checks and monitoring
- [ ] Add dashboard integration with Superset or Metabase
- [ ] Deploy to cloud environment (AWS/GCP/Azure)
- [ ] Add CI/CD pipeline for testing and deployment

## 10. Database Schema

For detailed information about the IMDb database schema, please refer to [SCHEMA.md](SCHEMA.md).

### Key Tables Overview

| Table                | Description           | Primary Key          | Notable Columns                  |
|----------------------|----------------------|-----------------------|----------------------------------|
| imdb_title_basics    | Core title data      | tconst                | titleType, primaryTitle, startYear |
| imdb_title_ratings   | User ratings         | tconst                | averageRating, numVotes          |
| imdb_name_basics     | Person information   | nconst                | primaryName, birthYear, knownForTitles |
| imdb_title_crew      | Directors and writers| tconst                | directors, writers               |
| imdb_title_principals| Cast and crew        | tconst + ordering     | nconst, category, job            |
| imdb_title_akas      | Alternative titles   | titleId + ordering    | title, region, language          |
| imdb_title_episode   | TV episode data      | tconst                | parentTconst, seasonNumber, episodeNumber |

### Data Relationships
- **Movies & TV Shows**: Stored in `imdb_title_basics`
- **People**: Actors, directors, writers in `imdb_name_basics`
- **Connections**: `imdb_title_principals` links people to titles
- **TV Structure**: `imdb_title_episode` links episodes to series

## 11. Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 12. License

This project is licensed under the MIT License - see the LICENSE file for details.