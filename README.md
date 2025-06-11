# IMDb Analytics Pipeline

[![Airflow](https://img.shields.io/badge/Airflow-2.7-red)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.4.6-purple)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue)](https://www.postgresql.org/)
[![Status](https://img.shields.io/badge/Status-Development-yellow)]()

## 📊 Project Overview

This project implements a complete analytics pipeline for IMDb data using modern data engineering tools:

- **Apache Airflow**: Orchestrates the entire pipeline (data extraction and loading)
- **dbt (data build tool)**: Handles transformation of raw data into analytics models
- **Astronomer Cosmos**: Integrates dbt with Airflow for seamless orchestration
- **PostgreSQL**: Stores both raw data and transformed models

The pipeline follows a modern ELT (Extract, Load, Transform) architecture, using Airflow for data extraction and loading, and dbt for transformation.

## 🏗️ Architecture

```
┌────────────┐     ┌────────────┐     ┌───────────────┐     ┌───────────────┐
│   IMDb     │     │  Airflow   │     │   Postgres    │     │      dbt      │
│  Datasets  │ ──► │  Extract   │ ──► │  Raw Tables   │ ──► │Transformations│
│  (TSV.gz)  │     │  & Load    │     │  (stage)      │     │   (models)    │
└────────────┘     └────────────┘     └───────────────┘     └───────────────┘
                        │                                          ▲
                        │                                          │
                        │       ┌──────────────────┐              │
                        └─────► │ Cosmos Operator  │ ─────────────┘
                                │ (dbt integration)│
                                └──────────────────┘
                                          │
                        ┌─────────────────┘
                        ▼
┌────────────────────────────────────────┐
│           Analytics Layer              │
├────────────────┬───────────────────────┤
│  Fact Tables   │  Dimension Tables     │
└────────────────┴───────────────────────┘
```

## 🚀 Getting Started

### Prerequisites

- Docker and Docker Compose
- At least 4GB free disk space for IMDb datasets
- Git
- Astronomer CLI (`astro`) installed

### Setup Instructions

1. **Clone the repository**

```bash
git clone <repository-url>
cd apache-airflow-dev-server
```

2. **Create directories for IMDb data**

```bash
mkdir -p imdb_data imdb_schemas
```

3. **Start the environment**

```bash
astro dev start
```

This command will start:
- Airflow Webserver (UI) at http://localhost:8080/ (login with admin/admin)
- Airflow Scheduler
- PostgreSQL database (accessible at localhost:5432 with username/password: postgres/postgres)

   **Note on Port Conflicts:** If you encounter issues starting the environment due to port conflicts (e.g., port `8080` for the webserver or `5432` for PostgreSQL are already in use), you can configure Astro CLI to use different ports. Run the following commands in your Astro project directory *before* `astro dev start`:

   ```powershell
   astro config set webserver.port <available-port>
   astro config set postgres.port <available-port>
   ```

   For example, to use port `8081` for the webserver and `5435` for the database:

   ```powershell
   astro config set webserver.port 8081
   astro config set postgres.port 5435
   ```

   Then, run `astro dev start` again.

   **Note on dbt Manifest for Cosmos:** This project uses Astronomer Cosmos with `LoadMode.DBT_MANIFEST` for improved DAG parsing performance. This requires a `manifest.json` file to be present in the `dags/dbt/target/` directory. To generate or update this file, run the following commands from the root of the Astro project *before* starting Airflow or whenever you make changes to your dbt models:

   ```powershell
   cd dags/dbt
   dbt deps
   dbt parse
   cd ../..
   ```

4. **Run the IMDb data ingestion DAGs**

In the Airflow UI, enable and trigger the following DAGs:
- `imdb_download_extract_dag`
- `imdb_load_tables_dag`

5. **Run dbt models**

```bash
cd dags/dbt
dbt run --profiles-dir .
```

## 📋 Project Components

### 1. Airflow DAGs

- **imdb_download_extract_dag**: Downloads and extracts IMDb dataset files
- **imdb_load_tables_dag**: Loads extracted data into PostgreSQL
- **dbt_cosmos_dag**: Runs dbt transformations using Cosmos

### 2. Astronomer Cosmos Integration

Cosmos is a powerful tool that bridges the gap between Airflow and dbt:

- **Automatic DAG Generation**: Creates Airflow tasks directly from dbt models
- **Dependency Management**: Preserves dbt model dependencies in Airflow task dependencies
- **Unified Monitoring**: View dbt runs directly in the Airflow UI
- **Failure Handling**: Leverages Airflow's retry and alerting capabilities for dbt jobs
- **Consistent Orchestration**: Manages the entire ELT pipeline in a single tool

The project uses Cosmos to automatically generate a DAG from the dbt project structure, ensuring that:
1. dbt models run in the correct dependency order
2. Failed transformations can be retried without rerunning the entire pipeline
3. All data operations are visible in a single interface

### 3. dbt Transformation Layer

The dbt project transforms raw IMDb data into analytics-ready dimensional models with a **staging → intermediate → marts** architecture.

#### Project Metrics
- **16 Total Models**: 7 staging, 5 intermediate, 4 marts
- **Multi-layer Architecture**: Staging → Intermediate → Marts
- **Comprehensive Documentation**: All models documented
- **Strategic Materialization**: Views for staging/intermediate, tables for marts

#### Architecture & Data Flow

```
Raw IMDb Tables          Staging Layer           Intermediate Layer        Marts Layer
┌─────────────────┐      ┌──────────────────┐    ┌─────────────────────┐    ┌──────────────────┐
│ imdb_title_     │  →   │ stg_title_       │ →  │ int_title_with_     │ →  │ mart_top_titles  │
│ basics          │      │ basics           │    │ ratings             │    │                  │
├─────────────────┤      ├──────────────────┤    ├─────────────────────┤    ├──────────────────┤
│ imdb_name_      │  →   │ stg_name_        │ →  │ int_person_         │ →  │ mart_person_     │
│ basics          │      │ basics           │    │ filmography         │    │ career           │
├─────────────────┤      ├──────────────────┤    ├─────────────────────┤    ├──────────────────┤
│ imdb_title_     │  →   │ stg_title_crew_  │ →  │ int_title_          │ →  │ mart_genre_      │
│ crew            │      │ (directors/      │    │ complete            │    │ analytics        │
│                 │      │  writers)        │    │                     │    │                  │
├─────────────────┤      ├──────────────────┤    ├─────────────────────┤    ├──────────────────┤
│ imdb_title_     │  →   │ stg_title_       │ →  │ int_title_          │ →  │ mart_series_     │
│ episode         │      │ episode          │    │ hierarchies         │    │ analytics        │
├─────────────────┤      ├──────────────────┤    └─────────────────────┘    └──────────────────┘
│ imdb_title_     │  →   │ stg_title_       │
│ principals      │      │ principals       │
├─────────────────┤      ├──────────────────┤
│ imdb_title_     │  →   │ stg_title_       │
│ ratings         │      │ ratings          │
├─────────────────┤      └──────────────────┘
│ imdb_title_     │
│ akas            │
└─────────────────┘
```

#### Data Sources

The following IMDb tables are used as sources:

| Source Name | Description |
|-------------|-------------|
| title_basics | Core title information: type, title, runtime, genres |
| name_basics | Person information: name, birth/death years, professions |
| title_akas | Alternative titles by region and language |
| title_crew | Directors and writers for each title |
| title_episode | TV episode data linking to parent series |
| title_principals | Cast and crew information for each title |
| title_ratings | User ratings: average rating and vote count |

## 🛠️ Project Structure

```
apache-airflow-dev-server/
├── dags/                       # Airflow DAGs
│   ├── imdb_download_extract.py      # Downloads and extracts IMDb datasets
│   ├── imdb_load_tables.py           # Loads data into PostgreSQL
│   ├── dbt_cosmos_dag.py             # Generated by Cosmos from dbt project
│   │
│   └── dbt/                    # dbt transformation project
│       ├── models/                   # 16 Total transformation models
│       │   ├── staging/                  # 7 staging models
│       │   ├── intermediate/             # 5 intermediate models
│       │   └── marts/                    # 4 mart models
│       ├── dbt_project.yml              # dbt project configuration
│       ├── profiles.yml                 # Database connection profiles
│       └── packages.yml                 # Package dependencies (dbt-utils)
│
├── imdb_data/                 # Directory for downloaded IMDb datasets
├── imdb_schemas/              # Directory for data schemas
├── Dockerfile                 # Airflow container definition (using Astro Runtime 13.0.0)
├── docker-compose.override.yml # Custom container configuration for PostgreSQL 13
├── requirements.txt           # Python dependencies (dbt 1.4.6, cosmos 1.1.1)
├── packages.txt               # System-level dependencies
├── .env                       # Environment variables for container configuration
├── airflow_settings.yaml      # Local Airflow connection settings
└── README.md                  # This documentation
```

## 📚 Usage Instructions

### Running Airflow DAGs

1. Access the Airflow UI at http://localhost:8080/ (login with admin/admin)
2. Enable the DAGs in the following order:
   - `imdb_download_extract_dag` (downloads and extracts IMDb datasets)
   - `imdb_load_tables_dag` (loads data into PostgreSQL)
   - `dbt_cosmos_dag` (runs dbt models via Cosmos)

### Running dbt Commands

```bash
# Navigate to the dbt project
cd dags/dbt

# Run all models
dbt run --profiles-dir .

# Run only staging models
dbt run --profiles-dir . --select staging.*

# Test all models
dbt test --profiles-dir .

# Generate documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

## 🧩 Data Models

For detailed information about the dbt models, tests, and documentation, refer to:
- [dbt README](./dags/dbt/README.md)
- dbt documentation site (after running `dbt docs serve`)

## 📝 Future Development

- Enhancing Cosmos integration with custom callbacks and sensors
- Additional data quality checks
- Extending transformation models for more analytics use cases
- Dashboard integration (e.g., with Superset or Metabase)

## 🔗 Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Astronomer Cosmos Documentation](https://docs.astronomer.io/learn/airflow-dbt)
- [Astronomer Documentation](https://www.astronomer.io/docs/astro/)
- [IMDb Dataset Documentation](https://www.imdb.com/interfaces/)
