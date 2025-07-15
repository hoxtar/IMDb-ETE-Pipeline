# IMDb Analytics Pipeline with Airflow & dbt

[![Airflow](https://img.shields.io/badge/Airflow-2.7-red)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.4.6-purple)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue)](https://www.postgresql.org/)
[![Astronomer](https://img.shields.io/badge/Astro_Runtime-13.0.0-orange)](https://www.astronomer.io/)
[![Cosmos](https://img.shields.io/badge/Cosmos-1.10.1-green)](https://github.com/astronomer/astronomer-cosmos)
[![Status](https://img.shields.io/badge/Status-Production_Ready-brightgreen)]()

## Project Overview

A production-ready data pipeline that processes IMDb datasets using modern data engineering best practices. This project demonstrates enterprise-grade ELT (Extract, Load, Transform) architecture with intelligent caching, modular design, and automated data transformations.

This project serves as a comprehensive learning platform showcasing advanced data engineering skills including Apache Airflow orchestration, dbt transformations, PostgreSQL optimization, and Docker containerization.

**Key Technologies:**
- **Apache Airflow (2.7)**: Pipeline orchestration with Astronomer Runtime
- **dbt (1.4.6)**: Data transformations and modeling framework  
- **Astronomer Cosmos (1.10.1)**: Seamless Airflow and dbt integration
- **PostgreSQL (13)**: High-performance data warehouse
- **Docker**: Containerized development environment

**Pipeline Features:**
- **Modular Architecture**: Refactored from monolithic design into focused, maintainable modules
- **Smart Caching**: HTTP Last-Modified validation and Airflow Variables-based persistence
- **Incremental Processing**: File-based change detection for largest dataset (title_principals)
- **Bulk Loading**: PostgreSQL COPY operations for optimal performance
- **Strategic Materialization**: Staging (tables + indexes) → Intermediate (views) → Marts (tables + indexes)
- **Production-grade Error Handling**: Airflow native retry mechanisms with exponential backoff
- **Comprehensive Testing**: 66+ automated data quality tests across all layers
- **Performance Optimization**: Strategic materialization, indexing, and incremental logic

## Architecture

```
┌──────────────────┐    ┌─────────────────────┐    ┌──────────────────────┐
│   IMDb Datasets  │    │    Airflow DAG      │    │     PostgreSQL       │
│                  │    │                     │    │                      │
│ • title.basics   │───►│ 1. Download Phase   │───►│ Raw Tables          │
│ • title.ratings  │    │    - Cache validation│    │ • imdb_title_basics │
│ • name.basics    │    │    - Smart downloads │    │ • imdb_title_ratings│
│ • title.crew     │    │    - File integrity  │    │ • imdb_name_basics  │
│ • title.episode  │    │                     │    │ • imdb_title_crew   │
│ • title.akas     │    │ 2. Load Phase       │    │ • imdb_title_episode│
│ • title.principals│    │    - Bulk COPY ops  │    │ • imdb_title_akas   │
└──────────────────┘    │    - Row validation │    │ • imdb_title_principals│
                        └─────────────────────┘    └──────────────────────┘
                                   │                           │
                                   ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     dbt Transformation Pipeline                                 │
│                      (Astronomer Cosmos Integration)                           │
│                                                                                 │
│ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐               │
│ │   Staging Layer  │ │ Intermediate     │ │   Marts Layer    │               │
│ │   (Tables +      │ │   Layer (Views)  │ │   (Tables +      │               │
│ │   Indexes)       │ │                  │ │   Indexes)       │               │
│ │                  │ │                  │ │                  │               │
│ │ • Data cleaning  │ │ • Business logic │ │ • Analytics ready│               │
│ │ • Standardization│ │ • Relationships  │ │ • Aggregations   │               │
│ │ • Type casting   │ │ • Enrichment     │ │ • BI optimized   │               │
│ │ • Performance    │ │ • Real-time      │ │ • Strategic      │               │
│ │   indexes        │ │   freshness      │ │   indexing       │               │
│ │ • Incremental*   │ │                  │ │                  │               │
│ └──────────────────┘ └──────────────────┘ └──────────────────┘               │
│         │                       │                       │                    │
│         ▼                       ▼                       ▼                    │
│ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐               │
│ │ • 7 staging      │ │ • 4 intermediate │ │ • 4 mart models  │               │
│ │   models         │ │   models         │ │                  │               │
│ │ • 66+ tests      │ │ • Business rules │ │ • Executive      │               │
│ │ • 1 incremental  │ │ • Data lineage   │ │   dashboards     │               │
│ │   (principals*)  │ │ • Relationships  │ │ • Performance    │               │
│ └──────────────────┘ └──────────────────┘ └──────────────────┘               │
└─────────────────────────────────────────────────────────────────────────────────┘
                                   │
                          * stg_title_principals uses file-based
                            incremental processing (57M records)
```

## Quick Start

### Prerequisites

- **Docker Desktop** (Windows/Mac) or **Docker Engine** (Linux) with Docker Compose
- **Astronomer CLI** (`astro`) - [Installation Guide](https://docs.astronomer.io/astro/cli/install-cli)
- **Minimum 4GB free disk space** for IMDb datasets (compressed: ~1GB, extracted: ~3GB)
- **Git** for version control

### Environment Setup

1. **Clone the repository**
   ```powershell
   git clone <repository-url>
   cd apache-airflow-dev-server
   ```

2. **Initialize Astro project** (if not already done)
   ```powershell
   astro dev init
   ```

3. **Start the complete environment**
   ```powershell
   astro dev start
   ```

   This command initializes:
   - **Airflow Webserver**: http://localhost:8080 (admin/admin)
   - **Airflow Scheduler**: Background task scheduling
   - **PostgreSQL Database**: localhost:5433 (postgres/postgres)
   - **Postgres Admin UI**: http://localhost:5050 (admin@admin.com/admin)

### Port Configuration

If ports are already in use, configure alternatives before starting:

```powershell
# Configure custom ports
astro config set webserver.port 8081
astro config set postgres.port 5434

# Apply configuration
astro dev start
```

### dbt Manifest Generation

**Critical Step**: Cosmos requires a dbt manifest file for optimal performance.

```powershell
# Navigate to dbt project
cd dags/dbt

# Install dbt dependencies
dbt deps

# Generate manifest (required for Cosmos)
dbt parse

# Return to project root
cd ../..
```

**When to regenerate manifest:**
- After adding/modifying dbt models
- After changing dbt project configuration
- Before deploying to production

### Pipeline Execution

1. **Access Airflow UI**: http://localhost:8080 (admin/admin)

2. **Enable the main DAG**:
   - Navigate to DAGs page
   - Find `imdb_pipeline_v2`
   - Toggle the DAG to **ON**

3. **Trigger the pipeline**:
   - Click the play button to trigger manually
   - Or wait for the daily schedule (`@daily`)

4. **Monitor execution**:
   - Download tasks: ~10-30 minutes (depends on internet speed)
   - Load tasks: ~30-90 minutes (depends on system resources, first run longest)
   - dbt transformations: ~5-15 minutes (single unified task group)

### Verify Installation

```powershell
# Check Airflow status
astro dev ps

# View logs
astro dev logs

# Connect to PostgreSQL
psql -h localhost -p 5433 -U postgres -d airflow
```

## Project Components

### 1. Modular Pipeline Architecture

**Refactored Design**: The pipeline has been refactored from a monolithic 925-line DAG into a modular, maintainable architecture:

**Core Components:**
- **imdb_pipeline/config.py**: Centralized configuration management
- **imdb_pipeline/cache_manager.py**: Intelligent caching system  
- **imdb_pipeline/downloader.py**: File downloading with retries
- **imdb_pipeline/loader.py**: PostgreSQL bulk loading
- **imdb_pipeline/dbt_tasks.py**: dbt/Cosmos integration
- **imdb_pipeline/utils.py**: Shared utility functions

**Main DAG** (`imdb_pipeline_v2.py`): Clean, modular DAG that orchestrates:

- **Download Tasks** (7 parallel tasks):
  - Smart caching with HTTP Last-Modified validation
  - Exponential backoff retry logic for network resilience
  - Automatic file validation and atomic writes

- **Load Tasks** (7 parallel tasks):
  - PostgreSQL COPY operations for high-performance bulk loading
  - Cache-based duplicate prevention with fixed timing bug
  - Memory-efficient streaming decompression
  - Comprehensive data integrity validation

- **dbt Transformations** (Single task group via Cosmos):
  - **Unified Execution**: All layers executed with proper dbt dependency resolution
  - **Incremental Processing**: File-based change detection for large datasets
  - **Comprehensive Testing**: 66+ data quality tests across all transformation layers

### 2. Astronomer Cosmos Integration

**Advanced dbt and Airflow Bridge:**

- **Automatic Task Generation**: Creates Airflow tasks directly from dbt models
- **Dependency Management**: Preserves dbt model dependencies in Airflow task graph
- **Unified Execution**: Single task group runs all dbt layers with proper dependencies
- **Unified Monitoring**: View dbt runs and tests directly in Airflow UI
- **Intelligent Failure Handling**: Leverages Airflow's retry and alerting for dbt jobs
- **Performance Optimization**: Uses `LoadMode.DBT_MANIFEST` for faster parsing

**Configuration Highlights:**
```python
# Single dbt task group with unified execution
dbt_tasks = create_dbt_task_group_simple(
    dag, 
    load_tasks,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt"
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        test_behavior=TestBehavior.AFTER_ALL,
        emit_datasets=True
    )
)
```

### 3. dbt Transformation Architecture

**Layer-based Data Modeling** following analytics engineering best practices:

#### Staging Layer (Raw Data Processing - Materialized as Tables)
| Model | Purpose | Features | Materialization |
|-------|---------|----------|----------------|
| `stg_title_basics` | Title standardization | Genre parsing, boolean conversion, type casting | Table + Indexes |
| `stg_name_basics` | Person data cleaning | Profession/filmography array parsing | Table + Indexes |
| `stg_title_crew_directors` | Director normalization | Director relationship extraction | Table + Indexes |
| `stg_title_crew_writers` | Writer normalization | Writer relationship extraction | Table + Indexes |
| `stg_title_episode` | Episode hierarchy | Season/episode number standardization | Table + Indexes |
| `stg_title_principals` | Cast/crew staging | Role and character data cleaning | **Incremental Table** |
| `stg_title_ratings` | Ratings validation | Data quality checks, type conversion | Table + Indexes |

**Key Innovation: Incremental Processing for Large Datasets**
- **Problem Solved**: `stg_title_principals` (57M records) required significant processing time
- **Solution**: File-based change detection using row count comparison
- **Performance Impact**: Skips processing when source file unchanged (row counts match)
- **Implementation**: dbt incremental materialization with intelligent WHERE clause
- **Business Value**: Maintains data freshness while optimizing resource usage
| `stg_title_crew_writers` | Writer normalization | Writer relationship extraction |
| `stg_title_ratings` | Ratings validation | Data quality checks, type conversion |
| `stg_title_episode` | Episode hierarchy | Season/episode number standardization |
| `stg_title_principals` | Cast/crew staging | Role and character data cleaning |

#### Intermediate Layer (Business Logic - Materialized as Views)
| Model | Purpose | Dependencies | Materialization |
|-------|---------|--------------|----------------|
| `int_title_with_genres` | Genre dimensional modeling | `stg_title_basics` | View |
| `int_person_filmography` | Comprehensive career analytics | Multiple staging models | View |
| `int_title_hierarchies` | Enhanced TV series analytics | `stg_title_episode` + `stg_title_basics` + `stg_title_ratings` | View |
| `int_title_complete` | Fully denormalized titles | All staging models | View |

#### Marts Layer (Analytics-Ready - Materialized as Tables)
| Mart | Analytics Focus | Key Metrics | Business Intelligence |
|------|----------------|-------------|----------------------|
| `mart_top_titles` | Content performance & rankings | Weighted scoring, percentile rankings, multi-dimensional analysis | Content discovery, investment insights, recommendation engines |
| `mart_person_career` | Individual talent analytics | Career spans, productivity tiers, role versatility, quality reputation | Talent acquisition, industry benchmarking, collaboration planning |
| `mart_genre_analytics` | Genre market intelligence | Market share, production trends, quality evolution, platform strategy | Portfolio optimization, trend intelligence, competitive analysis |
| `mart_series_analytics` | TV series insights & development | Episode analytics, season performance, longevity classification, engagement metrics | Series development, renewal decisions, audience retention analysis |

### 4. Production Features

**Enterprise-grade Capabilities:**

- **Smart Caching**: Airflow Variables-based persistence across runs with HTTP Last-Modified validation
- **Incremental Processing**: File-based change detection for the largest dataset (title_principals, 57M records)
- **Adaptive Timeouts**: Task-specific timeouts based on empirical data (up to 2 hours for large datasets)
- **Error Resilience**: Exponential backoff, configurable retry strategies with 3-attempt default
- **Comprehensive Monitoring**: Heartbeat updates, progress tracking, and row count validation
- **Automatic Cleanup**: Memory management, temporary file removal, and connection pooling
- **Security**: SQL injection prevention, input validation, and proper credential management
- **Performance**: Bulk COPY operations, streaming processing, and strategic materialization strategy

## Project Structure

```
apache-airflow-dev-server/                 # Astronomer Astro Project
├── dags/                                  # Airflow DAGs Directory
│   ├── imdb_pipeline/                         # Modular Pipeline Components
│   │   ├── __init__.py                            # Package initialization
│   │   ├── config.py                              # Configuration management
│   │   ├── cache_manager.py                       # Intelligent caching system
│   │   ├── downloader.py                          # File downloading with retries
│   │   ├── loader.py                              # PostgreSQL bulk loading
│   │   ├── dbt_tasks.py                           # dbt/Cosmos integration
│   │   └── utils.py                               # Shared utility functions
│   │
│   ├── imdb_pipeline_v2.py                   # Main Modular DAG (Production)
│   ├── download_uploading.py                 # Original Monolithic DAG (Backup)
│   ├── download_uploading_backup.py          # Additional Backup
│   │
│   └── dbt/                                  # dbt Transformation Project  
│       ├── models/                               # 15 Total Models
│       │   ├── staging/                              # 7 Staging Models
│       │   │   ├── stg_title_basics.sql                  # Title standardization & genre parsing
│       │   │   ├── stg_name_basics.sql                   # Person data with profession arrays
│       │   │   ├── stg_title_crew_directors.sql          # Normalized director relationships 
│       │   │   ├── stg_title_crew_writers.sql            # Normalized writer relationships
│       │   │   ├── stg_title_episode.sql                 # TV episode hierarchy cleaning
│       │   │   ├── stg_title_principals.sql              # Cast & crew standardization (INCREMENTAL)
│       │   │   ├── stg_title_ratings.sql                 # Ratings validation & typing
│       │   │   ├── schema.yml                            # Model tests & documentation
│       │   │   └── sources.yml                           # Source definitions & freshness
│       │   │
│       │   ├── intermediate/                         # 4 Intermediate Models  
│       │   │   ├── int_title_with_genres.sql             # Genre dimension modeling
│       │   │   ├── int_person_filmography.sql            # Comprehensive career analytics
│       │   │   ├── int_title_hierarchies.sql             # Enhanced TV series analytics
│       │   │   ├── int_title_complete.sql                # Fully denormalized titles
│       │   │   └── schema.yml                            # Relationship tests & docs
│       │   │
│       │   └── marts/                                # 4 Analytics-Ready Marts
│       │       ├── mart_top_titles.sql                   # Performance & ranking analytics
│       │       ├── mart_person_career.sql                # Individual achievement metrics
│       │       ├── mart_genre_analytics.sql              # Genre trends & time series
│       │       ├── mart_series_analytics.sql             # TV series & episode insights  
│       │       └── schema.yml                            # Business metric documentation
│       │
│       ├── target/                               # Compiled dbt Assets
│       │   ├── manifest.json                         # Model dependencies (for Cosmos)
│       │   ├── run_results.json                      # Execution results & performance
│       │   └── compiled/                             # Generated SQL for review
│       │
│       ├── dbt_packages/                         # External Dependencies
│       │   └── dbt_utils/                            # dbt community utilities
│       │
│       ├── dbt_project.yml                      # Project configuration & materialization
│       ├── profiles.yml                         # Database connection profiles (port 5433)
│       ├── packages.yml                         # Package dependencies (dbt-utils)
│       └── README.md                            # Detailed dbt documentation
│
├── Data Directories/                          # Local Storage (auto-created)
│   ├── imdb_data/                                # Downloaded datasets (.tsv.gz files)
│   └── imdb_schemas/                             # PostgreSQL table schemas (.sql files)
│
├── Docker Configuration/                      # Containerization Setup
│   ├── Dockerfile                                # Astro Runtime 13.0.0 (Airflow 2.7)
│   ├── docker-compose.override.yml               # PostgreSQL 13 configuration
│   └── .env                                      # Environment variables
│
├── Configuration Files/                       # Project Setup
│   ├── requirements.txt                          # Python dependencies
│   │                                             #    ├─ dbt-core==1.4.6
│   │                                             #    ├─ dbt-postgres==1.4.6  
│   │                                             #    └─ astronomer-cosmos==1.10.1
│   ├── packages.txt                              # System packages
│   ├── airflow_settings.yaml                    # Airflow connection configs
│   └── README.md                                 # This documentation
│
└── Testing/                                  # Quality Assurance  
    └── tests/dags/                               # DAG validation tests
        └── test_dag_example.py                       # Basic DAG import testing
```

### Key Directory Functions

| Directory | Purpose | Auto-generated |
|-----------|---------|----------------|
| `dags/` | Airflow DAGs and dbt project | Partial (dbt target/) |
| `imdb_data/` | Downloaded IMDb datasets | ✅ Yes |
| `imdb_schemas/` | PostgreSQL table definitions | ❌ Manual |
| `dbt/target/` | Compiled dbt artifacts | ✅ Yes (dbt parse) |
| `dbt/dbt_packages/` | External dbt packages | ✅ Yes (dbt deps) |

### Critical Files

| File | Purpose | Update Frequency |
|------|---------|------------------|
| `imdb_pipeline_v2.py` | Main production pipeline | As needed |
| `dbt/manifest.json` | Cosmos dependency mapping | After model changes |
| `dbt/profiles.yml` | Database connections | Environment changes |
| `requirements.txt` | Python dependencies | Version updates |

## Usage Guide

### Production Pipeline Execution

#### Primary Workflow: `imdb_pipeline_v2`

**Single DAG handles complete ELT process:**

1. **Navigate to Airflow UI**: http://localhost:8080 (admin/admin)

2. **Enable the pipeline**:
   ```
   DAGs → imdb_pipeline_v2 → Toggle ON
   ```

3. **Monitor execution phases**:
   ```
   Phase 1: Download Tasks       (7 parallel) →  ~10-30 min
   Phase 2: Load Tasks           (7 parallel) →  ~30-90 min*  
   Phase 3: dbt Transformations  (unified)    →  ~5-15 min
   
   * Load times vary by dataset size:
   * - Small files (ratings, crew): ~5-10 min each
   * - Medium files (basics, names): ~15-30 min each  
   * - Large files (principals, akas): ~30-90 min each (incremental logic helps)
   ```

4. **Execution results**:
   - Raw data: 7 PostgreSQL tables (`imdb_*`)
   - Staging models: 7 cleaned datasets (`stg_*`) - materialized as tables with indexes
   - Intermediate models: 4 business logic models (`int_*`) - materialized as views  
   - Marts models: 4 analytics-ready datasets (`mart_*`) - materialized as tables with indexes

### Manual dbt Operations

```powershell
# Navigate to dbt project
cd dags/dbt

# Core Operations
dbt deps                      # Install packages (dbt-utils)
dbt parse                     # Generate manifest for Cosmos
dbt run --profiles-dir .      # Run all models (staging → intermediate → marts)

# Model-specific Execution  
dbt run --profiles-dir . --select stg_title_principals    # Run incremental model only
dbt run --profiles-dir . --select +mart_person_career     # Run model + dependencies
dbt run --profiles-dir . --select tag:staging             # Run staging layer only
dbt run --profiles-dir . --select tag:marts               # Run marts layer only

# Incremental Processing
dbt run --profiles-dir . --select stg_title_principals --full-refresh  # Force full refresh
dbt run --profiles-dir . --select stg_title_principals                 # Incremental run

# Testing & Validation
dbt test --profiles-dir .                           # Run all 66+ tests
dbt test --profiles-dir . --select staging.*        # Test staging models
dbt test --profiles-dir . --select source:*         # Test source data freshness

# Documentation & Analysis
dbt docs generate --profiles-dir .                  # Generate docs
dbt docs serve --profiles-dir .                     # Serve locally (port 8080)
dbt ls --profiles-dir . --select +mart_top_titles   # Show dependencies
```

### Database Access

```powershell
# PostgreSQL Connection
psql -h localhost -p 5433 -U postgres -d airflow

# Query examples
\dt                           # List all tables
SELECT * FROM mart_top_titles LIMIT 10;
SELECT COUNT(*) FROM imdb_title_basics;
```

### Performance Monitoring

```powershell
# Airflow logs
astro dev logs scheduler      # Scheduler logs
astro dev logs webserver      # Webserver logs  

# Container status
astro dev ps                  # Running containers
astro dev stop                # Stop environment
astro dev start               # Restart environment
```

### Troubleshooting

#### Common Issues & Solutions

| Issue | Symptom | Solution |
|-------|---------|----------|
| **dbt Manifest Missing** | Cosmos parse errors | `cd dags/dbt && dbt parse` |
| **Port Conflicts** | Cannot start containers | Use `astro config set` |
| **Download Failures** | Network timeout errors | Check retry configuration |
| **Database Connection** | Connection refused | Verify port 5433 is free |
| **Memory Issues** | Tasks killed by OS | Increase Docker memory limit |

#### Debug Commands

```powershell
# dbt debugging
cd dags/dbt
dbt debug --profiles-dir .    # Test database connection

# Airflow task debugging  
astro dev bash               # Enter container
airflow tasks test <dag_id> <task_id> <date>

# Clean restart
astro dev kill               # Force stop
docker system prune -f       # Clean Docker cache
astro dev start              # Fresh start
```

### Environment Management

```powershell
# Development workflow
astro dev start              # Start development
# ... make changes ...
astro dev restart            # Apply changes
dbt parse                    # Update manifest after dbt changes

# Production deployment
astro deploy                 # Deploy to Astronomer (if configured)
```

### Data Pipeline Monitoring

**Key Metrics to Monitor:**
- **Download Cache Hit Rate**: Check Airflow Variables for HTTP Last-Modified validation success
- **Incremental Processing Efficiency**: Monitor stg_title_principals row count comparisons in logs
- **Load Performance**: Track row counts vs. file sizes across all datasets for performance trends
- **dbt Test Results**: Monitor 66+ data quality validations across all transformation layers
- **Task Duration Trends**: Performance over time, especially for large datasets and incremental processing

## Data Models & Analytics

### Source Data Overview

**IMDb Dataset Statistics:**
| Dataset | Records | Size (Compressed) | Processing Strategy | Primary Use |
|---------|---------|-------------------|-------------------|-------------|
| `title.basics.tsv.gz` | ~10M | ~200MB | Standard table refresh | Core title information |
| `title.principals.tsv.gz` | ~57M | ~600MB | **Incremental (file-based)** | Cast and crew roles |
| `title.akas.tsv.gz` | ~35M | ~300MB | Standard table refresh | International titles |
| `name.basics.tsv.gz` | ~13M | ~150MB | Standard table refresh | Person information |
| `title.crew.tsv.gz` | ~10M | ~100MB | Standard table refresh | Directors and writers |
| `title.episode.tsv.gz` | ~7.8M | ~80MB | Standard table refresh | TV episode hierarchy |
| `title.ratings.tsv.gz` | ~1.4M | ~25MB | Standard table refresh | User ratings |

**Processing Performance:**
- **First Run**: Full dataset processing (~2-3 hours total for all 7 datasets)
- **Subsequent Runs**: Cached downloads + incremental processing (~45-90 minutes typical)
- **Incremental Benefit**: title_principals skips processing when file unchanged (row count comparison)
- **Cache Efficiency**: Downloads skipped when HTTP Last-Modified headers indicate no changes

### Analytics Capabilities

#### **Available Marts**

| Mart | Analytics Focus | Key Questions Answered | Business Intelligence Features |
|------|----------------|------------------------|------------------------------|
| **`mart_top_titles`** | Content Performance & Discovery | • What are the highest-rated films by genre?<br>• Which titles have the most audience engagement?<br>• How do ratings vary by release decade? | • Multi-dimensional scoring algorithms<br>• Percentile-based rankings<br>• Investment category classification<br>• Content discovery optimization |
| **`mart_person_career`** | Talent Analytics & Industry Insights | • Who are the most prolific directors?<br>• Which actors have the longest careers?<br>• What's the average career span by profession? | • Career stage classification<br>• Productivity tier analysis<br>• Role versatility assessment<br>• Quality reputation tracking |
| **`mart_genre_analytics`** | Market Intelligence & Strategy | • How have genre preferences changed over time?<br>• Which genres consistently rate highest?<br>• What's the volume trend by genre and decade? | • Market share analysis<br>• Production trend intelligence<br>• Investment priority scoring<br>• Platform strategy optimization |
| **`mart_series_analytics`** | TV Industry & Series Development | • Which series have the most consistent ratings?<br>• How do episode ratings vary within seasons?<br>• What's the optimal series length by genre? | • Longevity classification<br>• Performance tier analysis<br>• Consistency rating assessment<br>• Engagement level tracking |

#### **Sample Analytics Queries**

```sql
-- Content Discovery: Top 10 highest-rated movies with confidence scoring
SELECT primary_title, average_rating, num_votes, weighted_score, performance_tier
FROM mart_top_titles 
WHERE title_type = 'movie' AND num_votes >= 10000
ORDER BY weighted_score DESC, num_votes DESC
LIMIT 10;

-- Talent Analysis: Most versatile professionals with quality track records
SELECT person_name, total_credits, career_span_years, avg_project_rating, 
       dominant_role, versatility_level, quality_reputation
FROM mart_person_career 
WHERE versatility_level = 'Multi-Talented' AND avg_project_rating >= 7.0
ORDER BY total_credits DESC, avg_project_rating DESC;

-- Market Intelligence: Investment opportunities by genre
SELECT genre, market_share, production_trend, quality_trend, 
       investment_priority, content_strategy
FROM mart_genre_analytics
WHERE investment_priority IN ('High Priority', 'Medium Priority', 'Emerging Opportunity')
ORDER BY market_share DESC, avg_rating DESC;

-- Series Development: Premium TV series with consistent quality
SELECT series_title, total_episodes, total_seasons, avg_series_rating, 
       consistency_rating, series_performance_tier, engagement_level
FROM mart_series_analytics
WHERE series_performance_tier IN ('Premium Franchise', 'Quality Series')
  AND consistency_rating IN ('Very Consistent', 'Consistent')
ORDER BY avg_series_rating DESC, total_episodes DESC;
```

### Advanced dbt Features

**Model Tagging Strategy:**
```sql
-- Staging models
{{ config(tags=['staging']) }}

-- Intermediate models  
{{ config(tags=['intermediate']) }}

-- Marts models
{{ config(tags=['marts']) }}
```

**Materialization Strategy:**
- **Staging**: `TABLE` with strategic indexes (performance foundation for downstream queries)
- **Intermediate**: `VIEW` (real-time freshness, leverages staging table indexes for performance)  
- **Marts**: `TABLE` with strategic indexes (BI tool optimization and fast query performance)

**Advanced Features:**
- **Incremental Processing**: File-based change detection for large datasets
- **Performance Optimization**: Strategic materialization based on layer purpose
- **Comprehensive Testing**: 66+ tests covering data quality, relationships, and business logic
- **Source Freshness**: Automated validation of data currency and completeness

For detailed model documentation and lineage, refer to:
- [dbt Project README](./dags/dbt/README.md)
- dbt docs site: `cd dags/dbt && dbt docs serve --profiles-dir .`

## Development & Deployment

### Future Enhancements

**Pipeline Improvements:**
- [ ] **Advanced Incremental Models**: Extend incremental processing to other large datasets beyond title_principals
- [ ] **Data Quality Monitoring**: Advanced anomaly detection and alerting dashboards
- [ ] **Performance Optimization**: Parallel dbt execution and advanced resource allocation
- [ ] **Schema Evolution**: Automatic handling of IMDb schema changes and data drift detection

**Analytics Extensions:**
- [ ] **Real-time Dashboards**: Integration with Superset or Metabase
- [ ] **Machine Learning Models**: Rating prediction and recommendation engines
- [ ] **API Layer**: REST API for analytics data consumption
- [ ] **Data Exports**: Automated report generation and distribution

**Production Features:**
- [ ] **Multi-environment Support**: Dev/Staging/Production configuration
- [ ] **CI/CD Pipeline**: Automated testing and deployment
- [ ] **Monitoring & Alerting**: Comprehensive observability stack
- [ ] **Data Lineage**: Visual impact analysis and documentation

### Contributing Guidelines

**Development Workflow:**
1. **Fork the repository** and create a feature branch
2. **Update dbt manifest** after model changes: `dbt parse`
3. **Run comprehensive tests**: `dbt test --profiles-dir .`
4. **Validate DAG syntax**: `astro dev parse`
5. **Update documentation** for significant changes
6. **Submit pull request** with detailed description

**Code Standards:**
- **dbt Models**: Follow naming conventions (`stg_*`, `int_*`, `mart_*`)
- **SQL Formatting**: Use consistent indentation and commenting
- **Documentation**: Update `schema.yml` files for new models
- **Testing**: Add appropriate tests for data quality validation

### Production Deployment

**Astronomer Platform:**
```powershell
# Initialize Astronomer deployment
astro login
astro deployment create --name imdb-production

# Deploy to production
astro deploy --deployment-id <deployment-id>
```

**Environment Configuration:**
```yaml
# astro.yaml example
project:
  name: imdb-analytics-pipeline
environments:
  development:
    dag_deploy_enabled: true
  production:
    dag_deploy_enabled: true
    webserver:
      replicas: 2
    scheduler:
      replicas: 2
```

## Resources & Documentation

### **Official Documentation**
- [Apache Airflow Docs](https://airflow.apache.org/docs/) - Core orchestration platform
- [dbt Documentation](https://docs.getdbt.com/) - Transformation framework
- [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/) - Airflow ↔ dbt integration
- [Astronomer Platform](https://www.astronomer.io/docs/astro/) - Managed Airflow service

### **Technical References**
- [IMDb Dataset Interface](https://www.imdb.com/interfaces/) - Source data documentation
- [PostgreSQL Documentation](https://www.postgresql.org/docs/) - Database platform
- [Docker Documentation](https://docs.docker.com/) - Containerization

### **Learning Resources**
- [Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/) - Best practices
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) - Production guidelines
- [Modern Data Stack](https://www.getdbt.com/blog/future-of-the-modern-data-stack/) - Architecture principles

### **Community & Support**
- [dbt Community](https://www.getdbt.com/community/) - Slack workspace and forums
- [Airflow Community](https://airflow.apache.org/community/) - Mailing lists and events
- [Astronomer Community](https://www.astronomer.io/community/) - Support and resources

---

## License & Credits

**Project Information:**
- **Author**: Andrea Usai
- **Version**: 2.1.0 (Production Ready with Incremental Processing)
- **Last Updated**: July 2025
- **License**: [Specify your license]

**Acknowledgments:**
- **IMDb**: For providing comprehensive entertainment datasets
- **Astronomer**: For the excellent Cosmos integration and Astro platform
- **dbt Labs**: For the powerful data transformation framework
- **Apache Airflow Community**: For the robust orchestration platform

**Data Source Attribution:**
> Information courtesy of IMDb (https://www.imdb.com). Used with permission.

---

**If this project helped you, please consider giving it a star on GitHub!**
