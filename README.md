# IMDb Analytics Pipeline with Airflow & dbt

[![Airflow](https://img.shields.io/badge/Airflow-2.7-red)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.4.6-purple)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13-blue)](https://www.postgresql.org/)
[![Astronomer](https://img.shields.io/badge/Astro_Runtime-13.0.0-orange)](https://www.astronomer.io/)
[![Cosmos](https://img.shields.io/badge/Cosmos-1.1.1-green)](https://github.com/astronomer/astronomer-cosmos)
[![Status](https://img.shields.io/badge/Status-Production_Ready-brightgreen)]()

## 📊 Project Overview

A production-ready data pipeline that processes IMDb datasets using modern data engineering best practices. This project demonstrates enterprise-grade ELT (Extract, Load, Transform) architecture with intelligent caching, advanced error handling, and automated data transformations.

**Key Technologies:**
- **Apache Airflow (2.7)**: Pipeline orchestration with Astronomer Runtime
- **dbt (1.4.6)**: Data transformations and modeling framework  
- **Astronomer Cosmos (1.1.1)**: Seamless Airflow ↔ dbt integration
- **PostgreSQL (13)**: High-performance data warehouse
- **Docker**: Containerized development environment

**Pipeline Features:**
- **Smart Caching**: HTTP Last-Modified validation and Airflow Variables-based caching
- **Bulk Loading**: PostgreSQL COPY operations for 10-100x faster data loading
- **Layer-based Transformations**: Staging → Intermediate → Marts architecture
- **Production-grade Error Handling**: Exponential backoff, configurable retries, comprehensive logging
- **Intelligent Monitoring**: Task-specific timeouts based on empirical data, heartbeat updates

## 🏗️ Architecture

```
┌──────────────────┐    ┌─────────────────────┐    ┌──────────────────────┐
│   IMDb Datasets  │    │    Airflow DAG      │    │     PostgreSQL       │
│                  │    │                     │    │                      │
│ • title.basics   │───►│ 1. Download Phase   │───►│ Raw Tables          │
│ • title.ratings  │    │    - Cache validation│    │ • imdb_title_basics │
│ • name.basics    │    │    - Smart downloads │    │ • imdb_title_ratings│
│ • title.crew     │    │                     │    │ • imdb_name_basics  │
│ • title.episode  │    │ 2. Load Phase       │    │ • ...               │
│ • title.akas     │    │    - Bulk COPY ops  │    │                      │
│ • title.principals│    │    - Duplicate prev │    │                      │
└──────────────────┘    └─────────────────────┘    └──────────────────────┘
                                   │                           │
                                   │        ┌─────────────────┼─────────────────┐
                        ┌──────────▼────────▼───┐             │                 │
                        │   Cosmos Integration   │             │                 │
                        │  (dbt ↔ Airflow)      │             │                 │
                        └──────────┬─────────────┘             │                 │
                                   │                           │                 │
              ┌────────────────────▼────────────────────┐      │                 │
              │            dbt Transformations          │      │                 │
              │                                         │      │                 │
              │ ┌─────────────┐ ┌──────────────────┐   │      │                 │
              │ │   Staging   │ │   Intermediate   │   │      │                 │
              │ │             │ │                  │   │      │                 │
              │ │ • Data      │ │ • Business       │   │      │                 │
              │ │   cleaning  │ │   logic          │   │      │                 │
              │ │ • Standards │ │ • Calculations   │   │      │                 │
              │ │ • Parsing   │ │ • Joins          │   │      │                 │
              │ └─────────────┘ └──────────────────┘   │      │                 │
              │                         │              │      │                 │
              │ ┌───────────────────────▼─────────────┐│      │                 │
              │ │            Marts                    ││      │                 │
              │ │                                     ││      │                 │
              │ │ • mart_top_titles                   ││      │                 │
              │ │ • mart_person_career                ││      │                 │
              │ │ • mart_genre_analytics              ││      │                 │
              │ │ • mart_series_analytics             ││      │                 │
              │ └─────────────────────────────────────┘│      │                 │
              └─────────────────────────────────────────┘      │                 │
                                   │                           │                 │
                                   └───────────────────────────┼─────────────────┘
                                                               │
┌─────────────────────────────────────────────────────────────▼─────────────────┐
│                          Analytics Layer                                       │
│                                                                                │
│ ┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐               │
│ │   Top Titles     │ │  Person Career   │ │ Genre Analytics  │               │
│ │                  │ │                  │ │                  │               │
│ │ • High ratings   │ │ • Filmography    │ │ • Trends         │               │
│ │ • Vote counts    │ │ • Career spans   │ │ • Popularity     │               │
│ │ • Genre analysis │ │ • Achievements   │ │ • Time series    │               │
│ └──────────────────┘ └──────────────────┘ └──────────────────┘               │
└────────────────────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Start

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
   - 🌐 **Airflow Webserver**: http://localhost:8080 (admin/admin)
   - 🔄 **Airflow Scheduler**: Background task scheduling
   - 🗄️ **PostgreSQL Database**: localhost:5433 (postgres/postgres)
   - 📊 **Postgres Admin UI**: http://localhost:5050 (admin@admin.com/admin)

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

**⚠️ Critical Step**: Cosmos requires a dbt manifest file for optimal performance.

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
   - Find `imdb_cosmos_pipeline`
   - Toggle the DAG to **ON**

3. **Trigger the pipeline**:
   - Click the play button (▶️) to trigger manually
   - Or wait for the daily schedule (`@daily`)

4. **Monitor execution**:
   - Download tasks: ~10-30 minutes (depends on internet speed)
   - Load tasks: ~20-60 minutes (depends on system resources) 
   - dbt transformations: ~5-15 minutes

### Verify Installation

```powershell
# Check Airflow status
astro dev ps

# View logs
astro dev logs

# Connect to PostgreSQL
psql -h localhost -p 5433 -U postgres -d airflow
```

## 📋 Project Components

### 1. Unified Data Pipeline (`imdb_cosmos_pipeline`)

**Current Implementation**: Single, production-ready DAG that handles the complete ELT process:

- **Download Tasks** (7 parallel tasks):
  - Smart caching with HTTP Last-Modified validation
  - Exponential backoff retry logic for network resilience
  - Automatic file validation and atomic writes

- **Load Tasks** (7 parallel tasks):
  - PostgreSQL COPY operations for high-performance bulk loading
  - Cache-based duplicate prevention
  - Memory-efficient streaming decompression
  - Comprehensive data integrity validation

- **dbt Transformation Groups** (3 sequential groups):
  - **Staging Layer**: Data cleaning and standardization (7 models)
  - **Intermediate Layer**: Business logic and calculated fields (5 models)
  - **Marts Layer**: Analytics-ready tables (4 marts)

### 2. Astronomer Cosmos Integration

**Advanced dbt ↔ Airflow Bridge:**

- ✅ **Automatic Task Generation**: Creates Airflow tasks directly from dbt models
- ✅ **Dependency Management**: Preserves dbt model dependencies in Airflow task graph
- ✅ **Layer-based Execution**: Separate task groups for staging/intermediate/marts
- ✅ **Unified Monitoring**: View dbt runs and tests directly in Airflow UI
- ✅ **Intelligent Failure Handling**: Leverages Airflow's retry and alerting for dbt jobs
- ✅ **Performance Optimization**: Uses `LoadMode.DBT_MANIFEST` for faster parsing

**Configuration Highlights:**
```python
# Staging layer (7 models)
stg_render_config = RenderConfig(
    select=["tag:staging"],
    test_behavior=TestBehavior.AFTER_ALL
)

# Intermediate layer (5 models)
int_render_config = RenderConfig(
    select=["tag:intermediate"],
    test_behavior=TestBehavior.AFTER_ALL
)

# Marts layer (4 models)
mart_render_config = RenderConfig(
    select=["tag:marts"],
    test_behavior=TestBehavior.AFTER_ALL
)
```

### 3. dbt Transformation Architecture

**Layer-based Data Modeling** following analytics engineering best practices:

#### 📥 **Staging Layer** (Raw Data Processing)
| Model | Purpose | Features |
|-------|---------|----------|
| `stg_title_basics` | Title standardization | Genre parsing, boolean conversion, type casting |
| `stg_name_basics` | Person data cleaning | Profession/filmography array parsing |
| `stg_title_crew_*` | Crew normalization | Director/writer relationship extraction |
| `stg_title_ratings` | Ratings validation | Data quality checks, type conversion |
| `stg_title_episode` | Episode hierarchy | Season/episode number standardization |
| `stg_title_principals` | Cast/crew staging | Role and character data cleaning |

#### ⚙️ **Intermediate Layer** (Business Logic)
| Model | Purpose | Dependencies |
|-------|---------|--------------|
| `int_title_with_ratings` | Enriched title data | `stg_title_basics` + `stg_title_ratings` |
| `int_title_with_genres` | Genre dimensional modeling | `stg_title_basics` |
| `int_person_filmography` | Comprehensive career data | Multiple staging models |
| `int_title_hierarchies` | Series-episode relationships | `stg_title_episode` + `stg_title_basics` |
| `int_title_complete` | Fully denormalized titles | All staging models |

#### 📊 **Marts Layer** (Analytics-Ready)
| Mart | Analytics Focus | Key Metrics |
|------|----------------|-------------|
| `mart_top_titles` | Performance rankings | Ratings, vote counts, genre analysis |
| `mart_person_career` | Individual achievements | Career spans, film counts, role diversity |
| `mart_genre_analytics` | Genre trends | Decade analysis, popularity shifts |
| `mart_series_analytics` | TV series insights | Episode ratings, season performance |

### 4. Production Features

**Enterprise-grade Capabilities:**

- 🔄 **Smart Caching**: Airflow Variables-based persistence across runs
- 📈 **Adaptive Timeouts**: Task-specific timeouts based on empirical data
- 🛡️ **Error Resilience**: Exponential backoff, configurable retry strategies
- 📊 **Comprehensive Monitoring**: Heartbeat updates, progress tracking
- 🧹 **Automatic Cleanup**: Memory management, temporary file removal
- 🔒 **Security**: SQL injection prevention, input validation
- ⚡ **Performance**: Bulk COPY operations, streaming processing

## 🛠️ Project Structure

```
apache-airflow-dev-server/                 # Astronomer Astro Project
├── 🚀 dags/                               # Airflow DAGs Directory
│   ├── download_uploading.py                  # 🎯 MAIN PIPELINE (Production-ready)
│   │                                          #    ├─ Download tasks (7 parallel)
│   │                                          #    ├─ Load tasks (7 parallel) 
│   │                                          #    └─ dbt transformation groups (3 layers)
│   │
│   └── 📊 dbt/                            # dbt Transformation Project  
│       ├── models/                            # 📈 16 Total Models
│       │   ├── staging/                           # 🧹 7 Staging Models
│       │   │   ├── stg_title_basics.sql               # Title standardization & genre parsing
│       │   │   ├── stg_name_basics.sql                # Person data with profession arrays
│       │   │   ├── stg_title_crew_directors.sql       # Normalized director relationships 
│       │   │   ├── stg_title_crew_writers.sql         # Normalized writer relationships
│       │   │   ├── stg_title_episode.sql              # TV episode hierarchy cleaning
│       │   │   ├── stg_title_principals.sql           # Cast & crew standardization
│       │   │   ├── stg_title_ratings.sql              # Ratings validation & typing
│       │   │   ├── schema.yml                         # Model tests & documentation
│       │   │   └── sources.yml                        # Source definitions & freshness
│       │   │
│       │   ├── intermediate/                      # ⚙️ 5 Intermediate Models  
│       │   │   ├── int_title_with_ratings.sql         # Titles + ratings enrichment
│       │   │   ├── int_title_with_genres.sql          # Genre dimension modeling
│       │   │   ├── int_person_filmography.sql         # Comprehensive career data
│       │   │   ├── int_title_hierarchies.sql          # Series-episode relationships
│       │   │   ├── int_title_complete.sql             # Fully denormalized titles
│       │   │   └── schema.yml                         # Relationship tests & docs
│       │   │
│       │   └── marts/                             # 📊 4 Analytics-Ready Marts
│       │       ├── mart_top_titles.sql                # Performance & ranking analytics
│       │       ├── mart_person_career.sql             # Individual achievement metrics
│       │       ├── mart_genre_analytics.sql           # Genre trends & time series
│       │       ├── mart_series_analytics.sql          # TV series & episode insights  
│       │       └── schema.yml                         # Business metric documentation
│       │
│       ├── target/                            # 🎯 Compiled dbt Assets
│       │   ├── manifest.json                      # 🔗 Model dependencies (for Cosmos)
│       │   ├── run_results.json                   # ✅ Execution results & performance
│       │   └── compiled/                          # 📄 Generated SQL for review
│       │
│       ├── dbt_packages/                      # 📦 External Dependencies
│       │   └── dbt_utils/                         # 🛠️ dbt community utilities
│       │
│       ├── dbt_project.yml                   # ⚙️ Project configuration & materialization
│       ├── profiles.yml                      # 🔌 Database connection profiles (port 5433)
│       ├── packages.yml                      # 📋 Package dependencies (dbt-utils)
│       └── README.md                         # 📚 Detailed dbt documentation
│
├── 📁 Data Directories/                       # Local Storage (auto-created)
│   ├── imdb_data/                                # 💾 Downloaded datasets (.tsv.gz files)
│   └── imdb_schemas/                             # 🗂️ PostgreSQL table schemas (.sql files)
│
├── 🐳 Docker Configuration/                   # Containerization Setup
│   ├── Dockerfile                                # 🏗️ Astro Runtime 13.0.0 (Airflow 2.7)
│   ├── docker-compose.override.yml               # 🔧 PostgreSQL 13 configuration
│   └── .env                                      # 🌍 Environment variables
│
├── 📋 Configuration Files/                    # Project Setup
│   ├── requirements.txt                          # 🐍 Python dependencies
│   │                                             #    ├─ dbt-core==1.4.6
│   │                                             #    ├─ dbt-postgres==1.4.6  
│   │                                             #    └─ astronomer-cosmos==1.1.1
│   ├── packages.txt                              # 📦 System packages
│   ├── airflow_settings.yaml                    # ⚙️ Airflow connection configs
│   └── README.md                                 # 📖 This documentation
│
└── 🧪 Testing/                               # Quality Assurance  
    └── tests/dags/                               # 🔍 DAG validation tests
        └── test_dag_example.py                       # ✅ Basic DAG import testing
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
| `download_uploading.py` | Main production pipeline | As needed |
| `dbt/manifest.json` | Cosmos dependency mapping | After model changes |
| `dbt/profiles.yml` | Database connections | Environment changes |
| `requirements.txt` | Python dependencies | Version updates |

## 📚 Usage Guide

### Production Pipeline Execution

#### 🎯 Primary Workflow: `imdb_cosmos_pipeline`

**Single DAG handles complete ELT process:**

1. **Navigate to Airflow UI**: http://localhost:8080 (admin/admin)

2. **Enable the pipeline**:
   ```
   DAGs → imdb_cosmos_pipeline → Toggle ON
   ```

3. **Monitor execution phases**:
   ```
   Phase 1: Download Tasks    (7 parallel) →  ~10-30 min
   Phase 2: Load Tasks        (7 parallel) →  ~20-60 min  
   Phase 3: dbt Staging       (7 models)   →  ~2-5 min
   Phase 4: dbt Intermediate  (5 models)   →  ~2-5 min
   Phase 5: dbt Marts         (4 models)   →  ~1-3 min
   ```

4. **Execution results**:
   - Raw data: 7 PostgreSQL tables (`imdb_*`)
   - Staging views: 7 cleaned datasets (`stg_*`)
   - Intermediate views: 5 business logic models (`int_*`)
   - Marts tables: 4 analytics-ready datasets (`mart_*`)

### Manual dbt Operations

```powershell
# Navigate to dbt project
cd dags/dbt

# 🔄 Core Operations
dbt deps                      # Install packages
dbt parse                     # Generate manifest for Cosmos
dbt run --profiles-dir .      # Run all models

# 🎯 Layer-specific Execution
dbt run --profiles-dir . --select tag:staging       # Staging only
dbt run --profiles-dir . --select tag:intermediate  # Intermediate only  
dbt run --profiles-dir . --select tag:marts         # Marts only

# 🧪 Testing & Validation
dbt test --profiles-dir .                           # Run all tests
dbt test --profiles-dir . --select staging.*        # Test staging models
dbt test --profiles-dir . --select source:*         # Test source data

# 📊 Documentation & Analysis
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
- **Download Cache Hit Rate**: Check Airflow Variables
- **Load Performance**: Row counts vs. file sizes
- **dbt Test Results**: Data quality validation
- **Task Duration Trends**: Performance over time

## 🧩 Data Models & Analytics

### Source Data Overview

**IMDb Dataset Statistics:**
| Dataset | Records | Size (Compressed) | Primary Use |
|---------|---------|-------------------|-------------|
| `title.basics.tsv.gz` | ~10M | ~200MB | Core title information |
| `title.principals.tsv.gz` | ~57M | ~600MB | Cast and crew roles |
| `title.akas.tsv.gz` | ~35M | ~300MB | International titles |
| `name.basics.tsv.gz` | ~13M | ~150MB | Person information |
| `title.crew.tsv.gz` | ~10M | ~100MB | Directors and writers |
| `title.episode.tsv.gz` | ~7.8M | ~80MB | TV episode hierarchy |
| `title.ratings.tsv.gz` | ~1.4M | ~25MB | User ratings |

### Analytics Capabilities

#### 📊 **Available Marts**

| Mart | Analytics Focus | Key Questions Answered |
|------|----------------|------------------------|
| **`mart_top_titles`** | Performance & Rankings | • What are the highest-rated films by genre?<br>• Which titles have the most votes?<br>• How do ratings vary by release decade? |
| **`mart_person_career`** | Individual Achievements | • Who are the most prolific directors?<br>• Which actors have the longest careers?<br>• What's the average career span by profession? |
| **`mart_genre_analytics`** | Genre Trends | • How have genre preferences changed over time?<br>• Which genres consistently rate highest?<br>• What's the volume trend by genre and decade? |
| **`mart_series_analytics`** | TV Series Insights | • Which series have the most consistent ratings?<br>• How do episode ratings vary within seasons?<br>• What's the average series length by genre? |

#### 🔍 **Sample Analytics Queries**

```sql
-- Top 10 highest-rated movies with 10k+ votes
SELECT title, average_rating, num_votes, primary_genre
FROM mart_top_titles 
WHERE title_type = 'movie' AND num_votes >= 10000
ORDER BY average_rating DESC, num_votes DESC
LIMIT 10;

-- Director career analysis
SELECT primary_name, total_titles, career_span_years, avg_rating
FROM mart_person_career 
WHERE primary_profession = 'director' AND total_titles >= 5
ORDER BY avg_rating DESC, total_titles DESC;

-- Genre popularity by decade
SELECT decade, genre, title_count, avg_rating 
FROM mart_genre_analytics
WHERE decade >= 2000
ORDER BY decade DESC, title_count DESC;

-- TV series with most consistent ratings
SELECT series_title, total_episodes, avg_rating, rating_std_dev
FROM mart_series_analytics
WHERE total_episodes >= 10
ORDER BY rating_std_dev ASC, avg_rating DESC;
```

### Advanced dbt Features

**🎯 Model Tagging Strategy:**
```sql
-- Staging models
{{ config(tags=['staging']) }}

-- Intermediate models  
{{ config(tags=['intermediate']) }}

-- Marts models
{{ config(tags=['marts']) }}
```

**📊 Materialization Strategy:**
- **Staging/Intermediate**: `VIEW` (fast, always fresh)
- **Marts**: `TABLE` (performance for analytics)

**🧪 Comprehensive Testing:**
- **Data Quality**: `not_null`, `unique`, `accepted_values`
- **Relationships**: `relationships` tests between models
- **Business Logic**: Custom tests for derived fields
- **Source Freshness**: Validation of data currency

For detailed model documentation and lineage, refer to:
- [dbt Project README](./dags/dbt/README.md)
- dbt docs site: `cd dags/dbt && dbt docs serve --profiles-dir .`

## 📝 Development & Deployment

### Future Enhancements

**🔄 Pipeline Improvements:**
- [ ] **Incremental Loading**: Implement incremental models for large datasets
- [ ] **Data Quality Monitoring**: Advanced anomaly detection and alerting
- [ ] **Performance Optimization**: Parallel dbt execution and resource allocation
- [ ] **Schema Evolution**: Automatic handling of IMDb schema changes

**📊 Analytics Extensions:**
- [ ] **Real-time Dashboards**: Integration with Superset or Metabase
- [ ] **Machine Learning Models**: Rating prediction and recommendation engines
- [ ] **API Layer**: REST API for analytics data consumption
- [ ] **Data Exports**: Automated report generation and distribution

**🚀 Production Features:**
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

## 🔗 Resources & Documentation

### 📚 **Official Documentation**
- [Apache Airflow Docs](https://airflow.apache.org/docs/) - Core orchestration platform
- [dbt Documentation](https://docs.getdbt.com/) - Transformation framework
- [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/) - Airflow ↔ dbt integration
- [Astronomer Platform](https://www.astronomer.io/docs/astro/) - Managed Airflow service

### 🛠️ **Technical References**
- [IMDb Dataset Interface](https://www.imdb.com/interfaces/) - Source data documentation
- [PostgreSQL Documentation](https://www.postgresql.org/docs/) - Database platform
- [Docker Documentation](https://docs.docker.com/) - Containerization

### 📖 **Learning Resources**
- [Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/) - Best practices
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html) - Production guidelines
- [Modern Data Stack](https://www.getdbt.com/blog/future-of-the-modern-data-stack/) - Architecture principles

### 🤝 **Community & Support**
- [dbt Community](https://www.getdbt.com/community/) - Slack workspace and forums
- [Airflow Community](https://airflow.apache.org/community/) - Mailing lists and events
- [Astronomer Community](https://www.astronomer.io/community/) - Support and resources

---

## 📄 License & Credits

**Project Information:**
- **Author**: Andrea Usai
- **Version**: 2.0.0 (Production Ready)
- **Last Updated**: January 2025
- **License**: [Specify your license]

**Acknowledgments:**
- **IMDb**: For providing comprehensive entertainment datasets
- **Astronomer**: For the excellent Cosmos integration and Astro platform
- **dbt Labs**: For the powerful data transformation framework
- **Apache Airflow Community**: For the robust orchestration platform

**Data Source Attribution:**
> Information courtesy of IMDb (https://www.imdb.com). Used with permission.

---

**⭐ If this project helped you, please consider giving it a star on GitHub!**
