# Utility Scripts

![Scripts Status](https://img.shields.io/badge/Status-Active-brightgreen)
![Coverage](https://img.shields.io/badge/Implementation-33%25-yellow)
![Last Updated](https://img.shields.io/badge/Updated-May%202025-blue)

This directory contains utility scripts for managing, monitoring, and troubleshooting the IMDb data pipeline infrastructure.

## Implementation Status

| Component | Status | Description | Progress |
|-----------|--------|-------------|----------|
| Database Validation | Complete | PostgreSQL connectivity and permissions check | 100% |
| Environment Reset | Planned | Development environment reset automation | 0% |
| Airflow Monitoring | Planned | Pipeline health monitoring utilities | 0% |
| Performance Profiling | Planned | DAG execution performance analysis | 0% |

## Available Scripts

### Production Ready
- **`database_check.sql`** - Comprehensive PostgreSQL database validation
  - Verifies current database and schema context
  - Checks IMDb table existence in system catalog
  - Validates Airflow user permissions and privileges
  - **Usage**: Execute in DBeaver or any PostgreSQL client

### Planned Development
- **`reset_environment.sh`** - Complete development environment reset
  - Docker container cleanup and rebuild
  - Database schema reset with fresh data
  - Airflow metadata database initialization
  
- **`monitor_airflow.py`** - Real-time pipeline monitoring
  - DAG execution status tracking
  - Task failure alerting and logging
  - Performance metrics collection
  
- **`performance_profiler.py`** - Pipeline performance analysis
  - Execution time profiling per task
  - Resource utilization monitoring
  - Bottleneck identification and reporting

## Quick Start

### Database Health Check
```sql
-- Connect to your PostgreSQL instance and run:
\i scripts/database_check.sql
```

### Expected Output
```sql
current_database | current_schema
-----------------|---------------
imdb_pipeline    | public

table_schema | table_name
-------------|------------------
public       | imdb_name_basics
public       | imdb_title_akas
public       | imdb_title_basics
public       | imdb_title_crew
public       | imdb_title_episode
public       | imdb_title_principals
public       | imdb_title_ratings
```

## Development Roadmap

### Phase 1: Core Utilities (Current)
- [x] Database connectivity validation
- [ ] Environment reset automation
- [ ] Basic monitoring scripts

### Phase 2: Advanced Monitoring
- [ ] Real-time DAG status dashboard
- [ ] Automated failure notifications
- [ ] Performance bottleneck detection

### Phase 3: DevOps Integration
- [ ] CI/CD pipeline health checks
- [ ] Automated deployment validation
- [ ] Production monitoring suite

## Technical Requirements

- **PostgreSQL Client**: psql or DBeaver for SQL script execution
- **Python 3.8+**: For monitoring utilities (when implemented)
- **Docker**: For environment reset functionality
- **Airflow CLI**: For advanced pipeline management

## Metrics & KPIs

| Metric | Current Value | Target |
|--------|---------------|--------|
| Script Coverage | 33% (1/3) | 100% |
| Automation Level | Basic | Advanced |
| Response Time | Manual | < 5 minutes |
| Error Detection | Reactive | Proactive |

---

**Last Updated**: May 28, 2025  
**Maintainer**: Development Team  
**Version**: 1.0.0-alpha
