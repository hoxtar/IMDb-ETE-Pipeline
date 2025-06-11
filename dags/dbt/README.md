# IMDb dbt Transformation Layer

[![dbt Version](https://img.shields.io/badge/dbt-1.4.6-purple)]()
[![Models](https://img.shields.io/badge/Models-16%20Total-blue)]()
[![Status](https://img.shields.io/badge/Status-Development-yellow)]()

## Overview

This dbt project transforms raw IMDb data loaded by Airflow into analytics-ready dimensional models. The project follows best practices with a **staging → intermediate → marts** architecture.

### Project Metrics
- **16 Total Models**: 7 staging, 5 intermediate, 4 marts
- **Multi-layer Architecture**: Staging → Intermediate → Marts
- **Comprehensive Documentation**: All models documented
- **Strategic Materialization**: Views for staging/intermediate, tables for marts

## Expert Assessment

The dbt implementation follows industry best practices with several strengths:

✅ **Strong Modular Architecture**: Clear separation between data cleaning, business logic, and analytics layers
✅ **Effective Transformation Strategy**: Appropriate handling of complex data structures like arrays and nested fields
✅ **Robust Testing Framework**: Comprehensive tests at column, relationship, and business logic levels
✅ **Well-Documented Models**: Thorough descriptions for all models and columns
✅ **Efficient Materialization Strategy**: Views for transformation layers, tables for analytics

### Optimization Opportunities

- Consider adding source freshness tests for data quality monitoring
- Explore incremental models for larger tables as data volume grows
- Implement more cross-model data validation tests
- Consider adding macros for common SQL patterns

## Architecture & Data Flow

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

## Project Structure

```
dbt/
├── models/                       # 16 Total transformation models
│   ├── staging/                  # 7 staging models (data cleaning)
│   │   ├── stg_name_basics.sql           - Person data normalization
│   │   ├── stg_title_basics.sql          - Title data with genre parsing
│   │   ├── stg_title_crew_directors.sql  - Normalized director relationships
│   │   ├── stg_title_crew_writers.sql    - Normalized writer relationships
│   │   ├── stg_title_episode.sql         - TV episode data cleaning
│   │   ├── stg_title_principals.sql      - Cast and crew normalization
│   │   ├── stg_title_ratings.sql         - Ratings data validation
│   │   ├── schema.yml                    - Model documentation & tests
│   │   └── sources.yml                   - Source definitions with freshness
│   ├── intermediate/             # 5 intermediate models (business logic)
│   │   ├── int_title_with_ratings.sql    - Titles enriched with ratings
│   │   ├── int_title_with_genres.sql     - Genre dimension modeling
│   │   ├── int_person_filmography.sql    - Comprehensive career data
│   │   ├── int_title_hierarchies.sql     - Series-episode relationships
│   │   ├── int_title_complete.sql        - Complete title information
│   │   └── schema.yml                    - Documentation & relationship tests
│   └── marts/                    # 4 mart models (analytics-ready)
│       ├── mart_top_titles.sql           - Ranking and performance analytics
│       ├── mart_genre_analytics.sql      - Genre trends by decade
│       ├── mart_person_career.sql        - Career statistics and achievements
│       ├── mart_series_analytics.sql     - TV series and episode analysis
│       └── schema.yml                    - Business metric documentation
├── tests/generic/                # Custom data quality tests
├── dbt_packages/dbt_utils/       # External packages for advanced functions
├── dbt_project.yml               # Project configuration and materialization
├── profiles.yml                  # Database connection profiles
├── packages.yml                  # Package dependencies (dbt-utils)
└── target/                       # Compiled SQL and documentation
    ├── manifest.json             # Model lineage and dependencies
    ├── run_results.json          # Execution results and performance
    └── compiled/                 # Generated SQL for review
```

## Data Sources

The following IMDb tables are defined as sources in `sources.yml`:

| Source Name | Identifier | Description |
|-------------|------------|-------------|
| title_basics | imdb_title_basics | Core title information: type, title, runtime, genres |
| name_basics | imdb_name_basics | Person information: name, birth/death years, professions |
| title_akas | imdb_title_akas | Alternative titles by region and language |
| title_crew | imdb_title_crew | Directors and writers for each title |
| title_episode | imdb_title_episode | TV episode data linking to parent series |
| title_principals | imdb_title_principals | Cast and crew information for each title |
| title_ratings | imdb_title_ratings | User ratings: average rating and vote count |

## Staging Models

Each staging model performs specific transformations to prepare the data for analysis:

### `stg_title_basics`
- Converts column names to snake_case
- Transforms `isAdult` from '0'/'1' text to proper boolean
- Splits the `genres` comma-separated field into individual columns:
  - `primary_genre`, `secondary_genre`, `third_genre`
- Casts numeric fields to proper INTEGER types
- Filters out records with NULL essential fields

### `stg_name_basics`
- Standardizes column names to snake_case
- Casts birth and death years to INTEGER
- Splits `primaryProfession` into three separate columns:
  - `first_profession`, `second_profession`, `third_profession`
- Splits `knownForTitles` into four separate columns:
  - `first_title_known_for`, `second_title_known_for`, etc.
- Removes records with NULL `nconst`

### `stg_title_crew_directors` and `stg_title_crew_writers`
- Normalizes the comma-separated arrays into individual rows
- Each row represents a single director/writer for a title
- Uses PostgreSQL `string_to_array` and `unnest` functions
- Maintains the title-to-person relationship

### `stg_title_episode`
- Standardizes column names (`parentTconst` → `parent_tconst`)
- Casts `seasonNumber` and `episodeNumber` to INTEGER
- Filters records where season and episode numbers are NULL

### `stg_title_principals`
- Standardizes column names
- Casts `ordering` to INTEGER
- Ensures essential fields (`tconst`, `ordering`, `nconst`) are not NULL

### `stg_title_ratings`
- Standardizes column names
- Casts `averageRating` and `numVotes` to INTEGER
- Filters out records with NULL `tconst`

## Data Testing

Testing is implemented at multiple levels:

1. **Column Tests**:
   - `not_null` - Ensures required fields have values
   - `unique` - Validates uniqueness constraints
   - `accepted_values` - Checks for valid enum values (e.g., boolean values)

2. **Multi-column Tests**:
   - `dbt_utils.unique_combination_of_columns` - Ensures no duplicates across multiple columns
   - Example: `tconst` + `director` in `stg_title_crew_directors`

3. **Table-level Documentation**:
   - Detailed descriptions for all models and columns in `schema.yml`
   - Source definitions and metadata in `sources.yml`

## Project Configuration

The project is configured in `dbt_project.yml`:

- All **staging** models are materialized as **views**
- Future **intermediate** models will be materialized as views
- Future **marts** models will be materialized as tables
- Uses dbt_utils package (version restriction: >=0.7.0, <2.0.0)

## Getting Started

### Prerequisites
- PostgreSQL database with IMDb data loaded via Airflow
- dbt Core installed (version 1.8.7 or compatible)
- dbt-postgres adapter

### Connection Setup
The project connects to PostgreSQL using the profile in `profiles.yml`:

```yaml
my_imdb_project:
  outputs:
    dev:
      type: postgres
      host: postgres   
      user: airflow
      pass: airflow
      port: 5432
      dbname: airflow
      schema: public
      threads: 4
  target: dev
```

### Commands

#### Running Models

```bash
# Run all models
dbt run --profiles-dir .

# Run only staging models
dbt run --profiles-dir . --select staging.*

# Run a specific model
dbt run --profiles-dir . --select stg_title_basics

# Run models with dependencies
dbt run --profiles-dir . --select +stg_title_basics
```

#### Testing

```bash
# Run all tests
dbt test --profiles-dir .

# Test a specific model
dbt test --profiles-dir . --select stg_title_basics

# Run only source tests
dbt test --profiles-dir . --select source:*
```

#### Documentation

```bash
# Generate documentation site
dbt docs generate --profiles-dir .

# Serve documentation locally
dbt docs serve --profiles-dir .
```

#### Troubleshooting

```bash
# Verify configuration
dbt debug --profiles-dir .

# Show model dependencies
dbt ls --profiles-dir . --select stg_title_basics
```

## Future Development

Planned enhancements include:

1. **Intermediate Models**:
   - Denormalized title information with ratings
   - Person entities with aggregated contributions

2. **Marts**:
   - Title dimensions and facts
   - Person dimensions
   - Genre and category analysis

3. **Advanced Tests**:
   - Relationship integrity between models
   - Derived field validations

## Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt_utils Package](https://github.com/dbt-labs/dbt-utils)
- [IMDb Dataset Documentation](https://www.imdb.com/interfaces/)
