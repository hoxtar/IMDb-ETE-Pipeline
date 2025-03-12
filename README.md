# IMDb-ETE-Pipeline

## **Project goal**
- Create an end-to-end pipeline that extracts data from **IMDb**, cleans/transforms it through **dbt** and loads it into a DB on the Cloud.    
- Unify data and information in order to conduct in-depth analyses.
- Schedule periodic execution with **Airflow**.

## **Roadmap**
1. **Sprint 1**: setup Airflow + Docker + test DAG.
2. **Sprint 2**: ingest IMDb (download + load into DB).
3. **Sprint 3**: dbt for transformations.
4. **Sprint 4**: possible deployment in Cloud, testing, and optimisation.

## **How-to**
- **Environment and prerequisites**
  - Docker (version X)
  - Docker Compose (version X)
  - Python (version X) if using it directly
  - Any specific OS or hardware requirements

- **Usage**
  - How to clone the repo (git clone …)
  - How to launch the containers (docker-compose up -d)
  - Where to find the Airflow UI (default: http://localhost:8080)
  - How to trigger or schedule your first DAG

- **Folder structure**
  - airflow/ (DAGs, plugins, etc.)
  - dbt_project/ (dbt models, dbt_project.yml)
  - docker/ (Dockerfiles, docker-compose.yml)

**Next steps**
