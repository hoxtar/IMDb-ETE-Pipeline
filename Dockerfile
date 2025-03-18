FROM apache/airflow:latest-python3.8

USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags

# Switch to airflow user BEFORE installing packages
USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# Switch back to the proper UID (optional if needed for compatibility)
USER ${AIRFLOW_UID}
