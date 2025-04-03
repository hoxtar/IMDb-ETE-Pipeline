FROM apache/airflow:latest-python3.8

# Use root user to install system packages and copy files
USER root

ARG AIRFLOW_HOME=/opt/airflow

# --------------------------------------------------
# Install system dependencies (e.g. git)
# --------------------------------------------------
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# --------------------------------------------------
# Copy Airflow components into the container
# --------------------------------------------------

# DAGs folder
COPY airflow/dags ${AIRFLOW_HOME}/dags

# Custom DAG permissions JSON
COPY airflow/config/auth/dag_permissions.json /opt/airflow/config/auth/dag_permissions.json

# Custom SecurityManager override
COPY airflow/config/auth/CustomSecurityManager.py /opt/airflow/config/auth/CustomSecurityManager.py


# --------------------------------------------------
# Python dependencies (switch to airflow user first)
# --------------------------------------------------

USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# --------------------------------------------------
# Best practice: drop back to Airflow's UID
# --------------------------------------------------

USER ${AIRFLOW_UID}
