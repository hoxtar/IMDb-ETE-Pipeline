FROM apache/airflow:latest-python3.8

USER root

ARG AIRFLOW_HOME=/opt/airflow

# Add DAGs
ADD dags /opt/airflow/dags

# Add your custom security manager and JSON permissions
ADD config/security/dag_permissions.json /opt/airflow/config/security/dag_permissions.json

#  ADD your override module to Airflow's expected location
ADD config/security/override_security_manager.py /opt/airflow/airflow/www/security/CustomSecurityManager.py

# Switch to airflow user BEFORE installing packages
USER airflow

# Install your Python requirements
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org -r requirements.txt

# Switch back (good practice)
USER ${AIRFLOW_UID}
