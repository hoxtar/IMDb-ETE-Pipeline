import json
import os
from airflow.www.security import AirflowSecurityManager
from flask_appbuilder.security.sqla.models import User

class CustomSecurityManager(AirflowSecurityManager):
    def __init__(self, appbuilder):
        super().__init__(appbuilder)
        self.dag_permissions = self.load_dag_permissions()

    def load_dag_permissions(self):
        try:
            permissions_file = "/opt/airflow/config/security/dag_permissions.json"
            with open(permissions_file, "r") as f:
                return json.load(f)
        except Exception as e:
            self.log.error(f"Failed to load DAG permissions: {e}")
            return {}

    def has_access_dag(self, dag_id: str, user: User) -> bool:
        username = user.username

        if username in self.dag_permissions:
            allowed_dags = self.dag_permissions[username]
            if "*" in allowed_dags or dag_id in allowed_dags:
                return True
            else:
                return False
        else:
            return False
