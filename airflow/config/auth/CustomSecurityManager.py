import json
import os

# Base class for Airflow's RBAC-based security
from airflow.www.security import AirflowSecurityManager

# Airflow's User model
from flask_appbuilder.security.sqla.models import User


class CustomSecurityManager(AirflowSecurityManager):
    """
    Custom security manager to control DAG-level access dynamically
    based on a JSON file mapping usernames to allowed DAGs.
    """

    def __init__(self, appbuilder):
        # Initialize Airflow's security manager
        super().__init__(appbuilder)

        # Load DAG permissions from JSON file once during startup
        self.dag_permissions = self.load_dag_permissions()

    def load_dag_permissions(self):
        """
        Loads the DAG permissions from a JSON file.
        Format expected:
        {
            "username1": ["dag1", "dag2"],
            "username2": ["*"],  # full access
            ...
        }
        """
        try:
            # Updated path
            permissions_file = "/opt/airflow/config/auth/dag_permissions.json"
            
            with open(permissions_file, "r") as f:
                return json.load(f)

        except Exception as e:
            self.log.error(f"[SECURITY] Failed to load DAG permissions: {e}")
            return {}

    def has_access_dag(self, dag_id: str, user: User) -> bool:
        """
        Override method to define whether a given user has access
        to a specific DAG based on the loaded JSON permissions.
        """
        username = user.username

        # Check if the user has a permissions list
        if username in self.dag_permissions:
            allowed_dags = self.dag_permissions[username]

            # Allow access if wildcard (*) or exact match
            if "*" in allowed_dags or dag_id in allowed_dags:
                return True
            else:
                return False
        else:
            # No permission entry found for the user
            return False
