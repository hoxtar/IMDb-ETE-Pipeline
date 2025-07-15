"""
dbt Task Management Module
=========================

Handles dbt transformations using Astronomer Cosmos with proper
layered architecture and task orchestration.

Key Features:
- Cosmos-based dbt task groups
- Layered transformation architecture
- Proper task dependencies
- Environment-aware configuration

Author: Andrea Usai
Version: 2.0.0
Last Updated: January 2025
"""

import os
from pathlib import Path
from datetime import timedelta
from typing import List

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import LoadMode, TestBehavior, InvocationMode

from airflow.models import BaseOperator
from airflow.utils.log.logging_mixin import LoggingMixin

try:
    from .config import POSTGRES_CONN_ID
except ImportError:
    # Handle case when module is parsed directly by Airflow
    import sys
    import os
    sys.path.append(os.path.dirname(__file__))
    from config import POSTGRES_CONN_ID

# Initialize logger
log = LoggingMixin().log


class DbtTaskManager:
    """
    Manages dbt transformations using Astronomer Cosmos with layered architecture.
    """
    
    def __init__(self, dag):
        self.dag = dag
        self.dbt_project_path = Path(__file__).parent.parent / "dbt"
        self.dbt_profiles_path = self.dbt_project_path
        
        # Set environment variable for dbt profiles
        os.environ['DBT_PROFILES_DIR'] = str(self.dbt_profiles_path)
        
        # Cosmos configuration following best practices
        self.profile_config = ProfileConfig(
            profile_name="imdb_analytics",
            target_name="dev",  # Standard target name
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id=POSTGRES_CONN_ID,
                profile_args={
                    "schema": "public"
                },
                disable_event_tracking=True  # Recommended for production
            )
        )
        
        self.project_config = ProjectConfig(
            dbt_project_path=self.dbt_project_path,
            project_name="imdb_analytics",
            install_dbt_deps=True  # Recommended for automatic dependency management
        )
        
        self.execution_config = ExecutionConfig(
            dbt_executable_path="dbt",
            # Note: env_vars moved to project_config as per Cosmos 1.3+ recommendations
        )
        
        self.render_config = RenderConfig(
            load_method=LoadMode.DBT_LS,  # Recommended method for performance
            invocation_mode=InvocationMode.DBT_RUNNER,  # Default since Cosmos 1.9
            select=["path:models/"],
            exclude=["tag:skip"],
            emit_datasets=True,  # Enable data-aware scheduling
            test_behavior=TestBehavior.AFTER_ALL,  # Run tests after all models complete
            enable_mock_profile=True  # Default behavior for better performance
        )
    
    def create_dbt_task_group(self, upstream_tasks: List[BaseOperator]) -> DbtTaskGroup:
        """
        Create a comprehensive dbt task group with layered transformation architecture.
        
        Implements three-layer dbt architecture:
        1. Staging Layer: Raw data cleaning and standardization
        2. Intermediate Layer: Business logic and calculated fields
        3. Marts Layer: Final analytical tables for reporting
        
        Args:
            upstream_tasks: List of tasks that must complete before dbt runs
            
        Returns:
            DbtTaskGroup: Configured dbt task group with proper dependencies
        """
        log.info("[DBT] Creating dbt task group with Cosmos")
        
        # Create the main dbt task group
        dbt_task_group = DbtTaskGroup(
            group_id="dbt_transformations",
            project_config=self.project_config,
            profile_config=self.profile_config,
            execution_config=self.execution_config,
            render_config=self.render_config,
            operator_args={
                "retries": 2,
                "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(hours=1),
                "depends_on_past": False,
                "pool": "default_pool"
            },
            dag=self.dag
        )
        
        # Set up dependencies - dbt runs after all data loading is complete
        for task in upstream_tasks:
            task >> dbt_task_group
        
        log.info("[DBT] dbt task group created successfully")
        return dbt_task_group
    
    def create_layered_dbt_groups(self, upstream_tasks: List[BaseOperator]) -> tuple:
        """
        Create separate task groups for each dbt layer for better control and monitoring.
        
        This approach provides:
        - Clear separation of concerns
        - Better error isolation
        - Granular monitoring and alerting
        - Ability to run layers independently
        
        Args:
            upstream_tasks: List of tasks that must complete before dbt runs
        """
        log.info("[DBT] Creating layered dbt task groups")
        
        # Staging layer - raw data cleaning
        staging_group = DbtTaskGroup(
            group_id="dbt_staging",
            project_config=self.project_config,
            profile_config=self.profile_config,
            execution_config=self.execution_config,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=["path:models/staging/"],
                exclude=["tag:skip"]
            ),
            operator_args={
                "retries": 2,
                "retry_delay": timedelta(minutes=3),
                "execution_timeout": timedelta(minutes=30),
                "pool": "default_pool"
            },
            dag=self.dag
        )
        
        # Intermediate layer - business logic
        intermediate_group = DbtTaskGroup(
            group_id="dbt_intermediate",
            project_config=self.project_config,
            profile_config=self.profile_config,
            execution_config=self.execution_config,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=["path:models/intermediate/"],
                exclude=["tag:skip"]
            ),
            operator_args={
                "retries": 2,
                "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(minutes=45),
                "pool": "default_pool"
            },
            dag=self.dag
        )
        
        # Marts layer - final analytical tables
        marts_group = DbtTaskGroup(
            group_id="dbt_marts",
            project_config=self.project_config,
            profile_config=self.profile_config,
            execution_config=self.execution_config,
            render_config=RenderConfig(
                load_method=LoadMode.DBT_LS,
                select=["path:models/marts/"],
                exclude=["tag:skip"]
            ),
            operator_args={
                "retries": 2,
                "retry_delay": timedelta(minutes=5),
                "execution_timeout": timedelta(minutes=30),
                "pool": "default_pool"
            },
            dag=self.dag
        )
        
        # Set up layer dependencies
        for task in upstream_tasks:
            task >> staging_group  # type: ignore
        
        staging_group >> intermediate_group >> marts_group  # type: ignore
        
        log.info("[DBT] Layered dbt task groups created successfully")
        
        return staging_group, intermediate_group, marts_group


def create_dbt_task_group_simple(dag, upstream_tasks: List[BaseOperator]) -> DbtTaskGroup:
    """
    Simple factory function for creating dbt task group.
    
    Args:
        dag: Airflow DAG object
        upstream_tasks: List of upstream tasks
        
    Returns:
        DbtTaskGroup: Configured dbt task group
    """
    manager = DbtTaskManager(dag)
    return manager.create_dbt_task_group(upstream_tasks)


def create_dbt_task_groups_layered(dag, upstream_tasks: List[BaseOperator]) -> tuple:
    """
    Factory function for creating layered dbt task groups.
    
    Args:
        dag: Airflow DAG object
        upstream_tasks: List of upstream tasks
        
    Returns:
        tuple: (staging_group, intermediate_group, marts_group)
    """
    manager = DbtTaskManager(dag)
    return manager.create_layered_dbt_groups(upstream_tasks)
