import sys
import os
from unittest.mock import MagicMock

# Add project root to Python path
project_root = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, project_root)

class MockDAG:
    """Mock DAG class that supports context manager protocol"""
    def __init__(self, *args, **kwargs):
        self.dag_id = kwargs.get('dag_id', 'test_dag')
        self.default_args = kwargs.get('default_args', {})
        self.tasks = []
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

class MockPythonOperator:
    """Mock PythonOperator"""
    def __init__(self, *args, **kwargs):
        self.task_id = kwargs.get('task_id', 'test_task')
        self.python_callable = kwargs.get('python_callable')
        self.op_kwargs = kwargs.get('op_kwargs', {})

def pytest_configure():
    """Configure pytest with necessary mocks"""
    mock_modules = {
        'airflow': MagicMock(),
        'airflow.operators': MagicMock(),
        'airflow.operators.python': MagicMock(),
        'airflow.utils': MagicMock(),
        'airflow.utils.dates': MagicMock(),
        'airflow.utils.log': MagicMock(),
        'airflow.utils.log.logging_mixin': MagicMock(),
        'airflow.hooks': MagicMock(),
        'airflow.hooks.postgres_hook': MagicMock(),
        'airflow.models': MagicMock(),
    }
    
    for module_name, mock_module in mock_modules.items():
        sys.modules[module_name] = mock_module
    
    # Set up specific mocks
    sys.modules['airflow'].DAG = MockDAG
    sys.modules['airflow.models'].DAG = MockDAG
    sys.modules['airflow.operators.python'].PythonOperator = MockPythonOperator
    
    # Mock datetime utilities
    mock_datetime = MagicMock()
    sys.modules['airflow.utils.dates'].days_ago = lambda x: mock_datetime
    
    # Mock logging
    mock_log = MagicMock()
    sys.modules['airflow.utils.log.logging_mixin'].LoggingMixin = type('LoggingMixin', (), {'log': mock_log})
