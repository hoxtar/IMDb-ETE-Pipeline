import pytest
import psycopg2
from unittest.mock import Mock, patch, mock_open, MagicMock
import sys
import os

# Add the project root to sys.path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Mock Airflow modules before importing the DAG
class MockDAG:
    def __init__(self, *args, **kwargs):
        self.dag_id = kwargs.get('dag_id', 'test_dag')
        self.default_args = kwargs.get('default_args', {})
        self.tasks = []
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

class MockPythonOperator:
    def __init__(self, *args, **kwargs):
        self.task_id = kwargs.get('task_id', 'test_task')
        self.python_callable = kwargs.get('python_callable')
        self.op_kwargs = kwargs.get('op_kwargs', {})
    
    def __rshift__(self, other):
        """Support for >> operator (task dependencies)"""
        return other
    
    def __lshift__(self, other):
        """Support for << operator (task dependencies)"""
        return other

class MockLoggingMixin:
    log = MagicMock()

# Set up the mocks before importing
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.operators'] = MagicMock()
sys.modules['airflow.operators.python'] = MagicMock()
sys.modules['airflow.utils'] = MagicMock()
sys.modules['airflow.utils.dates'] = MagicMock()
sys.modules['airflow.utils.log'] = MagicMock()
sys.modules['airflow.utils.log.logging_mixin'] = MagicMock()
sys.modules['airflow.hooks'] = MagicMock()
sys.modules['airflow.hooks.postgres_hook'] = MagicMock()
sys.modules['airflow.models'] = MagicMock()

# Set up specific mocks
sys.modules['airflow'].DAG = MockDAG
sys.modules['airflow.models'].DAG = MockDAG
sys.modules['airflow.operators.python'].PythonOperator = MockPythonOperator
sys.modules['airflow.utils.dates'].days_ago = lambda x: MagicMock()
sys.modules['airflow.utils.log.logging_mixin'].LoggingMixin = MockLoggingMixin

# Import the function after setting up the mocks
from project_airflow.dags.imdb_download_dag import setup_table_schema

class TestSetupTableSchema:
    def test_setup_table_schema_valid_sql(self):
        """Test that valid CREATE TABLE SQL executes successfully"""
        mock_cursor = Mock()
        valid_sql = 'CREATE TABLE test_table (id INT, name TEXT);'
        
        with patch('builtins.open', mock_open(read_data=valid_sql)):
            setup_table_schema(mock_cursor, 'fake_path.sql', 'test_table')
        
        mock_cursor.execute.assert_called_once_with(valid_sql)

    def test_setup_table_schema_invalid_sql(self):
        """Test that invalid SQL (non-CREATE TABLE) raises ValueError"""
        mock_cursor = Mock()
        invalid_sql = 'DROP TABLE test_table;'
        
        with patch('builtins.open', mock_open(read_data=invalid_sql)):
            with pytest.raises(ValueError, match="Invalid SQL content in schema file"):
                setup_table_schema(mock_cursor, 'fake_path.sql', 'test_table')

    def test_setup_table_schema_file_not_found(self):
        """Test that missing schema file raises appropriate error"""
        mock_cursor = Mock()
        
        with patch('builtins.open', side_effect=FileNotFoundError("File not found")):
            with pytest.raises(FileNotFoundError):
                setup_table_schema(mock_cursor, 'nonexistent.sql', 'test_table')
