import pytest
import os
from unittest.mock import patch

@pytest.fixture(autouse=True)
def mock_env_vars():
    """Mock environment variables for testing"""
    with patch.dict(os.environ, {
        'BIGQUERY_PROJECT_ID': 'test-project',
        'BIGQUERY_DATASET': 'test-dataset',
        'GOOGLE_APPLICATION_CREDENTIALS': 'test-credentials.json'
    }):
        yield 