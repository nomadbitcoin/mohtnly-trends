import pytest
from datetime import datetime
from database import DatabaseManager
from unittest.mock import MagicMock, patch
from google.cloud import bigquery

@pytest.fixture
def db_manager():
    with patch('google.cloud.bigquery.Client') as mock_client:
        manager = DatabaseManager()
        manager.client = mock_client
        yield manager

@pytest.fixture
def mock_query_job():
    mock = MagicMock()
    mock.result.return_value = []
    return mock

def test_add_influencer_with_unique_handles(db_manager, mock_query_job):
    # Setup
    db_manager.client.query.return_value = mock_query_job
    db_manager.client.load_table_from_dataframe.return_value.result.return_value = None
    
    influencer_data = {
        'id': 'test-id-1',
        'name': 'Test User',
        'twitter_handle': 'testuser',
        'instagram_handle': 'testuser',
        'active': True,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
    }
    
    # Execute
    db_manager.add_influencer(influencer_data)
    
    # Verify
    assert db_manager.client.query.called
    assert db_manager.client.load_table_from_dataframe.called

def test_add_influencer_with_duplicate_handle_different_user(db_manager, mock_query_job):
    # Setup
    class MockResult:
        def __init__(self, id, platform):
            self.id = id
            self.platform = platform
    
    mock_query_job.result.return_value = [
        MockResult('existing-id', 'twitter')  # Simulate existing handle
    ]
    db_manager.client.query.return_value = mock_query_job
    
    influencer_data = {
        'id': 'test-id-2',
        'name': 'Test User 2',
        'twitter_handle': 'duplicate_handle',
        'active': True,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
    }
    
    # Execute & Verify
    with pytest.raises(ValueError) as exc_info:
        db_manager.add_influencer(influencer_data)
    
    assert "Handle(s) already exist in other accounts" in str(exc_info.value)

def test_add_influencer_same_handle_same_user(db_manager, mock_query_job):
    # Setup
    class MockResult:
        def __init__(self, id, platform):
            self.id = id
            self.platform = platform
    
    user_id = 'test-id-3'
    mock_query_job.result.return_value = [
        MockResult(user_id, 'twitter')  # Same user_id
    ]
    db_manager.client.query.return_value = mock_query_job
    db_manager.client.load_table_from_dataframe.return_value.result.return_value = None
    
    influencer_data = {
        'id': user_id,  # Same user_id
        'name': 'Test User 3',
        'twitter_handle': 'same_handle',
        'instagram_handle': 'same_handle',
        'active': True,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow()
    }
    
    # Execute
    db_manager.add_influencer(influencer_data)  # Should not raise exception
    
    # Verify
    assert db_manager.client.load_table_from_dataframe.called

def test_update_handles_with_duplicate_in_other_account(db_manager, mock_query_job):
    # Setup
    class MockResult:
        def __init__(self, id, platform):
            self.id = id
            self.platform = platform
    
    mock_query_job.result.return_value = [
        MockResult('other-id', 'instagram')  # Handle exists in another account
    ]
    db_manager.client.query.return_value = mock_query_job
    
    updates = {
        'instagram_handle': 'duplicate_handle'
    }
    
    # Execute & Verify
    with pytest.raises(ValueError) as exc_info:
        db_manager.update_influencer_handles('test-id-4', updates)
    
    assert "Handle(s) already exist in other accounts" in str(exc_info.value)

def test_update_handles_no_duplicates(db_manager, mock_query_job):
    # Setup
    db_manager.client.query.return_value = mock_query_job
    mock_query_job.result.return_value = []  # No duplicates found
    
    updates = {
        'twitter_handle': 'new_handle',
        'instagram_handle': 'new_handle'
    }
    
    # Execute
    db_manager.update_influencer_handles('test-id-5', updates)
    
    # Verify
    assert db_manager.client.query.called
    # Should have called query twice: once for duplicate check, once for update
    assert db_manager.client.query.call_count == 2