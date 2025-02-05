import pytest
from fetchers.twitter_fetcher import TwitterFetcher
import vcr

@pytest.fixture
def fetcher():
    return TwitterFetcher()

@vcr.use_cassette('fixtures/vcr_cassettes/twitter_user.yaml')
def test_fetch_user(fetcher):
    data = fetcher.fetch_user('castacrypto')
    assert data is not None
    assert 'username' in data
    assert 'followers' in data
    assert 'following' in data
    assert 'tweets' in data
    assert 'timestamp' in data 