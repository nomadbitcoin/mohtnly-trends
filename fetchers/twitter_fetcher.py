import requests
from typing import List, Dict
from config import Config
import logging
from datetime import datetime, timedelta
from .base_fetcher import BaseFetcher
from database import DatabaseManager
import json

class TwitterFetcher(BaseFetcher):
    def __init__(self):
        self.client_id = Config.SOCIALBLADE_CLIENT_ID
        self.token = Config.SOCIALBLADE_TOKEN
        self.base_url = "https://matrix.sbapis.com/b/twitter/statistics"
        self.logger = logging.getLogger(__name__)

    def fetch_all(self, users: List[Dict], last_updates: Dict[str, datetime]) -> List[Dict]:
        """
        Fetch data for users that haven't been updated in the last 30 days
        
        Args:
            users: List of user dictionaries with 'id' and 'handle'
            last_updates: Dictionary mapping user_id to their last update datetime
        """
        results = []
        db = DatabaseManager()
        
        for user in users:
            try:
                last_update = last_updates.get(user['id'])
                if last_update and (datetime.utcnow() - last_update) < timedelta(days=30):
                    self.logger.info(f"Skipping {user['handle']} - last update was less than 30 days ago")
                    continue

                metrics = self.fetch_user(user['handle'])
                if metrics:
                    # Add influencer_id to each metric
                    for metric in metrics:
                        metric['influencer_id'] = user['id']
                    results.extend(metrics)
                    
                    # Save metrics and update last update timestamp
                    db.save_twitter_metrics(metrics)
                    latest_timestamp = max(m['timestamp'] for m in metrics)
                    db.update_last_platform_update('twitter', user['id'], latest_timestamp)
                    
            except Exception as e:
                self.logger.error(f"Error fetching data for Twitter user {user['handle']}: {str(e)}")
            
        return results

    def fetch_user(self, username: str) -> List[Dict]:
        """Fetch recent metrics for a user with default history"""
        return self._fetch_metrics(username, history_type='default')

    def fetch_user_history(self, user: Dict) -> List[Dict]:
        """Fetch extended historical data for a user"""
        db = DatabaseManager()
        metrics = self._fetch_metrics(user['handle'], history_type='extended')
        
        if metrics:
            # Add influencer_id to each metric
            for metric in metrics:
                metric['influencer_id'] = user['id']
                
            # Save metrics and update last update timestamp
            db.save_twitter_metrics(metrics)
            latest_timestamp = max(m['timestamp'] for m in metrics)
            db.update_last_platform_update('twitter', user['id'], latest_timestamp)
            
        return metrics

    def _fetch_metrics(self, username: str, history_type: str = 'default') -> List[Dict]:
        """
        Fetch metrics from Twitter API
        
        Args:
            username: Twitter handle
            history_type: Either 'default' or 'extended'
        """
        try:
            # Comment out the original API request code
            """
            headers = {
                'query': username,
                'history': history_type,
                'clientid': self.client_id,
                'token': self.token
            }
            
            response = requests.get(
                self.base_url,
                headers=headers,
                timeout=10
            )
            
            response.raise_for_status()
            data = response.json()
            """
            
            # Instead read from local JSON file
            with open('raw-data/twitter_history_castacrypto_20250207_002803.json', 'r') as f:
                data = json.load(f)
            
            # Save raw response
            response_type = 'twitter_history' if history_type == 'extended' else 'twitter'
            self._save_raw_response(data, response_type, username)
            
            # Extract daily metrics
            metrics = []
            if data.get('data', {}).get('daily'):
                for daily_data in data['data']['daily']:
                    metric = {
                        'username': username,
                        'followers': daily_data.get('followers'),
                        'following': daily_data.get('following'),
                        'tweets': daily_data.get('tweets'),
                        'favorites': daily_data.get('favorites'),
                        'timestamp': datetime.strptime(daily_data['date'], '%Y-%m-%d')
                    }
                    metrics.append(metric)
            
            return metrics

        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout while fetching data for {username}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {username}: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid JSON response for {username}: {str(e)}")
            raise 