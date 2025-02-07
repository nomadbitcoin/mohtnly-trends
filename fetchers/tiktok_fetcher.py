import requests
from typing import List, Dict
from config import Config
import logging
from datetime import datetime, timedelta, timezone
from .base_fetcher import BaseFetcher
from database import DatabaseManager
import json

class TiktokFetcher(BaseFetcher):
    def __init__(self):
        self.client_id = Config.SOCIALBLADE_CLIENT_ID
        self.token = Config.SOCIALBLADE_TOKEN
        self.base_url = "https://matrix.sbapis.com/b/tiktok/statistics"
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

                metrics = self._fetch_metrics(user['handle'], history_type='default')
                if metrics:
                    # Add influencer_id to each metric
                    for metric in metrics:
                        metric['influencer_id'] = user['id']
                    results.extend(metrics)
                    
                    # Save metrics and update last update timestamp
                    db.save_tiktok_metrics(metrics)
                    latest_timestamp = max(m['timestamp'] for m in metrics)
                    db.update_last_platform_update('tiktok', user['id'], latest_timestamp)
                    
            except Exception as e:
                self.logger.error(f"Error fetching data for TikTok user {user['handle']}: {str(e)}")
            
        return results

    def fetch_user(self, user: Dict) -> List[Dict]:
        """
        Fetch recent metrics for a user if needed
        
        Args:
            user: Dictionary with 'id' and 'handle'
        """
        db = DatabaseManager()
        
        # Check last update time
        last_update = db.get_platform_last_update('tiktok', user['id'])
        if last_update:
            # Convert last_update to UTC if it's naive
            if last_update.tzinfo is None:
                last_update = last_update.replace(tzinfo=timezone.utc)
            
            # Get current time in UTC
            now = datetime.now(timezone.utc)
            
            if (now - last_update) < timedelta(days=30):
                self.logger.info(f"Skipping {user['handle']} - last update was less than 30 days ago")
                return []

        metrics = self._fetch_metrics(user['handle'], history_type='default')
        if metrics:
            # Add influencer_id to each metric
            for metric in metrics:
                metric['influencer_id'] = user['id']
            
            # Save metrics and update last update timestamp
            db.save_tiktok_metrics(metrics)
            latest_timestamp = max(m['timestamp'] for m in metrics)
            # Ensure timestamp is UTC aware
            if latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)
            db.update_last_platform_update('tiktok', user['id'], latest_timestamp)
            
        return metrics

    def fetch_user_history(self, user: Dict) -> List[Dict]:
        """
        Fetch extended historical data for a user
        Always fetches regardless of last update time since this is for initialization
        """
        db = DatabaseManager()
        metrics = self._fetch_metrics(user['handle'], history_type='extended')
        
        if metrics:
            # Add influencer_id to each metric
            for metric in metrics:
                metric['influencer_id'] = user['id']
            
            # Save metrics and update last update timestamp
            db.save_tiktok_metrics(metrics)
            latest_timestamp = max(m['timestamp'] for m in metrics)
            db.update_last_platform_update('tiktok', user['id'], latest_timestamp)
            
        return metrics

    def _fetch_metrics(self, username: str, history_type: str = 'default') -> List[Dict]:
        """
        Fetch metrics from TikTok API
        
        Args:
            username: TikTok handle
            history_type: Either 'default' or 'extended'
        """
        try:
            # Comment out the original API request code
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
            
            # # Instead read from local JSON file
            # with open('raw-data/tiktok_castacrypto_mock.json', 'r') as f:
            #     data = json.load(f)
            
            # Save raw response
            response_type = 'tiktok_history' if history_type == 'extended' else 'tiktok'
            self._save_raw_response(data, response_type, username)
            
            # Extract daily metrics
            metrics = []
            if data.get('data', {}).get('daily'):
                for daily_data in data['data']['daily']:
                    # Parse date and make it timezone-aware
                    timestamp = datetime.strptime(daily_data['date'], '%Y-%m-%d')
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                    
                    metric = {
                        'username': username,
                        'followers': daily_data.get('followers'),
                        'following': daily_data.get('following'),
                        'likes': daily_data.get('likes'),
                        'uploads': daily_data.get('uploads'),
                        'timestamp': timestamp
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