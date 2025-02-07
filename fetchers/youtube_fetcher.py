import requests
from typing import List, Dict
from config import Config
import logging
from datetime import datetime, timedelta, timezone
from .base_fetcher import BaseFetcher
from database import DatabaseManager

class YoutubeFetcher(BaseFetcher):
    def __init__(self):
        self.client_id = Config.SOCIALBLADE_CLIENT_ID
        self.token = Config.SOCIALBLADE_TOKEN
        self.base_url = "https://matrix.sbapis.com/b/youtube/statistics"
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
                    db.save_youtube_metrics(metrics)
                    latest_timestamp = max(m['timestamp'] for m in metrics)
                    db.update_last_platform_update('youtube', user['id'], latest_timestamp)
                    
            except Exception as e:
                self.logger.error(f"Error fetching data for YouTube user {user['handle']}: {str(e)}")
            
        return results

    def fetch_user(self, user: Dict) -> List[Dict]:
        """
        Fetch recent metrics for a user if needed
        
        Args:
            user: Dictionary with 'id' and 'handle'
        """
        db = DatabaseManager()
        
        # Check last update time
        last_update = db.get_platform_last_update('youtube', user['id'])
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
            db.save_youtube_metrics(metrics)
            latest_timestamp = max(m['timestamp'] for m in metrics)
            if latest_timestamp.tzinfo is None:
                latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)
            db.update_last_platform_update('youtube', user['id'], latest_timestamp)
            
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
            db.save_youtube_metrics(metrics)
            latest_timestamp = max(m['timestamp'] for m in metrics)
            
        return metrics

    def _fetch_metrics(self, channel_id: str, history_type: str = 'default') -> List[Dict]:
        """
        Fetch metrics from YouTube API
        
        Args:
            channel_id: YouTube channel ID
            history_type: Either 'default' or 'extended'
        """
        try:
            headers = {
                'query': channel_id,
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
            
            # Save raw response
            response_type = 'youtube_history' if history_type == 'extended' else 'youtube'
            self._save_raw_response(data, response_type, channel_id)
            
            # Extract daily metrics
            metrics = []
            if data.get('daily'):
                for daily_data in data['daily']:
                    # Parse date and make it timezone-aware
                    timestamp = datetime.strptime(daily_data['date'], '%Y-%m-%d')
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                    
                    metric = {
                        'channel_id': channel_id,
                        'subscribers': daily_data.get('subs'),
                        'total_views': daily_data.get('views'),
                        'timestamp': timestamp
                    }
                    metrics.append(metric)
            
            return metrics

        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout while fetching data for {channel_id}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {channel_id}: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid JSON response for {channel_id}: {str(e)}")
            raise 