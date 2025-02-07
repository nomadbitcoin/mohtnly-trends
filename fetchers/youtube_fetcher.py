import requests
from typing import List, Dict
from config import Config
import logging
from datetime import datetime, timedelta
from .base_fetcher import BaseFetcher

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
        for user in users:
            try:
                last_update = last_updates.get(user['id'])
                if last_update and (datetime.utcnow() - last_update) < timedelta(days=30):
                    self.logger.info(f"Skipping {user['handle']} - last update was less than 30 days ago")
                    continue

                data = self.fetch_user(user['handle'])
                if data:
                    data['influencer_id'] = user['id']
                    results.append(data)
            except Exception as e:
                self.logger.error(f"Error fetching data for YouTube user {user['handle']}: {str(e)}")
        return results

    def fetch_user(self, channel_id: str) -> Dict:
        try:
            headers = {
                'query': channel_id,
                'history': 'default',
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
            self._save_raw_response(data, 'youtube', channel_id)
            
            return {
                'channel_id': channel_id,
                'subscribers': data.get('subscribers'),
                'total_views': data.get('views'),
                'videos': data.get('videos'),
                'engagement_rate': data.get('engagement_rate'),
                'timestamp': data.get('timestamp')
            }
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout while fetching data for {channel_id}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {channel_id}: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid JSON response for {channel_id}: {str(e)}")
            raise

    def fetch_user_history(self, user: Dict) -> List[Dict]:
        """Fetch one year of historical data for a user"""
        try:
            headers = {
                'query': user['handle'],
                'history': 'extended',
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
            self._save_raw_response(data, 'youtube_history', user['handle'])
            
            metrics = []
            for history_point in data.get('history', []):
                metric = {
                    'influencer_id': user['id'],
                    'channel_id': user['handle'],
                    'subscribers': history_point.get('subscribers'),
                    'total_views': history_point.get('views'),
                    'videos': history_point.get('videos'),
                    'engagement_rate': history_point.get('engagement_rate'),
                    'timestamp': history_point.get('timestamp')
                }
                metrics.append(metric)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error fetching history for {user['handle']}: {str(e)}")
            raise 