import requests
from typing import List, Dict
from config import Config
import logging
from datetime import datetime, timedelta

class TiktokFetcher:
    def __init__(self):
        self.client_id = Config.SOCIALBLADE_CLIENT_ID
        self.token = Config.SOCIALBLADE_TOKEN
        self.base_url = "https://matrix.sbapis.com/b/tiktok/statistics"
        self.logger = logging.getLogger(__name__)

    def fetch_all(self, users: List[Dict], last_updates: Dict[str, datetime]) -> List[Dict]:
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
                self.logger.error(f"Error fetching data for TikTok user {user['handle']}: {str(e)}")
        return results

    def fetch_user(self, username: str) -> Dict:
        try:
            headers = {
                'query': username,
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
            
            return {
                'username': username,
                'followers': data.get('followers'),
                'following': data.get('following'),
                'likes': data.get('likes'),
                'videos': data.get('videos'),
                'engagement_rate': data.get('engagement_rate'),
                'timestamp': data.get('timestamp')
            }
        except requests.exceptions.Timeout:
            self.logger.error(f"Timeout while fetching data for {username}")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request failed for {username}: {str(e)}")
            raise
        except ValueError as e:
            self.logger.error(f"Invalid JSON response for {username}: {str(e)}")
            raise

    def fetch_user_history(self, user: Dict) -> List[Dict]:
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
            
            metrics = []
            for history_point in data.get('history', []):
                metric = {
                    'influencer_id': user['id'],
                    'username': user['handle'],
                    'followers': history_point.get('followers'),
                    'following': history_point.get('following'),
                    'likes': history_point.get('likes'),
                    'videos': history_point.get('videos'),
                    'engagement_rate': history_point.get('engagement_rate'),
                    'timestamp': history_point.get('timestamp')
                }
                metrics.append(metric)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error fetching history for {user['handle']}: {str(e)}")
            raise 