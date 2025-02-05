from google.cloud import bigquery
from typing import List, Dict, Optional
import pandas as pd
import os
from config import Config
import logging
from google.api_core import retry
from datetime import datetime
import uuid

class DatabaseManager:
    def __init__(self):
        self.client = None if Config.DEV_MODE else bigquery.Client()
        self.project_id = Config.BIGQUERY_PROJECT_ID
        self.dataset_id = Config.BIGQUERY_DATASET
        self.logger = logging.getLogger(__name__)

    def get_active_influencers(self) -> List[Dict]:
        """Fetch all active influencers from BigQuery"""
        try:
            if Config.DEV_MODE:
                return self._get_dev_influencers()

            query = f"""
                SELECT 
                    id,
                    name,
                    twitter_handle,
                    instagram_handle,
                    youtube_handle,
                    tiktok_handle,
                    facebook_handle
                FROM `{self.project_id}.{self.dataset_id}.influencers`
                WHERE active = TRUE
            """
            
            query_job = self.client.query(query)
            results = query_job.result()
            
            influencers = []
            for row in results:
                influencer = dict(row.items())
                # Filter out None values
                handles = {
                    platform: handle for platform, handle in {
                        'twitter': influencer.get('twitter_handle'),
                        'instagram': influencer.get('instagram_handle'),
                        'youtube': influencer.get('youtube_handle'),
                        'tiktok': influencer.get('tiktok_handle'),
                        'facebook': influencer.get('facebook_handle')
                    }.items() if handle
                }
                if handles:  # Only include influencers with at least one handle
                    influencer['handles'] = handles
                    influencers.append(influencer)
            
            self.logger.info(f"Fetched {len(influencers)} active influencers")
            return influencers
            
        except Exception as e:
            self.logger.error(f"Error fetching influencers: {str(e)}")
            raise

    def save_influencer_data(self, platform: str, data: List[Dict]):
        try:
            if not data:
                self.logger.warning(f"No data to save for platform: {platform}")
                return

            if Config.DEV_MODE:
                self._save_to_csv(platform, data)
                return

            # Transform data to match social_metrics schema
            metrics_data = []
            for item in data:
                metric = {
                    'id': str(uuid.uuid4()),
                    'influencer_id': item.get('influencer_id'),
                    'platform': platform,
                    'followers': item.get('followers'),
                    'following': item.get('following'),
                    'posts': item.get('tweets') if platform == 'twitter' else item.get('posts'),
                    'engagement_rate': item.get('engagement_rate'),
                    'timestamp': item.get('timestamp'),
                    'created_at': datetime.utcnow()
                }
                metrics_data.append(metric)

            df = pd.DataFrame(metrics_data)
            table_id = f"{self.project_id}.{self.dataset_id}.social_metrics"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
            )

            @retry.Retry(predicate=retry.if_transient_error)
            def load_table():
                job = self.client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                return job.result()

            load_table()
            self.logger.info(f"Successfully saved {len(data)} records for {platform}")

        except Exception as e:
            self.logger.error(f"Error saving data for {platform}: {str(e)}")
            raise

    def _get_dev_influencers(self) -> List[Dict]:
        """Return sample influencers for development mode"""
        return [
            {
                'id': '1',
                'name': 'Sample Influencer 1',
                'handles': {
                    'twitter': 'elonmusk',
                    'youtube': 'MrBeast',
                    'instagram': 'cristiano'
                }
            }
        ]

    def _save_to_csv(self, platform: str, data: List[Dict]):
        try:
            df = pd.DataFrame(data)
            os.makedirs('dev_data', exist_ok=True)
            output_file = f'dev_data/{platform}_metrics.csv'
            df.to_csv(output_file, index=False)
            self.logger.info(f"Successfully saved {len(data)} records to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving CSV for {platform}: {str(e)}")
            raise

    def add_influencer(self, influencer_data: Dict):
        """Add a new influencer to the database"""
        try:
            if Config.DEV_MODE:
                self._save_to_csv('influencers', [influencer_data])
                return

            table_id = f"{self.project_id}.{self.dataset_id}.influencers"
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("name", "STRING"),
                    bigquery.SchemaField("twitter_handle", "STRING"),
                    bigquery.SchemaField("instagram_handle", "STRING"),
                    bigquery.SchemaField("youtube_handle", "STRING"),
                    bigquery.SchemaField("tiktok_handle", "STRING"),
                    bigquery.SchemaField("facebook_handle", "STRING"),
                    bigquery.SchemaField("active", "BOOLEAN"),
                    bigquery.SchemaField("created_at", "TIMESTAMP"),
                    bigquery.SchemaField("updated_at", "TIMESTAMP"),
                ]
            )

            df = pd.DataFrame([influencer_data])
            job = self.client.load_table_from_dataframe(
                df, table_id, job_config=job_config
            )
            job.result()
            
            self.logger.info(f"Successfully added influencer: {influencer_data['name']}")
        
        except Exception as e:
            self.logger.error(f"Error adding influencer to database: {str(e)}")
            raise

    def get_last_update_date(self, influencer_id: str) -> Optional[datetime]:
        """Get the last update date for an influencer"""
        try:
            if Config.DEV_MODE:
                return None

            query = f"""
                SELECT MAX(timestamp) as last_update
                FROM `{self.project_id}.{self.dataset_id}.social_metrics`
                WHERE influencer_id = @influencer_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("influencer_id", "STRING", influencer_id)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            result = next(query_job.result())
            
            return result.last_update

        except Exception as e:
            self.logger.error(f"Error fetching last update date: {str(e)}")
            return None 

    def update_last_platform_update(self, platform: str, influencer_id: str, timestamp: datetime):
        """Update the last update timestamp for a specific platform"""
        try:
            if Config.DEV_MODE:
                return

            query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.influencers`
                SET last_{platform}_updated = @timestamp,
                    updated_at = @timestamp
                WHERE id = @influencer_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
                    bigquery.ScalarQueryParameter("influencer_id", "STRING", influencer_id)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            
            self.logger.info(f"Updated last_{platform}_updated for influencer {influencer_id}")
            
        except Exception as e:
            self.logger.error(f"Error updating last_{platform}_updated: {str(e)}")
            raise

    def get_platform_last_update(self, platform: str, influencer_id: str) -> Optional[datetime]:
        """Get the last update date for a specific platform"""
        try:
            if Config.DEV_MODE:
                return None

            query = f"""
                SELECT last_{platform}_updated
                FROM `{self.project_id}.{self.dataset_id}.influencers`
                WHERE id = @influencer_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("influencer_id", "STRING", influencer_id)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            result = next(query_job.result())
            
            return result[f"last_{platform}_updated"]

        except Exception as e:
            self.logger.error(f"Error fetching last_{platform}_updated: {str(e)}")
            return None 