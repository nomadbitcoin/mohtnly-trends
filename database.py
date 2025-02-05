from google.cloud import bigquery
from typing import List, Dict, Optional
import pandas as pd
import os
from config import Config
import logging
from google.api_core import retry
from datetime import datetime
import uuid
from google.oauth2 import service_account

class DatabaseManager:
    def __init__(self):
        if Config.DEV_MODE:
            self.client = None
        else:
            try:
                self.client = bigquery.Client(
                    project=Config.BIGQUERY_PROJECT_ID,
                    credentials=service_account.Credentials.from_service_account_file(
                        Config.GOOGLE_CREDENTIALS_PATH
                    )
                )
            except Exception as e:
                self.logger.error(f"Failed to initialize BigQuery client: {str(e)}")
                raise
            
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

            # Transform data to match platform-specific metrics schema
            metrics_data = []
            table_id = f"{self.project_id}.{self.dataset_id}.{platform}_metrics"
            
            for item in data:
                metric = {
                    'id': str(uuid.uuid4()),
                    'influencer_id': item.get('influencer_id'),
                    'timestamp': item.get('timestamp') or datetime.utcnow(),
                    'created_at': datetime.utcnow()
                }
                
                # Add platform-specific fields with proper NULL handling
                if platform == 'twitter':
                    metric.update({
                        'followers': item.get('followers', 0),
                        'following': item.get('following', 0),
                        'tweets': item.get('tweets', 0),
                        'engagement_rate': item.get('engagement_rate', 0.0)
                    })
                elif platform == 'youtube':
                    metric.update({
                        'subscribers': item.get('subscribers', 0),
                        'total_views': item.get('total_views', 0),
                        'videos': item.get('videos', 0),
                        'engagement_rate': item.get('engagement_rate', 0.0)
                    })
                elif platform == 'instagram':
                    metric.update({
                        'followers': item.get('followers', 0),
                        'following': item.get('following', 0),
                        'posts': item.get('posts', 0),
                        'engagement_rate': item.get('engagement_rate', 0.0)
                    })
                
                metrics_data.append(metric)

            df = pd.DataFrame(metrics_data)
            
            # Define schema to ensure proper data types
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=self._get_metrics_schema(platform)
            )

            @retry.Retry(predicate=retry.if_transient_error)
            def load_table():
                job = self.client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                return job.result()

            load_table()
            
            # Update last platform update timestamp
            for item in data:
                self.update_last_platform_update(
                    platform, 
                    item['influencer_id'], 
                    item.get('timestamp') or datetime.utcnow()
                )
                
            self.logger.info(f"Successfully saved {len(data)} records for {platform}")

        except Exception as e:
            self.logger.error(f"Error saving data for {platform}: {str(e)}")
            raise

    def _get_metrics_schema(self, platform: str) -> List[bigquery.SchemaField]:
        """Get the schema for a specific platform's metrics table"""
        base_schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("influencer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("engagement_rate", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        if platform == 'twitter':
            base_schema.extend([
                bigquery.SchemaField("followers", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("following", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("tweets", "INTEGER", mode="NULLABLE"),
            ])
        elif platform == 'youtube':
            base_schema.extend([
                bigquery.SchemaField("subscribers", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("total_views", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("videos", "INTEGER", mode="NULLABLE"),
            ])
        elif platform == 'instagram':
            base_schema.extend([
                bigquery.SchemaField("followers", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("following", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("posts", "INTEGER", mode="NULLABLE"),
            ])
        
        return base_schema

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

            # Ensure required fields
            required_fields = {
                'id': str(uuid.uuid4()) if 'id' not in influencer_data else influencer_data['id'],
                'name': influencer_data['name'],
                'active': True if 'active' not in influencer_data else influencer_data['active'],
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            
            # Merge with optional handle fields and ensure NULL for missing platforms
            platforms = ['twitter', 'youtube', 'instagram', 'tiktok', 'facebook']
            handle_fields = {}
            for platform in platforms:
                handle_key = f"{platform}_handle"
                last_updated_key = f"last_{platform}_updated"
                handle_fields[handle_key] = influencer_data.get(handle_key)
                handle_fields[last_updated_key] = None  # Initialize all last_updated fields as NULL
            
            influencer_data = {
                **required_fields,
                **handle_fields
            }

            table_id = f"{self.project_id}.{self.dataset_id}.influencers"
            df = pd.DataFrame([influencer_data])
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("twitter_handle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("instagram_handle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("youtube_handle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("tiktok_handle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("facebook_handle", "STRING", mode="NULLABLE"),
                    bigquery.SchemaField("last_twitter_updated", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("last_instagram_updated", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("last_youtube_updated", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("last_tiktok_updated", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("last_facebook_updated", "TIMESTAMP", mode="NULLABLE"),
                    bigquery.SchemaField("active", "BOOLEAN", mode="REQUIRED"),
                    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
                    bigquery.SchemaField("updated_at", "TIMESTAMP", mode="REQUIRED"),
                ]
            )

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
                AND {platform}_handle IS NOT NULL  -- Only update if platform handle exists
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("timestamp", "TIMESTAMP", timestamp),
                    bigquery.ScalarQueryParameter("influencer_id", "STRING", influencer_id)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            result = query_job.result()
            
            if result.num_rows_modified > 0:
                self.logger.info(f"Updated last_{platform}_updated for influencer {influencer_id}")
            else:
                self.logger.warning(f"No update performed for {platform} - handle might not exist for influencer {influencer_id}")
            
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
                AND {platform}_handle IS NOT NULL  -- Only check if platform handle exists
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("influencer_id", "STRING", influencer_id)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            results = list(query_job.result())
            
            if not results:  # No record found or no handle for this platform
                return None
            
            return results[0][f"last_{platform}_updated"]

        except Exception as e:
            self.logger.error(f"Error fetching last_{platform}_updated: {str(e)}")
            return None 