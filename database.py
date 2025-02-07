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
        self.project_id = Config.BIGQUERY_PROJECT_ID
        self.dataset_id = Config.BIGQUERY_DATASET
        self.logger = logging.getLogger(__name__)
        
        try:
            self.client = bigquery.Client(
                project=Config.BIGQUERY_PROJECT_ID,
                credentials=service_account.Credentials.from_service_account_file(
                    Config.GOOGLE_CREDENTIALS_PATH
                )
            )
            # Initialize tables after client and logger are set up
            init_database(self.client)
            
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {str(e)}")
            raise

    def get_active_influencers(self) -> List[Dict]:
        """Fetch all active influencers from BigQuery"""
        try:

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

            # In dev_mode, also save to CSV but continue with normal operation
            if Config.DEV_MODE:
                self._save_to_csv(platform, data)

            # Normal BigQuery save
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
                        'influencer_handle': item.get('username'),
                        'followers': item.get('followers'),
                        'following': item.get('following'),
                        'tweets': item.get('tweets'),
                        'favorites': item.get('favorites')
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
            
            self.logger.info(f"Successfully saved {len(data)} records for {platform}")

        except Exception as e:
            self.logger.error(f"Error saving data for {platform}: {str(e)}")
            raise

    def _get_metrics_schema(self, platform: str) -> List[bigquery.SchemaField]:
        """Get the schema for a specific platform's metrics table"""
        base_schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("influencer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        if platform == 'twitter':
            base_schema.extend([
                bigquery.SchemaField("influencer_handle", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("followers", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("following", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("tweets", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("favorites", "INTEGER", mode="NULLABLE"),
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

    def check_existing_handles(self, handles: Dict[str, str]) -> List[str]:
        """
        Check if any handles already exist in the database
        Returns list of platforms where duplicates were found
        """
        try:
            duplicates = []
            for platform_handle, handle in handles.items():
                if not handle:
                    continue
                    
                query = f"""
                    SELECT COUNT(*) as count
                    FROM `{self.project_id}.{self.dataset_id}.influencers`
                    WHERE {platform_handle} = @handle
                    AND active = TRUE
                """
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("handle", "STRING", handle)
                    ]
                )
                
                query_job = self.client.query(query, job_config=job_config)
                result = next(query_job.result())
                
                if result.count > 0:
                    platform = platform_handle.replace('_handle', '')
                    duplicates.append(f"{platform} ({handle})")
                    
            return duplicates
            
        except Exception as e:
            self.logger.error(f"Error checking existing handles: {str(e)}")
            raise

    def add_influencer(self, influencer_data: Dict):
        """Add a new influencer to the database"""
        try:
            if Config.DEV_MODE:
                self._save_to_csv('influencers', [influencer_data])

            # Ensure all required fields are present with default values
            complete_data = {
                'id': influencer_data['id'],
                'name': influencer_data['name'],
                'twitter_handle': influencer_data.get('twitter_handle'),
                'instagram_handle': influencer_data.get('instagram_handle'),
                'youtube_handle': influencer_data.get('youtube_handle'),
                'tiktok_handle': influencer_data.get('tiktok_handle'),
                'facebook_handle': influencer_data.get('facebook_handle'),
                'last_twitter_updated': None,
                'last_instagram_updated': None,
                'last_youtube_updated': None,
                'last_tiktok_updated': None,
                'last_facebook_updated': None,
                'active': influencer_data.get('active', True),
                'created_at': influencer_data.get('created_at', datetime.utcnow()),
                'updated_at': influencer_data.get('updated_at', datetime.utcnow())
            }

            # Check for duplicate handles
            handles = {k: v for k, v in complete_data.items() if k.endswith('_handle')}
            query_parts = []
            params = []
            
            for platform_handle, handle in handles.items():
                if not handle:
                    continue
                    
                query_parts.append(f"""
                    SELECT DISTINCT id, '{platform_handle.replace('_handle', '')}' as platform
                    FROM `{self.project_id}.{self.dataset_id}.influencers`
                    WHERE {platform_handle} = @{platform_handle}
                    AND active = TRUE
                """)
                params.append(
                    bigquery.ScalarQueryParameter(platform_handle, "STRING", handle)
                )
            
            if query_parts:
                query = " UNION ALL ".join(query_parts)
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=params
                )
                
                query_job = self.client.query(query, job_config=job_config)
                results = query_job.result()
                
                # Group duplicates by user_id to check if they belong to different users
                duplicates_by_user = {}
                for row in results:
                    duplicates_by_user.setdefault(row.id, []).append(row.platform)
                
                # Filter out the current user's duplicates
                other_users_duplicates = {
                    user_id: platforms 
                    for user_id, platforms in duplicates_by_user.items()
                    if user_id != influencer_data.get('id')
                }
                
                if other_users_duplicates:
                    duplicate_details = []
                    for platforms in other_users_duplicates.values():
                        for platform in platforms:
                            handle = handles[f"{platform}_handle"]
                            duplicate_details.append(f"{platform} ({handle})")
                    raise ValueError(f"Handle(s) already exist in other accounts: {', '.join(duplicate_details)}")

            # Insert the new influencer
            table_id = f"{self.project_id}.{self.dataset_id}.influencers"
            df = pd.DataFrame([complete_data])
            
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
            query_job.result()
            
            self.logger.info(f"Updated last_{platform}_updated for influencer {influencer_id}")
            
        except Exception as e:
            self.logger.error(f"Error updating last_{platform}_updated: {str(e)}")
            raise

    def get_platform_last_update(self, platform: str, influencer_id: str) -> Optional[datetime]:
        """Get the last update date for a specific platform"""
        try:

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

    def update_influencer_handles(self, influencer_id: str, updates: Dict[str, str]):
        """Update social media handles for an influencer"""
        try:

            # Check for duplicate handles in other accounts
            query_parts = []
            params = []
            
            for platform_handle, handle in updates.items():
                if not handle:
                    continue
                    
                query_parts.append(f"""
                    SELECT DISTINCT id, '{platform_handle.replace('_handle', '')}' as platform
                    FROM `{self.project_id}.{self.dataset_id}.influencers`
                    WHERE {platform_handle} = @{platform_handle}
                    AND id != @influencer_id
                    AND active = TRUE
                """)
                params.append(
                    bigquery.ScalarQueryParameter(platform_handle, "STRING", handle)
                )
            
            if query_parts:
                query = " UNION ALL ".join(query_parts)
                
                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        *params,
                        bigquery.ScalarQueryParameter("influencer_id", "STRING", influencer_id)
                    ]
                )
                
                query_job = self.client.query(query, job_config=job_config)
                results = query_job.result()
                
                # Group duplicates by user_id
                duplicates_by_user = {}
                for row in results:
                    duplicates_by_user.setdefault(row.id, []).append(row.platform)
                
                if duplicates_by_user:
                    duplicate_details = []
                    for platforms in duplicates_by_user.values():
                        for platform in platforms:
                            handle = updates[f"{platform}_handle"]
                            duplicate_details.append(f"{platform} ({handle})")
                    raise ValueError(f"Handle(s) already exist in other accounts: {', '.join(duplicate_details)}")

            # Build SET clause
            set_clause = ", ".join([
                f"{handle_key} = @{handle_key}"
                for handle_key in updates.keys()
            ])
            
            query = f"""
                UPDATE `{self.project_id}.{self.dataset_id}.influencers`
                SET {set_clause},
                    updated_at = @updated_at
                WHERE id = @influencer_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    *params,
                    bigquery.ScalarQueryParameter("updated_at", "TIMESTAMP", datetime.utcnow()),
                    bigquery.ScalarQueryParameter("influencer_id", "STRING", influencer_id)
                ]
            )
            
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            
            self.logger.info(f"Successfully updated handles for influencer {influencer_id}")
            
        except Exception as e:
            self.logger.error(f"Error updating influencer handles: {str(e)}")
            raise 

    def save_twitter_metrics(self, metrics: List[Dict]):
        """
        Save Twitter metrics to BigQuery
        
        Args:
            metrics: List of Twitter metrics with fields:
                - influencer_id: str
                - username: str
                - followers: int
                - following: int
                - tweets: int
                - favorites: int
                - timestamp: datetime
        """
        try:
            if not metrics:
                self.logger.warning("No Twitter metrics to save")
                return

            # In dev_mode, also save to CSV
            if Config.DEV_MODE:
                self._save_to_csv('twitter', metrics)

            # Prepare data for BigQuery
            metrics_data = []
            for item in metrics:
                metric = {
                    'id': str(uuid.uuid4()),
                    'influencer_id': item['influencer_id'],
                    'influencer_handle': item['username'],
                    'followers': item['followers'],
                    'following': item['following'],
                    'tweets': item['tweets'],
                    'favorites': item['favorites'],
                    'timestamp': item['timestamp'],
                    'created_at': datetime.utcnow()
                }
                metrics_data.append(metric)

            df = pd.DataFrame(metrics_data)
            table_id = f"{self.project_id}.{self.dataset_id}.twitter_metrics"
            
            # Define schema
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("influencer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("influencer_handle", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("followers", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("following", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("tweets", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("favorites", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            ]

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=schema
            )

            @retry.Retry(predicate=retry.if_transient_error)
            def load_table():
                job = self.client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                return job.result()

            load_table()
            self.logger.info(f"Successfully saved {len(metrics)} Twitter metrics")

        except Exception as e:
            self.logger.error(f"Error saving Twitter metrics: {str(e)}")
            raise

    def save_youtube_metrics(self, metrics: List[Dict]):
        """
        Save YouTube metrics to BigQuery
        
        Args:
            metrics: List of YouTube metrics with fields:
                - influencer_id: str
                - channel_id: str
                - subscribers: int
                - total_views: int
                - timestamp: datetime
        """
        try:
            if not metrics:
                self.logger.warning("No YouTube metrics to save")
                return

            # In dev_mode, also save to CSV
            if Config.DEV_MODE:
                self._save_to_csv('youtube', metrics)

            # Prepare data for BigQuery
            metrics_data = []
            for item in metrics:
                metric = {
                    'id': str(uuid.uuid4()),
                    'influencer_id': item['influencer_id'],
                    'channel_id': item['channel_id'],
                    'subscribers': item['subscribers'],
                    'total_views': item['total_views'],
                    'timestamp': item['timestamp'],
                    'created_at': datetime.utcnow()
                }
                metrics_data.append(metric)

            df = pd.DataFrame(metrics_data)
            table_id = f"{self.project_id}.{self.dataset_id}.youtube_metrics"
            
            # Define schema
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("influencer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("channel_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("subscribers", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("total_views", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
            ]

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=schema
            )

            @retry.Retry(predicate=retry.if_transient_error)
            def load_table():
                job = self.client.load_table_from_dataframe(
                    df, table_id, job_config=job_config
                )
                return job.result()

            load_table()
            self.logger.info(f"Successfully saved {len(metrics)} YouTube metrics")

        except Exception as e:
            self.logger.error(f"Error saving YouTube metrics: {str(e)}")
            raise

def init_database(client):
    logger = logging.getLogger(__name__)
    try:
        # Read the SQL script
        with open('schema/create_tables.sql', 'r') as f:
            sql_script = f.read()
        
        # Replace placeholders with Config values
        sql_script = sql_script.format(
            project_id=Config.BIGQUERY_PROJECT_ID,
            dataset=Config.BIGQUERY_DATASET
        )
        
        # Execute each statement separately
        statements = [s.strip() for s in sql_script.split(';') if s.strip()]
        for statement in statements:
            try:
                query_job = client.query(statement)
                query_job.result()  # Wait for the query to complete
            except Exception as e:
                logger.error(f"Error executing statement: {statement}")
                logger.error(f"Error details: {str(e)}")
                raise
                
        logger.info("Database tables initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing database tables: {str(e)}")
        raise

def get_client():
    client = bigquery.Client()
    init_database(client)
    return client 