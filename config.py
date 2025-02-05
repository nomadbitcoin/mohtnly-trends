import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    SOCIALBLADE_API_KEY = os.getenv('SOCIALBLADE_API_KEY')
    BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')
    BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
    DEV_MODE = os.getenv('DEV_MODE', 'false').lower() == 'true'
    
    @staticmethod
    def validate():
        if not Config.SOCIALBLADE_API_KEY:
            raise ValueError("SOCIALBLADE_API_KEY must be set")
        if not Config.BIGQUERY_PROJECT_ID or not Config.BIGQUERY_DATASET:
            raise ValueError("BIGQUERY_PROJECT_ID and BIGQUERY_DATASET must be set when not in dev mode") 