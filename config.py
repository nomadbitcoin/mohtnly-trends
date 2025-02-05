import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # SocialBlade credentials
    SOCIALBLADE_CLIENT_ID = os.getenv('SOCIALBLADE_CLIENT_ID')
    SOCIALBLADE_TOKEN = os.getenv('SOCIALBLADE_TOKEN')
    
    # Google Cloud settings
    BIGQUERY_PROJECT_ID = os.getenv('BIGQUERY_PROJECT_ID')
    BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET')
    GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    # Application settings
    DEV_MODE = os.getenv('DEV_MODE', 'false').lower() == 'true'

    @classmethod
    def validate(cls):
        """Validate required configuration"""
        required_vars = [
            'SOCIALBLADE_CLIENT_ID',
            'SOCIALBLADE_TOKEN',
            'BIGQUERY_PROJECT_ID',
            'BIGQUERY_DATASET',
            'GOOGLE_APPLICATION_CREDENTIALS'
        ]
        
        missing = [var for var in required_vars if not getattr(cls, var.lower(), None)]
        
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        if not cls.DEV_MODE and not os.path.exists(cls.GOOGLE_CREDENTIALS_PATH):
            raise FileNotFoundError(
                f"Google Cloud credentials file not found at: {cls.GOOGLE_CREDENTIALS_PATH}"
            ) 