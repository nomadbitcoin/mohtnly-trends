import os
import pandas as pd
from datetime import datetime
import logging

def save_to_csv(data: dict, platform: str, username: str):
    """Save API response to CSV"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs('raw-data', exist_ok=True)
    
    filename = f'raw-data/{platform}_{username}_{timestamp}.csv'
    pd.DataFrame([data]).to_csv(filename, index=False)
    logging.info(f"Saved raw data to {filename}") 