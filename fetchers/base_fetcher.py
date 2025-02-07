import json
import os
from datetime import datetime
from typing import Dict
import logging
from config import Config

class BaseFetcher:
    def _save_raw_response(self, data: Dict, platform: str, username: str) -> None:
        """
        Save raw API response to JSON file when in dev mode
        
        Args:
            data: JSON response data
            platform: Social media platform name
            username: User handle/username
        """
        # Only save responses if dev_mode is enabled
        if not getattr(Config, 'DEV_MODE', False):
            return
            
        try:
            # Create raw-data directory if it doesn't exist
            os.makedirs('raw-data', exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"{platform}_{username}_{timestamp}.json"
            filepath = os.path.join('raw-data', filename)
            
            # Save JSON response
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logging.getLogger(__name__).error(f"Error saving raw response for {platform} user {username}: {str(e)}") 