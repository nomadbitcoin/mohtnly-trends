import argparse
import os
import logging
from config import Config
from database import DatabaseManager
from fetchers.twitter_fetcher import TwitterFetcher
from fetchers.youtube_fetcher import YoutubeFetcher
from fetchers.instagram_fetcher import InstagramFetcher
from fetchers.tiktok_fetcher import TiktokFetcher
import pandas as pd
from datetime import datetime
from cli import add_influencer, edit_influencer, save_to_csv

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('app.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Social Media Influencer Data Fetcher')
    parser.add_argument('--dev_mode', action='store_true', help='Run in development mode')
    parser.add_argument('--add_user', action='store_true', help='Add a new influencer')
    parser.add_argument('--edit_user', action='store_true', help='Edit existing influencer')
    parser.add_argument('--save_csv', action='store_true', help='Save API responses to CSV')
    
    args = parser.parse_args()
    
    if args.dev_mode:
        os.environ['DEV_MODE'] = 'true'
    
    logger = setup_logging()
    
    try:
        Config.validate()
        
        # Handle CLI commands
        if args.add_user:
            add_influencer()
            return
        elif args.edit_user:
            edit_influencer()
            return
        
        # Main data collection flow
        db = DatabaseManager()
        fetchers = {
            'twitter': TwitterFetcher(),
            'youtube': YoutubeFetcher(),
            'instagram': InstagramFetcher(),
            'tiktok': TiktokFetcher()
        }
        
        influencers = db.get_active_influencers()
        if not influencers:
            logger.warning("No active influencers found")
            return
            
        for platform, fetcher in fetchers.items():
            try:
                logger.info(f"Starting data collection for {platform}")
                platform_users = [
                    {'id': inf['id'], 'handle': inf['handles'][platform]} 
                    for inf in influencers 
                    if platform in inf.get('handles', {})
                ]
                
                if not platform_users:
                    logger.info(f"No users found for {platform}")
                    continue
                
                # Get last update dates for all users at once
                last_updates = {}
                for user in platform_users:
                    last_update = db.get_platform_last_update(platform, user['id'])
                    if last_update:
                        last_updates[user['id']] = last_update
                
                # Pass last_updates to fetcher to validate before making API calls
                data = fetcher.fetch_all(platform_users, last_updates)
                
                if data:
                    # Save to CSV if requested
                    if args.save_csv:
                        for item in data:
                            save_to_csv(item, platform, item.get('username') or item.get('channel_id'))
                    
                    # Save to database
                    db.save_influencer_data(platform, data)
                    logger.info(f"Successfully processed {len(data)} records for {platform}")
                else:
                    logger.warning(f"No data collected for {platform}")
                    
            except Exception as e:
                logger.error(f"Error processing {platform}: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Critical error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main() 