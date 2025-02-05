import argparse
from config import Config
from database import DatabaseManager
from fetchers.twitter_fetcher import TwitterFetcher
from fetchers.youtube_fetcher import YoutubeFetcher
from fetchers.instagram_fetcher import InstagramFetcher
import os
import logging
import sys
from datetime import datetime
import uuid

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('app.log')
        ]
    )
    return logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev_mode', action='store_true', help='Run in development mode')
    parser.add_argument('--add_user', action='store_true', help='Add a new influencer')
    return parser.parse_args()

def get_platform_handle(platform: str) -> str:
    while True:
        handle = input(f"Enter {platform} handle (press Enter to skip): ").strip()
        if not handle:
            return None
        confirm = input(f"Is '{handle}' correct for {platform}? (y/n): ").lower()
        if confirm == 'y':
            return handle

def add_new_influencer(db: DatabaseManager, logger: logging.Logger):
    """Interactive flow to add a new influencer"""
    print("\n=== Adding New Influencer ===")
    
    # Get basic information
    name = input("Enter influencer name: ").strip()
    
    # Get handles for each platform
    platforms = ['twitter', 'youtube', 'instagram', 'tiktok', 'facebook']
    handles = {}
    
    for platform in platforms:
        handle = get_platform_handle(platform)
        if handle:
            handles[f"{platform}_handle"] = handle
    
    if not handles:
        logger.error("At least one social media handle must be provided")
        return
    
    # Create influencer record
    influencer_data = {
        'id': str(uuid.uuid4()),
        'name': name,
        'active': True,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow(),
        **handles  # Generate a single dict with all metadata
    }
    
    # Save influencer to database
    try:
        db.add_influencer(influencer_data)
        logger.info(f"Successfully added influencer: {name}")
        
        # Fetch historical data
        print("\nFetching one year of historical data...")
        fetchers = {
            'twitter': TwitterFetcher(),
            'youtube': YoutubeFetcher(),
            'instagram': InstagramFetcher()
        }
        
        for platform, fetcher in fetchers.items():
            platform_handle = handles.get(f"{platform}_handle")
            if platform_handle:
                try:
                    logger.info(f"Fetching historical data for {platform}...")
                    historical_data = fetcher.fetch_user_history(
                        {'id': influencer_data['id'], 'handle': platform_handle}
                    )
                    if historical_data:
                        db.save_influencer_data(platform, historical_data)
                        logger.info(f"Successfully saved historical {platform} data")
                except Exception as e:
                    logger.error(f"Error fetching historical {platform} data: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error adding influencer: {str(e)}")
        return

def main(event=None, context=None):
    logger = setup_logging()
    
    try:
        args = parse_args()
        if args.dev_mode:
            os.environ['DEV_MODE'] = 'true'
            logger.info("Running in development mode")
        
        Config.validate()
        db = DatabaseManager()
        
        if args.add_user:
            add_new_influencer(db, logger)
            return
        
        # Fetch influencers from database
        influencers = db.get_active_influencers()
        
        if not influencers:
            logger.warning("No active influencers found")
            return
        
        fetchers = {
            'twitter': TwitterFetcher(),
            'youtube': YoutubeFetcher(),
            'instagram': InstagramFetcher()
        }
        
        for platform, fetcher in fetchers.items():
            try:
                logger.info(f"Starting data collection for {platform}")
                platform_users = [
                    {'id': inf['id'], 'handle': inf['handles'][platform]}
                    for inf in influencers
                    if platform in inf['handles']
                ]
                
                if not platform_users:
                    logger.info(f"No users found for platform {platform}")
                    continue

                # Get last update dates for all users at once
                last_updates = {}
                for user in platform_users:
                    last_update = db.get_last_update_date(user['id'])
                    if last_update:
                        last_updates[user['id']] = last_update

                # Pass last_updates to fetcher to validate before making API calls
                data = fetcher.fetch_all(platform_users, last_updates)
                
                if data:
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