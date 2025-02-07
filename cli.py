import logging
from datetime import datetime
import os
from database import DatabaseManager
from config import Config
import uuid
import pandas as pd
from utils import save_to_csv
from fetchers.twitter_fetcher import TwitterFetcher
from fetchers.youtube_fetcher import YoutubeFetcher
from fetchers.instagram_fetcher import InstagramFetcher
from fetchers.tiktok_fetcher import TiktokFetcher

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

def save_to_csv(data: dict, platform: str, username: str):
    """Save API response to CSV"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    os.makedirs('raw-data', exist_ok=True)
    
    filename = f'raw-data/{platform}_{username}_{timestamp}.csv'
    pd.DataFrame([data]).to_csv(filename, index=False)
    logging.info(f"Saved raw data to {filename}")

def prompt_handle(platform: str, same_handle: bool = False, previous_handle: str = None) -> str:
    """Prompt for social media handle"""
    if same_handle and previous_handle:
        return previous_handle
    
    handle = input(f"Enter {platform} handle (press Enter to skip): ").strip()
    return handle if handle else None

def add_influencer():
    """Interactive CLI to add a new influencer"""
    db = DatabaseManager()
    logger = logging.getLogger(__name__)
    
    name = input("Enter influencer name: ").strip()
    if not name:
        logger.error("Name is required")
        return

    # Get first handle
    platforms = ['twitter', 'youtube', 'instagram', 'tiktok', 'facebook']
    first_platform = platforms[0]
    first_handle = prompt_handle(first_platform)
    
    # Ask if same handle for all platforms
    same_handle = False
    if first_handle:
        same_for_all = input("Use this handle for all platforms? (y/N): ").strip().lower()
        same_handle = same_for_all == 'y'

    # Build handles dict
    handles = {}
    if first_handle:
        handles[f"{first_platform}_handle"] = first_handle
    
    # Get remaining handles
    for platform in platforms[1:]:
        handle = prompt_handle(platform, same_handle, first_handle)
        if handle:
            handles[f"{platform}_handle"] = handle

    if not handles:
        logger.error("At least one social media handle is required")
        return

    # Create influencer data
    influencer_data = {
        'id': str(uuid.uuid4()),
        'name': name,
        'active': True,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow(),
        **handles
    }

    try:
        # First add the influencer to the database
        db.add_influencer(influencer_data)
        logger.info(f"Successfully added influencer: {name}")
        
        # Then fetch historical data for each platform
        if handles.get('twitter_handle'):
            try:
                twitter_fetcher = TwitterFetcher()
                twitter_fetcher.fetch_user_history({
                    'id': influencer_data['id'],
                    'handle': handles['twitter_handle']
                })
                logger.info(f"Successfully fetched Twitter history for {handles['twitter_handle']}")
            except Exception as e:
                logger.error(f"Error fetching Twitter history: {str(e)}")

        # Add similar blocks for other platforms when implemented
        # if handles.get('youtube_handle'):
        #     from fetchers.youtube_fetcher import YoutubeFetcher
        #     youtube_fetcher = YoutubeFetcher()
        #     ...
        # if handles.get('instagram_handle'):
        #     from fetchers.instagram_fetcher import InstagramFetcher
        #     instagram_fetcher = InstagramFetcher()
        #     ...
                
    except ValueError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"Error in add_influencer flow: {str(e)}")

def edit_influencer():
    """Interactive CLI to edit an existing influencer"""
    db = DatabaseManager()
    logger = logging.getLogger(__name__)
    
    # List active influencers
    influencers = db.get_active_influencers()
    if not influencers:
        logger.error("No active influencers found")
        return
        
    print("\nAvailable influencers:")
    for i, inf in enumerate(influencers, 1):
        print(f"{i}. {inf['name']}")
    
    # Select influencer
    while True:
        try:
            choice = int(input("\nSelect influencer number: "))
            if 1 <= choice <= len(influencers):
                influencer = influencers[choice - 1]
                break
            print("Invalid choice")
        except ValueError:
            print("Please enter a number")
    
    # Show current handles
    print(f"\nCurrent handles for {influencer['name']}:")
    platforms = ['twitter', 'youtube', 'instagram', 'tiktok', 'facebook']
    for platform in platforms:
        handle_key = f"{platform}_handle"
        current = influencer.get(handle_key, 'Not set')
        print(f"{platform.title()}: {current}")
    
    # Update handles
    updates = {}
    print("\nEnter new handles (press Enter to keep current):")
    for platform in platforms:
        handle_key = f"{platform}_handle"
        new_handle = input(f"{platform.title()}: ").strip()
        if new_handle:
            updates[handle_key] = new_handle
    
    if not updates:
        logger.info("No changes made")
        return
    
    try:
        # Update influencer data
        db.update_influencer_handles(influencer['id'], updates)
        logger.info(f"Successfully updated handles for {influencer['name']}")
    except ValueError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"Error updating influencer: {str(e)}")

def fetch_user_history():
    """Interactive CLI to fetch history for a specific influencer"""
    db = DatabaseManager()
    logger = logging.getLogger(__name__)
    
    # List active influencers
    influencers = db.get_active_influencers()
    if not influencers:
        logger.error("No active influencers found")
        return
        
    print("\nAvailable influencers:")
    for i, inf in enumerate(influencers, 1):
        print(f"{i}. {inf['name']}")
    
    # Select influencer
    while True:
        try:
            choice = int(input("\nSelect influencer number: "))
            if 1 <= choice <= len(influencers):
                influencer = influencers[choice - 1]
                break
            print("Invalid choice")
        except ValueError:
            print("Please enter a number")
    
    # Show available platforms for the influencer
    platforms = ['twitter', 'youtube', 'instagram', 'tiktok', 'facebook']
    available_platforms = [p for p in platforms if p in influencer.get('handles', {})]
    
    if not available_platforms:
        logger.error(f"No social media handles found for {influencer['name']}")
        return
    
    print("\nAvailable platforms:")
    for i, platform in enumerate(available_platforms, 1):
        handle = influencer['handles'][platform]
        print(f"{i}. {platform.title()} ({handle})")
    
    # Select platform
    while True:
        try:
            choice = int(input("\nSelect platform number: "))
            if 1 <= choice <= len(available_platforms):
                selected_platform = available_platforms[choice - 1]
                break
            print("Invalid choice")
        except ValueError:
            print("Please enter a number")
    
    try:
        # Initialize appropriate fetcher
        fetchers = {
            'twitter': TwitterFetcher(),
            'youtube': YoutubeFetcher(),
            'instagram': InstagramFetcher(),
            'tiktok': TiktokFetcher()
        }
        
        fetcher = fetchers.get(selected_platform)
        if not fetcher:
            logger.error(f"Fetcher not implemented for {selected_platform}")
            return
            
        # Prepare user data for fetcher
        user_data = {
            'id': influencer['id'],
            'handle': influencer['handles'][selected_platform]
        }
        
        # Fetch history using the appropriate fetcher
        history = fetcher.fetch_user_history(user_data)
        
        if not history:
            logger.info(f"No history found for {influencer['name']} on {selected_platform}")
            return
        
        # Save to CSV
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"history_{selected_platform}_{user_data['handle']}_{timestamp}.csv"
        
        df = pd.DataFrame(history)
        df.to_csv(filename, index=False)
        logger.info(f"Successfully exported history to {filename}")
        
    except Exception as e:
        logger.error(f"Error fetching history: {str(e)}")

def fetch_user_metrics():
    """Interactive CLI to fetch current metrics for a specific user"""
    db = DatabaseManager()
    logger = logging.getLogger(__name__)
    
    # List active influencers
    influencers = db.get_active_influencers()
    if not influencers:
        logger.error("No active influencers found")
        return
        
    print("\nAvailable influencers:")
    for i, inf in enumerate(influencers, 1):
        print(f"{i}. {inf['name']}")
    
    # Select influencer
    while True:
        try:
            choice = int(input("\nSelect influencer number: "))
            if 1 <= choice <= len(influencers):
                influencer = influencers[choice - 1]
                break
            print("Invalid choice")
        except ValueError:
            print("Please enter a number")
    
    # Fetch metrics for each available platform
    if 'twitter' in influencer.get('handles', {}):
        try:
            twitter_fetcher = TwitterFetcher()
            twitter_fetcher.fetch_user({
                'id': influencer['id'],
                'handle': influencer['handles']['twitter']
            })
            logger.info(f"Successfully processed Twitter metrics for {influencer['handles']['twitter']}")
        except Exception as e:
            logger.error(f"Error fetching Twitter metrics: {str(e)}")

    if 'instagram' in influencer.get('handles', {}):
        try:
            instagram_fetcher = InstagramFetcher()
            instagram_fetcher.fetch_user({
                'id': influencer['id'],
                'handle': influencer['handles']['instagram']
            })
            logger.info(f"Successfully processed Instagram metrics for {influencer['handles']['instagram']}")
        except Exception as e:
            logger.error(f"Error fetching Instagram metrics: {str(e)}")

    # Add similar blocks for other platforms when implemented
    # if 'youtube' in influencer.get('handles', {}):
    #     youtube_fetcher = YoutubeFetcher()
    #     ...