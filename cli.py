import argparse
import logging
from datetime import datetime
import os
from database import DatabaseManager
from config import Config
import uuid
import pandas as pd
from utils import save_to_csv

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
        db.add_influencer(influencer_data)
        logger.info(f"Successfully added influencer: {name}")
    except ValueError as e:
        logger.error(str(e))
    except Exception as e:
        logger.error(f"Error adding influencer: {str(e)}")

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

def main():
    parser = argparse.ArgumentParser(description='Social Media Influencer Data CLI')
    parser.add_argument('--add_user', action='store_true', help='Add a new influencer')
    parser.add_argument('--edit_user', action='store_true', help='Edit existing influencer')
    parser.add_argument('--save_csv', action='store_true', help='Save API responses to CSV')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    try:
        Config.validate()
        
        if args.add_user:
            add_influencer()
        elif args.edit_user:
            edit_influencer()
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Critical error in CLI execution: {str(e)}")
        raise

if __name__ == "__main__":
    main() 