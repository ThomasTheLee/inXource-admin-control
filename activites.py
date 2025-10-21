from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from analysis import AnalAI
import numpy as np
from typing import Dict
from settings import SettingsManager
settings_manager = SettingsManager()

from clients import Clients
# Removed ProductClassifier - using AI only now


load_dotenv()  # loads the .env file
ai_key = os.getenv('OPEN_AI_TEST_KEY')



def singleton(cls):
    """Decorator to ensure only one instance of a class is created"""
    instances = {}
    
    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return wrapper

@singleton
class Activites(Clients):
    """Manages the products data in the inXource platform"""

    def __init__(self):
        super().__init__()

    

    def get_recent_activities(
    self,
    period=settings_manager.summariy_activity_days,
    tables=settings_manager.summary_activity_tables,
    ):
        """Fetches recent activities according to the specified period."""

        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=period)

        recent_activities = []

        for table in tables:
            try:
                # Choose correct date field
                date_field = 'requested_at' if table == 'withdrawals' else 'created_at'

                # Fetch data for this period
                response = (
                    self.supabase_client.table(table)
                    .select('*')
                    .lte(date_field, end_date)
                    .gte(date_field, start_date)
                    .execute()
                )

                # Safely skip empty or invalid responses
                if not response or not getattr(response, 'data', None):
                    continue

                for record in response.data:
                    # Filter out admin user activities
                    if table == 'users':
                        # Skip if this is the admin user registration
                        if record.get('id') == self.admin_user_id:
                            continue
                    elif table == 'businesses':
                        # Skip if this business belongs to admin
                        if record.get('id') in self.admin_business_ids:
                            continue
                    elif table == 'withdrawals':
                        # Skip if withdrawal is from an admin business
                        if record.get('business_id') in self.admin_business_ids:
                            continue
                    
                    raw_date = record.get(date_field)
                    if not raw_date:
                        continue

                    # Parse date safely
                    try:
                        parsed_date = datetime.strptime(raw_date.split(' ')[0], "%Y-%m-%d")
                        date_str = parsed_date.strftime("%d %B %Y")  # e.g. 07 October 2025
                    except Exception:
                        date_str = raw_date.split(' ')[0]  # fallback

                    # Build summary text
                    if table == 'users':
                        summary = f"New user registration on {date_str}."
                    elif table == 'businesses':
                        summary = f"New business registration on {date_str}."
                    elif table == 'withdrawals':
                        amount = record.get('amount')
                        if amount is not None:
                            summary = f"Withdrawal request on {date_str} for K{amount}."
                        else:
                            summary = f"Withdrawal request on {date_str}."
                    else:
                        summary = f"Activity in '{table}' on {date_str}."

                    recent_activities.append(summary)

            except Exception as e:
                print(f"Exception while processing table '{table}': {e}")
                continue

        return recent_activities


# test = Activites()
# print(test.get_recent_activities())