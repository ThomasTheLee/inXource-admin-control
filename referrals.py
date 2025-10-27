from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from analysis import AnalAI
import numpy as np
from typing import Dict
from users import Users

from settings import SettingsManager
settings_manager = SettingsManager()

from clients import Clients
# Removed ProductClassifier - using AI only now


load_dotenv()  # loads the .env file
ai_key = os.getenv('OPEN_AI_TEST_KEY')

# module instances
user_manager = Users()



def singleton(cls):
    """Decorator to ensure only one instance of a class is created"""
    instances = {}
    
    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return wrapper

@singleton
class Referrals(Clients):
    """Manages the products data in the inXource platform"""

    def __init__(self):
        super().__init__()
    
    def search_user(self, query):
        """returns a user details ased on the search query of email or user name" 
        using the retrieve_users_information method from Users class"""
        return user_manager.retrieve_users_information(query)
    
    def load_active_referrals(self):
        """Loads all active referrals from the database"""
        try:
            # Load users with an active referral code using the correct FK
            # Use is_() with 'null' string for Supabase NULL checks
            users_response = (
                self.supabase_client
                .table("users")
                .select("*, referral_codes!fk_referral_code(*)")
                .not_.is_("ref_code", "null") 
                .execute()
            )

            users = users_response.data or []
            print(f"Loaded {len(users)} users with active referrals.")
            if not users:
                return []

            # Filter out invalid ref_code values
            ref_code_ids = [
                user.get("ref_code")
                for user in users
                if user.get("ref_code") is not None  # Added filter
            ]

            # Fetch all sunhistory records for these ref_codes in one query
            if ref_code_ids:
                sunhistory_response = (
                    self.supabase_client
                    .table("sunhistory")
                    .select("ref_code, amount")
                    .in_("ref_code", ref_code_ids)
                    .execute()
                )
                sunhistory_data = sunhistory_response.data or []
            else:
                sunhistory_data = []

            # Group sunhistory by ref_code
            earnings_by_ref = {}
            for record in sunhistory_data:
                ref_code = record.get("ref_code")
                amount = record.get("amount", 0)
                if ref_code:
                    earnings_by_ref.setdefault(ref_code, []).append(amount)
            print(f"Earnings grouped by referral code: {earnings_by_ref}")

            # Calculate total earned for each user
            for user in users:
                ref_code_id = user.get("ref_code")
                referral_data = user.get("referral_codes") or []

                # Get percentage_cut safely
                if isinstance(referral_data, list) and referral_data:
                    percentage_cut = referral_data[0].get("percentage_cut", 0)
                elif isinstance(referral_data, dict):
                    percentage_cut = referral_data.get("percentage_cut", 0)
                else:
                    percentage_cut = 0

                # Calculate total earned
                amounts = earnings_by_ref.get(ref_code_id, [])
                total_earned = sum(amount * (percentage_cut / 100) for amount in amounts)
                user["total_earned"] = round(total_earned, 2)

            return users

        except Exception as e:
            print(f"Error loading active referrals: {e}")
            return None

    def assign_referral(self, user_id, ref_code, percentage):
        """Assigns a referral code to a user"""
        data = {
            "user_id": user_id,
            "percentage_cut": percentage,
            "updated_at" : datetime.utcnow().isoformat(),
            "ref_code" : ref_code
        }

        try:
            # check to make sure that the referral code is unique
            response = self.supabase_client.table("referral_codes").select("*").eq("ref_code", ref_code).execute()

            if response.data and len(response.data) > 0:
                print("Referral code already exists. Please choose a different code.")
                return None
            # insert the referral code
            insert_response = self.supabase_client.table("referral_codes").insert(data).execute()

            # collect the id for that record and update the ref_code in users table
            if insert_response.data and len(insert_response.data) > 0:
                id = insert_response.data[0]["id"]
                user_update = self.supabase_client.table("users").update({"ref_code": id}).eq("id", user_id).execute()
            return insert_response.data
        except Exception as e:
            print(f"Error assigning referral code: {e}")
            return None
        
    def edit_referral(self, user_id, ref_code, percentage):
        """Edits a referral code assigned to a user"""
        data = {
            "user_id": user_id,
            "percentage_cut": percentage,
            "updated_at" : datetime.utcnow().isoformat(),
            "ref_code" : ref_code
        }

        try:
            # update the referral code
            response = self.supabase_client.table("referral_codes").update(data).eq("user_id", user_id).execute()
            return response.data
        except Exception as e:
            print(f"Error editing referral code: {e}")
            return None
        


test = Referrals()
print(test.load_active_referrals())


        

    