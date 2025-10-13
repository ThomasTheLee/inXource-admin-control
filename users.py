from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from clients import Clients

load_dotenv()  # loads the .env file

def singleton(cls):
    """Decorator to ensure only one instance of a class is created"""
    instances = {}
    
    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return wrapper

@singleton
class Users(Clients):
    """Manages the users data in the inXource platform"""

    def __init__(self):
        super().__init__()

    def total_users(self):
        """Returns the total number of users on the platform"""
        try:
            response = self.supabase_client.table("users").select("*").execute()
            if response.data:
                return len(response.data or []) 
            return 0
        except Exception as e:
            print(f"Error fetching users: {e}")
            return 0
        
    

    def total_user_growth_rate(self):
        """Returns the total user growth rate comparing current total vs 30 days ago."""
        try:
            # Fetch all users once
            response = self.supabase_client.table("users").select("*").execute()
            users = response.data or []

            # Current total
            current_total = len(users)

            # Cutoff date
            one_month_ago = datetime.now() - timedelta(days=30)

            # Count users that existed 30 days ago
            users_30_days_ago = 0
            for user in users:
                created_at = user.get("created_at")
                if created_at:
                    try:
                        created_dt = datetime.fromisoformat(created_at)
                        if created_dt <= one_month_ago:
                            users_30_days_ago += 1
                    except ValueError:
                        continue

            # Avoid division by zero
            if users_30_days_ago == 0:
                return 0.0

            # Growth rate formula
            growth_rate = ((current_total - users_30_days_ago) / users_30_days_ago) * 100
            return round(growth_rate, 2)

        except Exception as e:
            print(f"Error calculating user growth rate: {e}")
            return 0.0


    def total_new_registrations(self):
        """Returns the total number of new registrations in the last 30 days"""
        try:
            now = datetime.now()
            thirty_days_ago = now - timedelta(days=30)
            
            response = self.supabase_client.table("users").select("*").execute()
            
            recent_users = 0  # Last 30 days
            
            for user in response.data:
                created_at = user.get('created_at')
                if created_at:
                    try:
                        created_dt = datetime.fromisoformat(created_at)
                        if created_dt >= thirty_days_ago:
                            recent_users += 1
                    except ValueError:
                        continue
            
            return recent_users
            
        except Exception as e:
            print(f"Error calculating new registrations: {e}")
            return 0


        
    def new_registrations_rate(self):
        """Returns new registrations in last 30 days vs previous 30 days"""
        try:
            now = datetime.now()
            thirty_days_ago = now - timedelta(days=30)
            sixty_days_ago = now - timedelta(days=60)
            
            response = self.supabase_client.table("users").select("*").execute()
            
            recent_users = 0  # Last 30 days
            previous_users = 0  # 30-60 days ago
            
            for user in response.data:
                created_at = user.get('created_at')
                if created_at:
                    try:
                        created_dt = datetime.fromisoformat(created_at)
                        if created_dt >= thirty_days_ago:
                            recent_users += 1
                        elif created_dt >= sixty_days_ago:
                            previous_users += 1
                    except ValueError:
                        continue
            
            if previous_users == 0:
                return 0.0
                
            growth_rate = ((recent_users - previous_users) / previous_users) * 100
            return round(growth_rate, 2)
            
        except Exception as e:
            print(f"Error calculating new registrations rate: {e}")
            return 0.0



    def total_active_users(self):
        """Counts users who own businesses with withdrawals in the last 7 days"""
        try:
            response = (
                self.supabase_client.table("users")
                .select("id")
                .eq("hasSubscription", True)
                .execute()
            )

            if not response.data:
                return 0
            user_ids = [user['id'] for user in response.data if user.get('id')]
            return len(user_ids)

        except Exception as e:
            print(f"Error calculating active users: {e}")
            return 0
        
    def active_users_growh_rate(self):
        """Calculates growth rate of active users compared to previous 7-day period"""
        try:
            now = datetime.now()
            seven_days_ago = now - timedelta(days=7)
            fourteen_days_ago = now - timedelta(days=14)

            # Step 1: Get current active users (last 7 days)
            current_response = (
                self.supabase_client.table("withdrawals")
                .select("business_id")
                .gte("requested_at", seven_days_ago.isoformat())
                .execute()
            )

            if not current_response.data:
                return 0.0

            current_business_ids = {w['business_id'] for w in current_response.data if w.get('business_id')}
            if not current_business_ids:
                return 0.0

            current_user_response = (
                self.supabase_client.table("business_owners")
                .select("user_id")
                .in_("business_id", list(current_business_ids))
                .execute()
            )

            if not current_user_response.data:
                return 0.0

            current_active_user_ids = {b['user_id'] for b in current_user_response.data if b.get('user_id')}
            current_active_count = len(current_active_user_ids)

            # Step 2: Get previous active users (7-14 days ago)
            previous_response = (
                self.supabase_client.table("withdrawals")
                .select("business_id")
                .gte("requested_at", fourteen_days_ago.isoformat())
                .lt("requested_at", seven_days_ago.isoformat())
                .execute()
            )

            if not previous_response.data:
                return 0.0

            previous_business_ids = {w['business_id'] for w in previous_response.data if w.get('business_id')}
            if not previous_business_ids:
                return 0.0

            previous_user_response = (
                self.supabase_client.table("business_owners")
                .select("user_id")
                .in_("business_id", list(previous_business_ids))
                .execute()
            )

            if not previous_user_response.data:
                return 0.0

            previous_active_user_ids = {b['user_id'] for b in previous_user_response.data if b.get('user_id')}
            previous_active_count = len(previous_active_user_ids)

            if previous_active_count == 0:
                return 0.0

            growth_rate = ((current_active_count - previous_active_count) / previous_active_count) * 100
            return round(growth_rate, 2)
        except Exception as e:
            print(f"Error calculating active users growth rate: {e}")
            return 0.0
        


    def retrieve_users_information(self, query):
        """Most compatible version"""
        try:
            all_results = []
            seen_ids = set()
            
            # Try exact UUID match first
            if len(query) == 36 and query.count('-') == 4:  # Basic UUID format check
                try:
                    response = self.supabase_client.table("users").select("*").eq("id", query).execute()
                    if response.data:
                        return response.data
                except:
                    pass
            
            # Search text fields
            for column in ["name", "email", "phone", "location", "role"]:
                try:
                    response = (
                        self.supabase_client.table("users")
                        .select("*")
                        .ilike(column, f"%{query}%")
                        .execute()
                    )
                    
                    if response.data:
                        for user in response.data:
                            if user.get('id') not in seen_ids:
                                all_results.append(user)
                                seen_ids.add(user.get('id'))
                                # add the businesses info
                                user['businesses'] = self.users_businesses(user.get('id'))
                except:
                    continue
            
            return all_results
        

        except Exception as e:
            print(f"Error retrieving users information: {e}")
            return []
        
    def users_businesses(self, user_id):
        """retirves information about the users businesses as a dictioanry"""
        try:
            # Step 1: Get business IDs owned by the user
            response = (
                self.supabase_client.table("business_owners")
                .select("business_id")
                .eq("user_id", user_id)
                .execute()
            )

            if not response.data:
                return []

            business_ids = [b['business_id'] for b in response.data if b.get('business_id')]
            if not business_ids:
                return []

            # Step 2: Get business details
            business_response = (
                self.supabase_client.table("businesses")
                .select("*")
                .in_("id", business_ids)
                .execute()
            )

            if business_response.data:
                return business_response.data
            return []
        
        except Exception as e:
            print(f"Error retrieving user's businesses: {e}")
            return []
        
    def users_per_location(self):
        """Returns a breakdown of users by location, with count and width % for chart bars"""
        try:
            response = self.supabase_client.table("users").select("location").execute()
            if not response.data:
                return []

            # Count users per location
            location_counts = {}
            for user in response.data:
                location = user.get('location', 'Unknown')
                location_counts[location] = location_counts.get(location, 0) + 1

            # Find the max count to normalize bar widths
            max_count = max(location_counts.values()) if location_counts else 1

            # Build a clean list with precomputed widths
            regions = [
                {
                    "name": location,
                    "count": count,
                    "width": int((count / max_count) * 100)  # always an integer like 25, 50, 100
                }
                for location, count in location_counts.items()
            ]

            return regions

        except Exception as e:
            print(f"Error calculating users per location: {e}")
            return []


    def monthly_user_trend(self):
        """
        Returns a DataFrame showing the number of users registered in each
        of the last 12 months. Columns: 'month', 'user_count'.
        """
        try:
            # get all users
            response = self.supabase_client.table("users").select("*").execute()
            all_users = response.data

            #  Convert to DataFrame
            df = pd.DataFrame(all_users)

            if df.empty or 'created_at' not in df.columns:
                # Return empty DataFrame if no data
                return pd.DataFrame(columns=['month', 'user_count'])

            #  Ensure created_at is datetime
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df = df.dropna(subset=['created_at'])

            #  Filter last 12 months
            one_year_ago = pd.Timestamp.now() - pd.DateOffset(months=12)
            df_last_12 = df[df['created_at'] >= one_year_ago]

            #  Extract month and group
            df_last_12['month'] = df_last_12['created_at'].dt.to_period('M')
            monthly_counts = df_last_12.groupby('month').size().reset_index(name='user_count')

            #  Sort by month
            monthly_counts = monthly_counts.sort_values('month').reset_index(drop=True)

            return monthly_counts

        except Exception as e:
            print("Error generating monthly user trend:", e)
            return pd.DataFrame(columns=['month', 'user_count'])


    def monthly_activity_trend(self):
        """
        Returns a DataFrame showing the number of active users (with withdrawals)
        in each of the last 12 months. Columns: 'month', 'active_user_count'.
        """
        try:
            # Get all withdrawals
            response = self.supabase_client.table("withdrawals").select("*").execute()
            all_withdrawals = response.data

            if not all_withdrawals:
                return pd.DataFrame(columns=['month', 'active_user_count'])

            # Convert to DataFrame
            df = pd.DataFrame(all_withdrawals)

            if df.empty or 'requested_at' not in df.columns:
                return pd.DataFrame(columns=['month', 'active_user_count'])

            # Ensure requested_at is datetime
            df['requested_at'] = pd.to_datetime(df['requested_at'], errors='coerce')
            df = df.dropna(subset=['requested_at'])

            # Filter last 12 months
            one_year_ago = pd.Timestamp.now() - pd.DateOffset(months=12)
            df_last_12 = df[df['requested_at'] >= one_year_ago]

            if df_last_12.empty:
                return pd.DataFrame(columns=['month', 'active_user_count'])

            # Extract month and group by unique user IDs per month
            df_last_12['month'] = df_last_12['requested_at'].dt.to_period('M')
            monthly_active = (
                df_last_12.groupby('month')['business_id']
                .nunique()
                .reset_index(name='active_user_count')
            )

            # Sort by month
            monthly_active = monthly_active.sort_values('month').reset_index(drop=True)

            return monthly_active

        except Exception as e:
            print("Error generating monthly activity trend:", e)
            return pd.DataFrame(columns=['month', 'active_user_count'])

    
    
