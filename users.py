from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

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
class Users:
    """Manages the users data in the inXource platform"""

    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key = os.getenv('SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")

        self.client: Client = create_client(self.supabase_url, self.supabase_service_role_key) 

    def total_users(self):
        """Returns the total number of users on the platform"""
        try:
            response = self.client.table("users").select("*").execute()
            if response.data:
                return len(response.data or []) 
            return 0
        except Exception as e:
            print(f"Error fetching users: {e}")
            return 0
        
    

    def total_user_growth_rate(self):
        """Returns the total user growth rate comparing current total vs 30 days ago"""
        try:
            # Get current total
            current_total = len(self.client.table("users").select("*").execute().data)
            
            # Get total from 30 days ago (users who existed then)
            one_month_ago = datetime.now() - timedelta(days=30)
            
            users_30_days_ago = 0
            response = self.client.table("users").select("*").execute()
            
            for user in response.data:
                created_at = user.get('created_at')
                if created_at:
                    try:
                        created_dt = datetime.fromisoformat(created_at)
                        if created_dt <= one_month_ago:  
                            users_30_days_ago += 1
                    except ValueError:
                        continue
            
            if users_30_days_ago == 0:
                return 0.0
                
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
            
            response = self.client.table("users").select("*").execute()
            
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
            
            response = self.client.table("users").select("*").execute()
            
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
            seven_days_ago = datetime.now() - timedelta(days=7)

            # Step 1: Get withdrawals from the last 7 days and collect unique business IDs
            response = (
                self.client.table("withdrawals")
                .select("business_id")
                .gte("requested_at", seven_days_ago.isoformat())
                .execute()
            )

            if not response.data:
                return 0

            business_ids = {w['business_id'] for w in response.data if w.get('business_id')}
            if not business_ids:
                return 0

            # Step 2: Get user IDs owning these businesses
            user_response = (
                self.client.table("business_owners")
                .select("user_id")
                .in_("business_id", list(business_ids))
                .execute()
            )

            if not user_response.data:
                return 0

            active_user_ids = {b['user_id'] for b in user_response.data if b.get('user_id')}
            return len(active_user_ids)

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
                self.client.table("withdrawals")
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
                self.client.table("business_owners")
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
                self.client.table("withdrawals")
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
                self.client.table("business_owners")
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




# Test 
test = Users()
print(type(test.new_registrations_rate()))
