# %%
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
class Businesses:
    """Manages the businesses data in the inXource platform"""

    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key = os.getenv('SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")

        self.client: Client = create_client(self.supabase_url, self.supabase_service_role_key) 

    def get_business_deatils(self, business_id):
        """Returns the business details using the business ID"""
        business_details = {}

        try:
            # Fetch business details
            business_response = (
                self.client.table('businesses')
                .select('*')
                .eq('id', business_id)
                .execute()
            )

            if not business_response.data:
                return {}

            business = business_response.data[0]  # first matching business
            business_details['business_id'] = business.get('id')
            business_details['business_name'] = business.get('business_name')
            business_details['indsutry'] = business.get('industry')
            business_details['wallet_balance'] = business.get('wallet_balance')
            business_details['phone'] = business.get('phone')
            business_details['is_active'] = business.get('is_active')
            business_details['is_deleted'] = business.get('is_deleted')

            # Fetch business owner
            owner_response = (
                self.client.table('business_owners')
                .select('user_id')
                .eq('business_id', business_id)
                .execute()
            )

            if not owner_response.data:
                print("No owner found for this business.")
                return business_details  # no owner found

            owner = owner_response.data[0]

            # Fetch user details
            user_response = (
                self.client.table('users')
                .select('*')
                .eq('id', owner['user_id'])
                .execute()
            )

            if user_response.data:
                user = user_response.data[0]
                business_details['user_id'] = user.get('id')
                business_details['user_name'] = user.get('name')
                business_details['user_email'] = user.get('email')

            return business_details

        except Exception as e:
            print(f"Error fetching business details: {e}")
            return {}
   

    def total_businesses(self):
        """Returns the total number of users on the platform"""
        try:
            response = self.client.table("businesses").select("*").execute()
            if response.data:
                return len(response.data or []) 
            return 0
        except Exception as e:
            print(f"Error fetching users: {e}")
            return 0
        

    def total_businesses_growth_rate(self):
        """Returns the growth rate of businesses on the platform."""
        try:
            # Fetch all businesses once
            response = self.client.table("businesses").select("*").execute()
            businesses = response.data or []

            # Current total
            current_total = len(businesses)

            # Cutoff date
            one_month_ago = datetime.now() - timedelta(days=30)

            # Count businesses that existed 30 days ago
            businesses_30_days_ago = 0
            for biz in businesses:
                created_at = biz.get("created_at")
                if created_at:
                    try:
                        created_dt = datetime.fromisoformat(created_at)
                        if created_dt <= one_month_ago:
                            businesses_30_days_ago += 1
                    except ValueError:
                        continue

            # Avoid division by zero
            if businesses_30_days_ago == 0:
                return 0.0

            # Growth rate formula
            growth_rate = ((current_total - businesses_30_days_ago) / businesses_30_days_ago) * 100
            return round(growth_rate, 2)

        except Exception as e:
            print(f"Error calculating business growth rate: {e}")
            return 0.0
        
    def new_businesses_registrations(self, days=30):
        """Returns the number of new businesses registered in the last 'days' days."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            response = self.client.table("businesses").select("*").execute()
            if response.data:
                new_businesses = [
                    biz for biz in response.data
                    if biz.get("created_at") and datetime.fromisoformat(biz["created_at"]) >= cutoff_date
                ]
                return len(new_businesses)
            return 0
        except Exception as e:
            print(f"Error fetching new businesses: {e}")
            return 0


    def new_businesses_registrations_rate(self, days=30):
        """Returns the growth rate of new business registrations over the last 'days' days."""
        try:
            # Current period
            current_period_count = self.new_businesses_registrations(days)

            # Previous period
            previous_period_count = self.new_businesses_registrations(days * 2) - current_period_count

            # Avoid division by zero
            if previous_period_count == 0:
                return 0.0

            # Growth rate formula
            growth_rate = ((current_period_count - previous_period_count) / previous_period_count) * 100
            return round(growth_rate, 2)

        except Exception as e:
            print(f"Error calculating new business registrations growth rate: {e}")
            return 0.0
        
    def total_active_businesses(self, days=30):
        """returns the total active businesses based on the number on the withdraws i the last 30 days"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            response = self.client.table("withdrawals").select("business_id, requested_at").execute()
            if response.data:
                active_business_ids = {
                    wd["business_id"] for wd in response.data
                    if wd.get("requested_at") and datetime.fromisoformat(wd["requested_at"]) >= cutoff_date
                }
                return len(active_business_ids)
            return 0
        except Exception as e:
            print(f"Error fetching active businesses: {e}")
            return 0
        
    
    def total_active_businesses_growth_rate(self, days=30):
        """Returns the growth rate of active businesses over the last 'days' days."""
        try:
            # Current period
            current_period_count = self.total_active_businesses(days)

            # Previous period
            previous_period_count = self.total_active_businesses(days * 2) - current_period_count

            # Avoid division by zero
            if previous_period_count == 0:
                return 0.0

            # Growth rate formula
            growth_rate = ((current_period_count - previous_period_count) / previous_period_count) * 100
            return round(growth_rate, 2)

        except Exception as e:
            print(f"Error calculating active business growth rate: {e}")
            return 0.0

# test
test = Businesses()
print(test.total_active_businesses_growth_rate())