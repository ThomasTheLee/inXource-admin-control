# %%
from supabase import create_client, Client
from dotenv import load_dotenv
import os

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
        self.supabase_url: str = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key: str = os.getenv('SERVICE_ROLE_KEY')

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
        

# Test
test = Businesses()
print(test.get_business_deatils('510133cb-0531-419a-9735-6733a280f591'))


"""

{'business_name': 'Anime Merch',
'indsutry': 'Fashion & Apparel',
'wallet_balance': 0.0,
'phone': '',
'is_active': True,
'is_deleted': False,
'user_id': '16e695d3-1f99-4820-8cd6-42ad655de308',
'user_name': 'Joshua',
'user_email': 'jsibanda407@gmail.com'}

"""