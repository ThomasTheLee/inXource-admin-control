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
class Products:
    """Manages the products data in the inXource platform"""

    def __init__(self):
        self.supabase_url: str = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key: str = os.getenv('SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")

        self.client: Client = create_client(self.supabase_url, self.supabase_service_role_key) 

    def total_products(self):
        """Returns the total number of users on the platform"""
        try:
            response = self.client.table("products").select("*").execute()
            if response.data:
                return len(response.data or []) 
            return 0
        except Exception as e:
            print(f"Error fetching users: {e}")
            return 0
