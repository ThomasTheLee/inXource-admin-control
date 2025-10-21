from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from collections import defaultdict
from openai import OpenAI

from settings import SettingsManager
settings_manager = SettingsManager()

load_dotenv()  # loads the .env file
api_key = os.getenv('OPEN_AI_TEST_KEY')

class Clients:
    def __init__(self) -> None:
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key = os.getenv('SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")

        self.supabase_client: Client = create_client(self.supabase_url, self.supabase_service_role_key) 

        self.open_ai_client = OpenAI(api_key=api_key)
        
        # Load admin user and their business IDs
        self.admin_user_id = os.getenv('ADMIN_USER')
        self.admin_business_ids = []
        self._load_admin_business_ids()

    def _load_admin_business_ids(self):
        """Load and cache all business IDs owned by the admin user"""
        if not self.admin_user_id:
            print("[WARNING] ADMIN_USER not set in environment variables")
            return
        
        try:
            owner_response = (
                self.supabase_client.table('business_owners')
                .select('business_id')
                .eq('user_id', self.admin_user_id)
                .execute()
            )
            self.admin_business_ids = [o['business_id'] for o in (owner_response.data or [])]
            print(f"[INFO] Loaded {len(self.admin_business_ids)} admin business IDs to exclude from metrics")
        except Exception as e:
            print(f"[ERROR] Failed to load admin business IDs: {e}")
            self.admin_business_ids = []