# %%
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
