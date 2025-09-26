# %%
from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd

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
class SettingsManager:
    """Manages the businesses data in the inXource platform"""
    def __init__(self):
        self.business_activity_days = 3  # Default to last 3 days
        self.open_ai_modal = 'gpt-5-nano'
        self.low_stock_count = 3
        self.product_performance_by = 'volume'
        