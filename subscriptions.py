from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
from clients import Clients


from businesses import Businesses
from settings import SettingsManager




load_dotenv()  # loads the .env file

business_manager = Businesses()
settings_manager = SettingsManager()

def singleton(cls):
    """Decorator to ensure only one instance of a class is created"""
    instances = {}
    
    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return wrapper

@singleton
class Subscriptions(Clients):
    """Manages the subscription data in the inXource platform"""

    def __init__(self):
        super().__init__()

    

    def total_revenue(self):
        """Returns total revenue for all time, this year, and this month (excluding admin user)."""
        # Fetch subscription history
        response = (
            self.supabase_client
            .table('sunhistory')
            .select("amount, created_at, userid")
            .execute()
        )

        if not response.data:
            return {"all_time": 0, "this_year": 0, "this_month": 0}

        # Exclude the admin user's data
        filtered_data = [
            record for record in response.data
            if record.get("userid") != self.admin_user_id
        ]

        total_all_time = 0
        total_this_year = 0
        total_this_month = 0

        now = datetime.now()
        current_year = now.year
        current_month = now.month

        for record in filtered_data:
            price = record.get("amount", 0)
            created_at = record.get("created_at")

            if not created_at:
                continue

            try:
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except Exception:
                continue

            total_all_time += price
            if created_dt.year == current_year:
                total_this_year += price
                if created_dt.month == current_month:
                    total_this_month += price

        return {
            "all_time": round(total_all_time, 2),
            "this_year": round(total_this_year, 2),
            "this_month": round(total_this_month, 2)
        }



    def revenue_period_data(self):
        """Returns four pandas DataFrames for revenue in the past 7 days, month, quarter, and year (excluding admin user)."""

        # Fetch all data from sunhistory
        response = (
            self.supabase_client
            .table("sunhistory")
            .select("amount, created_at, userid")
            .execute()
        )

        if not response.data:
            empty_df = pd.DataFrame(columns=["amount", "created_at", "userid"])
            return {
                "past_7_days": empty_df,
                "past_month": empty_df,
                "past_quarter": empty_df,
                "past_year": empty_df,
            }

        # Filter out admin user's subscriptions
        filtered_data = [
            record for record in response.data
            if record.get("userid") != self.admin_user_id
        ]

        # Convert to DataFrame
        df = pd.DataFrame(filtered_data)

        # Ensure 'created_at' exists as a Series, even if all values are missing
        if 'created_at' not in df.columns:
            df = df.assign(created_at=pd.Series([pd.NaT] * len(df)))

        # Convert created_at to datetime; nulls become NaT
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['created_at'] = df['created_at'].dt.tz_localize(None, ambiguous='NaT', nonexistent='NaT')

        # Define time thresholds
        now = datetime.now()
        seven_days_ago = now - timedelta(days=7)
        one_month_ago = now - timedelta(days=30)
        one_quarter_ago = now - timedelta(days=90)
        one_year_ago = now - timedelta(days=365)

        # Filter DataFrames by period, ignoring rows with NaT
        df_7_days = df[df['created_at'].notna() & (df['created_at'] >= seven_days_ago)]
        df_month = df[df['created_at'].notna() & (df['created_at'] >= one_month_ago)]
        df_quarter = df[df['created_at'].notna() & (df['created_at'] >= one_quarter_ago)]
        df_year = df[df['created_at'].notna() & (df['created_at'] >= one_year_ago)]

        # Return the results
        return {
            "past_7_days": df_7_days.reset_index(drop=True),
            "past_month": df_month.reset_index(drop=True),
            "past_quarter": df_quarter.reset_index(drop=True),
            "past_year": df_year.reset_index(drop=True),
        }
