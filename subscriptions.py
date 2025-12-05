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
        """
        Returns total revenue for all time, this year, and this month from:
          - sunhistory.amount
          - invoices.amount_due WHERE status='paid'
        Excludes admin user by checking:
          - sunhistory.userid != admin
          - invoices.business_id → business_owners.user_id != admin
        """

        now = datetime.now()
        current_year = now.year
        current_month = now.month

        total_all_time = 0
        total_this_year = 0
        total_this_month = 0

        # ---------- 1. Fetch sunhistory data ---------- #
        sun_resp = (
            self.supabase_client
            .table('sunhistory')
            .select("amount, created_at, userid")
            .execute()
        )

        sun_data = [
            r for r in (sun_resp.data or [])
            if r.get("userid") != self.admin_user_id
        ]

        # ---------- 2. Fetch invoices WHERE status='paid' ---------- #
        invoice_resp = (
            self.supabase_client
            .table('invoices')
            .select("amount_due, created_at, business_id, status")
            .eq("status", "paid")
            .execute()
        )

        invoices_raw = invoice_resp.data or []

        # ---------- 3. Fetch business → owner mapping ---------- #
        business_ids = list({inv["business_id"] for inv in invoices_raw if inv.get("business_id")})

        owners_resp = (
            self.supabase_client
            .table("business_owners")
            .select("business_id, user_id")
            .in_("business_id", business_ids)
            .execute()
        )

        owners_map = {}  # business_id → user_id
        for owner in (owners_resp.data or []):
            owners_map[owner["business_id"]] = owner["user_id"]

        # ---------- 4. Filter invoices by owner != admin ---------- #
        invoice_data = []
        for inv in invoices_raw:
            bid = inv.get("business_id")
            owner_id = owners_map.get(bid)

            if owner_id and owner_id != self.admin_user_id:
                invoice_data.append(inv)

        # ---------- 5. Combine records ---------- #
        all_records = []

        # Add sunhistory rows directly
        for s in sun_data:
            all_records.append({
                "amount": s.get("amount", 0),
                "created_at": s.get("created_at")
            })

        # Add invoice rows using amount_due
        for inv in invoice_data:
            all_records.append({
                "amount": inv.get("amount_due", 0),
                "created_at": inv.get("created_at")
            })

        # ---------- 6. Calculate totals ---------- #
        for rec in all_records:
            amount = rec.get("amount", 0)
            created_at = rec.get("created_at")

            if not created_at:
                continue

            try:
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            except Exception:
                continue

            total_all_time += amount

            if created_dt.year == current_year:
                total_this_year += amount

                if created_dt.month == current_month:
                    total_this_month += amount

        # ---------- 7. Return totals ---------- #
        return {
            "all_time": round(total_all_time, 2),
            "this_year": round(total_this_year, 2),
            "this_month": round(total_this_month, 2)
        }

    def revenue_period_data(self):
        """
        Returns four pandas DataFrames for combined revenue from:
          - sunhistory.amount
          - invoices.amount_due WHERE invoices.status = 'paid'
        Excludes admin user by checking:
          - sunhistory.userid != admin
          - invoices.business_id → business_owners.user_id != admin
        """

        # ---------- 1. Fetch sunhistory data ---------- #
        sun_resp = (
            self.supabase_client
            .table("sunhistory")
            .select("amount, created_at, userid")
            .execute()
        )

        # Filter admin
        sun_data = [
            r for r in (sun_resp.data or [])
            if r.get("userid") != self.admin_user_id
        ]

        # ---------- 2. Fetch invoices WHERE status='paid' ---------- #
        invoice_resp = (
            self.supabase_client
            .table("invoices")
            .select("amount_due, created_at, business_id, status")
            .eq("status", "paid")
            .execute()
        )

        invoices_raw = invoice_resp.data or []

        # ---------- 3. Fetch business → owner mapping ---------- #
        # Get all business ids from invoices
        business_ids = list({inv["business_id"] for inv in invoices_raw if inv.get("business_id")})

        owners_resp = (
            self.supabase_client
            .table("business_owners")
            .select("business_id, user_id")
            .in_("business_id", business_ids)
            .execute()
        )

        owners_map = {}  # business_id → user_id
        for o in owners_resp.data or []:
            owners_map[o["business_id"]] = o["user_id"]

        # ---------- 4. Filter invoices by owner != admin ---------- #
        invoice_data = []
        for inv in invoices_raw:
            bid = inv.get("business_id")
            owner = owners_map.get(bid)

            if owner and owner != self.admin_user_id:
                invoice_data.append({
                    "amount": inv.get("amount_due", 0),
                    "created_at": inv.get("created_at"),
                    "userid": owner
                })

        # ---------- 5. Combine sunhistory + invoices ---------- #
        combined = sun_data + invoice_data

        if not combined:
            empty_df = pd.DataFrame(columns=["amount", "created_at", "userid"])
            return {
                "past_7_days": empty_df,
                "past_month": empty_df,
                "past_quarter": empty_df,
                "past_year": empty_df
            }

        df = pd.DataFrame(combined)

        # ---------- Ensure created_at exists ---------- #
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
        df["created_at"] = df["created_at"].dt.tz_localize(None)

        # ---------- Time periods ---------- #
        now = datetime.now()
        seven_days_ago = now - timedelta(days=7)
        one_month_ago = now - timedelta(days=30)
        one_quarter_ago = now - timedelta(days=90)
        one_year_ago = now - timedelta(days=365)

        # ---------- Filters ---------- #
        df_7_days = df[df["created_at"].notna() & (df["created_at"] >= seven_days_ago)]
        df_month = df[df["created_at"].notna() & (df["created_at"] >= one_month_ago)]
        df_quarter = df[df["created_at"].notna() & (df["created_at"] >= one_quarter_ago)]
        df_year = df[df["created_at"].notna() & (df["created_at"] >= one_year_ago)]

        return {
            "past_7_days": df_7_days.reset_index(drop=True),
            "past_month": df_month.reset_index(drop=True),
            "past_quarter": df_quarter.reset_index(drop=True),
            "past_year": df_year.reset_index(drop=True),
        }

