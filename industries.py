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
class Industry(Clients):
    """Manages the users data in the inXource platform"""

    def __init__(self):
        super().__init__()

    def total_industries(self):
        """returns the total list of industries in the database"""
        try:
            response = (
                self.supabase_client.table('businesses')
                .select('industry')
                .execute()
            )

            if not response.data:  # covers None and empty
                return []

            industries = list(set(industry['industry'] for industry in response.data if industry.get('industry')))
            return industries

        except Exception:
            return []  # fallback in case of query error


    def list_industry_totals(self):
        """Returns a dictionary of totals for all industries."""

        # Step 1: Get all completed orders
        orders_response = (
            self.supabase_client
            .table('orders')
            .select('business_id, total_amount')
            .eq('order_payment_status', 'completed')
            .execute()
        )
        orders = orders_response.data or []

        # Step 2: Get all businesses with their industries
        businesses_response = (
            self.supabase_client
            .table('businesses')
            .select('id, industry')
            .execute()
        )
        businesses = {b['id']: b['industry'] for b in businesses_response.data or []}

        # Step 3: Aggregate totals per industry
        industry_totals = {}
        for order in orders:
            industry = businesses.get(order['business_id'], "Unknown")
            industry_totals[industry] = industry_totals.get(industry, 0) + order['total_amount']

        return industry_totals
    
    def get_industries_total(self):
        """Gets the total amount across all industries as a float"""
        industry_totals = self.list_industry_totals()
        totals = sum(industry_totals.values()) if industry_totals else 0.0
        return float(totals)
    
    def total_industry_revenue_rate(self, days=30, industries=None):
        """
        Returns the revenue growth rate(s) compared to the previous period.

        - If industries is None: returns one growth rate for all industries combined.
        - If industries is a list of industry names: returns {industry: growth_rate}.
        """

        try:
            # Step 1: Define date ranges
            end_date = datetime.now(timezone.utc)
            start_current = (end_date - timedelta(days=days)).isoformat()
            start_previous = (end_date - timedelta(days=2 * days)).isoformat()

            def compute_growth(current_total, previous_total):
                """Helper to calculate growth rate."""
                if previous_total == 0:
                    return 0.0 if current_total == 0 else 100.0
                return ((current_total - previous_total) / previous_total) * 100

            # Case 1: No industries passed → calculate overall growth
            if industries is None:
                current_response = (
                    self.supabase_client.table("orders")
                    .select("total_amount, created_at")
                    .eq("order_payment_status", "completed")
                    .gte("created_at", start_current)
                    .execute()
                )
                current_orders = current_response.data or []
                current_total = sum(o["total_amount"] for o in current_orders if o.get("total_amount"))

                previous_response = (
                    self.supabase_client.table("orders")
                    .select("total_amount, created_at")
                    .eq("order_payment_status", "completed")
                    .gte("created_at", start_previous)
                    .lt("created_at", start_current)
                    .execute()
                )
                previous_orders = previous_response.data or []
                previous_total = sum(o["total_amount"] for o in previous_orders if o.get("total_amount"))

                return float(compute_growth(current_total, previous_total))

            # Case 2: Industries list passed → calculate growth for each industry
            results = {}
            for industry in industries:
                # Current period
                current_response = (
                    self.supabase_client.table("orders")
                    .select("total_amount, created_at, businesses(industry)")
                    .eq("order_payment_status", "completed")
                    .eq("businesses.industry", industry)   # filter by joined column
                    .gte("created_at", start_current)
                    .execute()
                )
                current_orders = current_response.data or []
                current_total = sum(o["total_amount"] for o in current_orders if o.get("total_amount"))

                # Previous period
                previous_response = (
                    self.supabase_client.table("orders")
                    .select("total_amount, created_at, businesses(industry)")
                    .eq("order_payment_status", "completed")
                    .eq("businesses.industry", industry)   # filter by joined column
                    .gte("created_at", start_previous)
                    .lt("created_at", start_current)
                    .execute()
                )

                previous_orders = previous_response.data or []
                previous_total = sum(o["total_amount"] for o in previous_orders if o.get("total_amount"))

                results[industry] = float(compute_growth(current_total, previous_total))

            return results

        except Exception as e:
            print(f"Exception: {e}")
            return 0.0 if industries is None else {}



    
    def industry_market_share(self, industry=None):
        """returns the market share percentage of the industry passed in the parameter"""

        if industry is None:
            industry = max(
                business_manager.get_top_performing_industries(),
                key=lambda x: x[1]
            )[0]

        # get the total amount for that industry
        industry_total = self.list_industry_totals().get(industry.lower(), 0)

        # get the total market (sum of all industries)
        market_total = float(sum(self.list_industry_totals().values()))

        # return the percentage share
        return (industry_total / market_total * 100) if market_total > 0 else 0


    def industry_average_growth_rate(self, days=30):
        """Returns the average growth rate across all industries"""
        
        industries = self.total_industries()

        # get the growth rate of each industry
        growth_rates = self.total_industry_revenue_rate(days=days, industries=industries)

        if not growth_rates:  # avoid division by zero
            return 0.0

        average_rate = sum(growth_rates.values()) / len(growth_rates)
        return average_rate
    
    def check_new_industries(self, days=settings_manager.business_activity_days):
        """returns the list of new industries added according in the last days"""

        end_date = datetime.now(timezone.utc)
        start_current = (end_date - timedelta(days=days)).isoformat()

        try:
            response = (
                self.supabase_client.table('industry_trucking')
                .select('*')
                .gte('created_at', start_current)  
                .execute() 
                )
            return response.data

        except Exception as e:
            print(f"Exception: {e}")
            return []
        


    def industry_revenue_trend(self, industry):
        """Returns a DataFrame for that industry required to create a revenue trend chart.
        Columns: month, amount
        """
        industry = industry.lower()
        # Step 1: Get business IDs for the industry
        businesses_response = (
            self.supabase_client.table('businesses')
            .select('id')
            .eq('industry', industry)
            .execute()
        )
        business_ids = [b['id'] for b in (businesses_response.data or [])]

        # Step 2: Collect all orders for these businesses
        all_orders = []
        for biz_id in business_ids:
            orders_response = (
                self.supabase_client.table('orders')
                .select('created_at, total_amount')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .eq('business_id', biz_id)
                .execute()
            )
            orders = orders_response.data or []
            all_orders.extend(orders)

        if not all_orders:
            # Return empty dataframe if no orders
            return pd.DataFrame(columns=['month', 'amount'])

        # Step 3: Convert to DataFrame
        df = pd.DataFrame(all_orders)

        # Step 4: Ensure total_amount is numeric
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)

        # Step 5: Convert created_at to datetime and extract month
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['month'] = df['created_at'].dt.strftime('%Y-%m')  # e.g., '2025-09'

        # Step 6: Group by month and sum total_amount
        monthly_revenue = df.groupby('month')['total_amount'].sum().reset_index()
        monthly_revenue.rename(columns={'total_amount': 'amount'}, inplace=True)

        # Step 7: Sort by month
        monthly_revenue = monthly_revenue.sort_values('month')

        return monthly_revenue


    def customer_growth_trend(self, industry):
        """Returns a DataFrame for that industry required to create a customer trend chart.
        Columns: month, customers
        """

        industry = industry.lower()

        # Step 1: Get business IDs for the industry
        businesses_response = (
            self.supabase_client.table('businesses')
            .select('id')
            .eq('industry', industry)
            .execute()
        )
        business_ids = [b['id'] for b in (businesses_response.data or [])]

        # Step 2: Collect all customers for these businesses
        all_customers = []
        for biz_id in business_ids:
            response = (
                self.supabase_client.table('customers')
                .select('created_at')
                .eq('business_id', biz_id)
                .execute()
            )
            customers = response.data or []
            all_customers.extend(customers)

        if not all_customers:
            # Return empty DataFrame if no customers
            return pd.DataFrame(columns=['month', 'customers'])

        # Step 3: Convert to DataFrame
        df = pd.DataFrame(all_customers)

        # Step 4: Convert created_at to datetime and extract month
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['month'] = df['created_at'].dt.strftime('%Y-%m')  # e.g., '2025-09'

        # Step 5: Group by month and count customers
        monthly_customers = df.groupby('month').size().reset_index(name='customers')

        # Step 6: Sort by month
        monthly_customers = monthly_customers.sort_values('month')

        return monthly_customers



    def industry_average_order_trend(self, industry):
        """
        Returns a DataFrame showing the average order size per month for a given industry.
        Columns: month, average_order_size
        """

        industry = industry.lower()

        # Get business IDs for the industry
        businesses_response = (
            self.supabase_client.table('businesses')
            .select('id')
            .eq('industry', industry)
            .execute()
        )
        business_ids = [b['id'] for b in (businesses_response.data or [])]

        # Collect all orders for these businesses
        all_orders = []
        for biz_id in business_ids:
            orders_response = (
                self.supabase_client.table('orders')
                .select('created_at, total_amount')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .eq('business_id', biz_id)
                .execute()
            )
            orders = orders_response.data or []
            all_orders.extend(orders)

        if not all_orders:
            # Return empty DataFrame if no orders
            return pd.DataFrame(columns=['month', 'average_order_size'])

        # Convert to DataFrame
        df = pd.DataFrame(all_orders)

        # Step 4: Ensure total_amount is numeric
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)

        # Convert created_at to datetime and extract month
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['month'] = df['created_at'].dt.strftime('%Y-%m')  # e.g., '2025-09'

        # Group by month and calculate average order size
        avg_order_trend = df.groupby('month')['total_amount'].mean().reset_index()
        avg_order_trend.rename(columns={'total_amount': 'average_order_size'}, inplace=True)

        # Sort by month
        avg_order_trend = avg_order_trend.sort_values('month')

        return avg_order_trend

    def industry_seasonal_performance_trend(self, industry):
        """
        Returns a DataFrame showing total sales per season for a given industry.
        Columns: season, total_sales
        Overlapping months are counted in all applicable seasons.
        """

        industry = industry.lower()

        # Define seasons with their corresponding months
        seasons = {
            "School Holidays": [4, 8, 12],
            "Rainy Season": [11, 12, 1],
            "Summer": [8, 10],
            "Cold Season": [6, 7],
            "Festive Holidays": [4, 12, 1],
        }

        # Get business IDs for the industry
        businesses_response = (
            self.supabase_client.table('businesses')
            .select('id')
            .eq('industry', industry)
            .execute()
        )
        business_ids = [b['id'] for b in (businesses_response.data or [])]

        # Collect all completed orders for these businesses
        all_orders = []
        for biz_id in business_ids:
            orders_response = (
                self.supabase_client.table('orders')
                .select('created_at, total_amount')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .eq('business_id', biz_id)
                .execute()
            )
            orders = orders_response.data or []
            all_orders.extend(orders)

        if not all_orders:
            # Return empty DataFrame if no orders
            return pd.DataFrame(columns=['season', 'total_sales'])

        # Convert to DataFrame and ensure numeric
        df = pd.DataFrame(all_orders)
        df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce').fillna(0)
        df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
        df['month'] = df['created_at'].dt.month  # Extract month as integer

        # Initialize dictionary for season totals
        season_totals = {season: 0 for season in seasons.keys()}

        # Sum sales per season (allow overlaps)
        for season, months in seasons.items():
            season_totals[season] = df[df['month'].isin(months)]['total_amount'].sum()

        # Convert dictionary to DataFrame
        season_df = pd.DataFrame(list(season_totals.items()), columns=['season', 'total_sales'])

        # Optional: sort by predefined order
        season_order = ["School Holidays", "Rainy Season", "Summer", "Cold Season", "Festive Holidays"]
        season_df['season'] = pd.Categorical(season_df['season'], categories=season_order, ordered=True)
        season_df = season_df.sort_values('season').reset_index(drop=True)

        return season_df


    def industry_customer_retention_rate(self, industry):
        """returns the customer retention rate for that indsutry"""

        # get the busienss ids for that industry
        businesses_response = (
            self.supabase_client.table('businesses')
            .select('id')
            .eq('industry', industry)
            .execute()
        )
        business_ids = [b['id'] for b in (businesses_response.data or [])]

        # get the the numbers from th customers table for thise busienss ids
        customer_numbers = []
        for biz_id in business_ids:
            customers_response = (
                self.supabase_client.table('customers')
                .select('phone')
                .eq('business_id', biz_id)
                .execute()
            )

            customer_numbers.extend(customers_response.data)

        indsutry_numbers = [num['phone'] for num in customer_numbers]
        
        # Count how many times each customer appears
        customer_counts = Counter(indsutry_numbers)

        total_customers = len(customer_counts)

        # Customers who came back (appear more than once)
        retained_customers = sum(1 for c in customer_counts.values() if c > 1)

        # Retention rate
        retention_rate = (retained_customers / total_customers) * 100

        return round(retention_rate,2)
    
    def industry_average_order_value(self, industry):
        """returns an average order value for that industry"""

        # get business ids that belong to that indsutry
        businesses_response = (
            self.supabase_client.table('businesses')
            .select('id')
            .eq('industry', industry)
            .execute()
        )
        business_ids = [b['id'] for b in (businesses_response.data or [])]

        orders = []

        # get a list of orsers fo those busienss ids
        for biz_id in business_ids:
            order_response = (
                self.supabase_client.table('orders')
                .select('total_amount')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .eq('business_id', biz_id)
                .execute()
            )

            orders.extend(order_response.data or [])

        industry_orders = [amount['total_amount'] for amount in orders]
        order_average = round(float(sum(industry_orders)/len(industry_orders)), 2)
        return order_average
    

#test = Industry()





 