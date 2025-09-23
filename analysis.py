from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta, timezone
import re
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter
import unicodedata
from itertools import combinations
from selenium import webdriver
from selenium.webdriver.common.by import By
import json

from openai import OpenAI


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



class AnalAI:
    """
    Analysis AI class that inherits from FileCleaner.
    Note: Removed @singleton decorator to fix the TypeError.
    """
    
    def __init__(self, api_key):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key = os.getenv('SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")

        self.supabase_client: Client = create_client(self.supabase_url, self.supabase_service_role_key)
        self.open_ai_client = OpenAI(api_key=api_key)
        self.tables = [
            'withdrawals',
            'users',
            'orders',
            'businesses',
            'industry_trucking',
            'business_owners',
            'business_settings',
        ]
        self.weekly_insights = {}

        self.store_weekly_report(self.weekly_insights)

    def generate_haiku(self):
        """Generate a haiku using OpenAI."""
        try:
            response = self.open_ai_client.chat.completions.create(
                model="gpt-3.5-turbo",  # Fixed model name
                messages=[
                    {"role": "user", "content": "write a haiku about AI"}
                ]
            )
            haiku = response.choices[0].message.content
            print(haiku)
            return haiku
        except Exception as e:
            print(f"Error generating haiku: {str(e)}")
            return None

    def extract_tables(self):
        """
        Extracts the tables defined in self.tables as pandas DataFrames.
        - If 'created_at' exists, gets records from the past 7 days.
        - Otherwise, gets the most recent 14 records.
        """
        dataframe_data = {}
        seven_days_ago = (datetime.utcnow() - timedelta(days=7)).isoformat()

        for table in self.tables:
            try:
                # First, fetch column info to check for 'created_at'
                columns_resp = self.supabase_client.table(table).select('*').limit(1).execute()
                df_columns = pd.DataFrame(columns_resp.data)

                if 'created_at' in df_columns.columns:
                    # Table has 'created_at', filter last 7 days
                    response = (
                        self.supabase_client.table(table)
                        .select('*')
                        .gte('created_at', seven_days_ago)
                        .order('created_at', desc=True)
                        .execute()
                    )
                else:
                    # No 'created_at', just get most recent 14 records
                    response = (
                        self.supabase_client.table(table)
                        .select('*')
                        .order('id', desc=True)  # Assuming 'id' is auto-incrementing
                        .limit(14)
                        .execute()
                    )

                dataframe_data[table] = pd.DataFrame(response.data)
                
            except Exception as e:
                print(f"Error extracting table {table}: {str(e)}")
                dataframe_data[table] = pd.DataFrame()

        return dataframe_data

    def generate_weekly_prompts(self):
        """
        Generates GPT prompts for weekly insights for all tables.
        Uses extract_tables() internally and loops through each table.
        """
        dataframe_data = self.extract_tables()
        prompts = {}

        for table_name, main_df in dataframe_data.items():
            # Convert main table to string (top 10 rows for context)
            if not main_df.empty:
                main_table_str = main_df.head(10).to_dict(orient='records')
            else:
                main_table_str = "No data available."

            # Include related tables for correlations
            related_tables_str = ""
            if table_name == "withdrawals":
                related_df = dataframe_data.get("orders", pd.DataFrame())
                if not related_df.empty:
                    related_tables_str = f"Related table: orders\n{related_df.head(10).to_dict(orient='records')}\n"
                related_df2 = dataframe_data.get("users", pd.DataFrame())
                if not related_df2.empty:
                    related_tables_str += f"Related table: users\n{related_df2.head(10).to_dict(orient='records')}\n"
            elif table_name == "orders":
                related_df = dataframe_data.get("users", pd.DataFrame())
                if not related_df.empty:
                    related_tables_str = f"Related table: users\n{related_df.head(10).to_dict(orient='records')}\n"
                related_df2 = dataframe_data.get("businesses", pd.DataFrame())
                if not related_df2.empty:
                    related_tables_str += f"Related table: businesses\n{related_df2.head(10).to_dict(orient='records')}\n"
            elif table_name == "users":
                related_df = dataframe_data.get("businesses", pd.DataFrame())
                if not related_df.empty:
                    related_tables_str = f"Related table: businesses\n{related_df.head(10).to_dict(orient='records')}\n"

            # Build the prompt with structured output requirement
            prompt = f"""
You are a business analyst for InXource, a platform for vendors.

Your task is to provide actionable insights, not just raw numbers.
Focus on patterns, correlations, anomalies, and opportunities.

Main table for analysis: {table_name}
Data:
{main_table_str}

{related_tables_str}

Guidelines:
1. Identify patterns and trends in the main table.
2. Correlate the main table data with related tables (if any).
3. Highlight anomalies, opportunities, or unusual behaviors.
4. Provide actionable recommendations for marketing, platform improvements, or user engagement.
5. Present insights clearly in human-readable language.

IMPORTANT: Structure your response as follows:
CONCERNS:
[List your main concerns and issues identified in the data]

RECOMMENDATIONS:
[List your actionable recommendations based on the concerns]

Keep each section clear and separate.
"""
            prompts[table_name] = prompt

        return prompts

    def parse_ai_response(self, response_text):
        """
        Parse the AI response to extract concerns and recommendations.
        Returns a dictionary with 'concern' and 'recommendation' keys.
        """
        try:
            # Split the response by sections
            sections = response_text.split('CONCERNS:')
            if len(sections) < 2:
                # Fallback if format is different
                return {
                    'concern': 'Format parsing error - full response stored',
                    'recommendation': response_text
                }
            
            # Extract everything after CONCERNS:
            after_concerns = sections[1]
            
            # Split by RECOMMENDATIONS:
            rec_sections = after_concerns.split('RECOMMENDATIONS:')
            
            if len(rec_sections) < 2:
                # If no RECOMMENDATIONS section found
                concern_text = after_concerns.strip()
                recommendation_text = "No specific recommendations provided"
            else:
                concern_text = rec_sections[0].strip()
                recommendation_text = rec_sections[1].strip()
            
            return {
                'concern': concern_text,
                'recommendation': recommendation_text
            }
            
        except Exception as e:
            # Fallback in case of parsing error
            return {
                'concern': f'Parsing error: {str(e)}',
                'recommendation': response_text
            }

    def generate_weekly_insights(self):
        """
        Sends weekly prompts to GPT for all tables and stores the insights in structured format.
        """
        self.weekly_insights = {}
        prompts = self.generate_weekly_prompts()

        for table_name, prompt in prompts.items():
            try:
                response = self.open_ai_client.chat.completions.create(
                    model="gpt-3.5-turbo",  # Fixed model name
                    messages=[
                        {"role": "user", "content": prompt}
                    ]
                )
                
                raw_response = response.choices[0].message.content
                
                # Parse the response into structured format
                parsed_response = self.parse_ai_response(raw_response)
                self.weekly_insights[table_name] = parsed_response

            except Exception as e:
                self.weekly_insights[table_name] = {
                    'concern': f"Error generating insight: {str(e)}",
                    'recommendation': "Unable to generate recommendations due to error"
                }

        return self.weekly_insights

    def store_weekly_report(self, data):
        """stores the weekly report in the database"""

        # turn data to JSON
        processed_data = json.dumps(data)

        response = (
            self.supabase_client.table('admin_insights')
            .insert({
                "insight": processed_data,
                "type": "weekly"
            })
            .execute()
        )

        return response

    def grab_weekly_insights(self):
        """returns the most recent weekly insight from the admin_insights table"""

        response = (
            self.supabase_client.table('admin_insights')
            .select('insight, created_at')
            .order('created_at', desc=True)
            .limit(1)
            .execute()
        )

        insight = response.data[0] if response.data else None
        return insight

