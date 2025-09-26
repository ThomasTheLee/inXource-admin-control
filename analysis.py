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
from file_processor import FileCleaner




load_dotenv()  # loads the .env file

business_manager = Businesses()
settings_manager = SettingsManager()


class AnalAI(FileCleaner):
    """
    Analysis AI class that inherits from FileCleaner.
    Note: Removed @singleton decorator to fix the TypeError.
    """
    
    def __init__(self, api_key):
        super().__init__()
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
                You are a business analyst for inXource, a platform for vendors.

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
        """Stores the weekly report in the database only if 7 days have passed since the last upload"""

        # Fetch the most recent weekly report
        latest = (
            self.supabase_client.table('admin_insights')
            .select('created_at')
            .eq('type', 'weekly')
            .order('created_at', desc=True)
            .limit(1)
            .execute()
        )

        # Check if there is a record
        if latest.data and len(latest.data) > 0:
            last_created_at_str = latest.data[0]['created_at']
            last_created_at = datetime.fromisoformat(last_created_at_str.replace('Z', '+00:00'))  # handle UTC Z
            seven_days_ago = datetime.utcnow() - timedelta(days=7)

            if last_created_at > seven_days_ago:
                # Less than 7 days since last report
                return {"error": "Weekly report already uploaded within the last 7 days."}

        # Turn data into JSON
        processed_data = json.dumps(data)

        # Insert the new report
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


    def clean_file(self, file):
        """
        Returns a cleaned DataFrame from an uploaded file.
        File can be a file path, file-like object, or Flask FileStorage object.
        """
        df = None  # ensure variable is always defined
        
        # Handle Flask FileStorage objects
        if hasattr(file, 'filename'):
            filename = file.filename.lower()
            if filename.endswith(".csv"):
                if hasattr(file, 'stream'):
                    file.stream.seek(0)
                    df = pd.read_csv(file.stream)
                else:
                    df = pd.read_csv(file)  # fallback
            elif filename.endswith((".xls", ".xlsx")):
                try:
                    df = pd.read_excel(file.stream)
                except Exception:
                    df = pd.read_excel(file)  # fallback
            else:
                raise ValueError(f"Unsupported file format: {filename}. Please upload CSV or Excel only.")
        else:
            # Handle file paths
            ext = str(file).lower()
            if ext.endswith(".csv"):
                df = pd.read_csv(file)
            elif ext.endswith((".xls", ".xlsx")):
                df = pd.read_excel(file)
            else:
                raise ValueError(f"Unsupported file format: {file}. Please upload CSV or Excel only.")

        if df is None:
            raise ValueError("Could not read the provided file into a DataFrame.")

        return self.clean_all(df)


    
    def ai_analyse_df(self, df):
        """
        Returns DataFrames of relationships that have been analysed from the dataframe by OpenAI.
        The AI identifies patterns and returns structured data suitable for chart creation.
        
        :param df: pandas DataFrame to analyze
        :return: dict containing chart_data and metadata for visualization
        """
        if df is None or not hasattr(df, 'empty'):
            return {"error": "No DataFrame provided for analysis"}
        
        if df.empty:
            return {"error": "DataFrame is empty"}
        
        try:
            # Prepare DataFrame summary for AI analysis
            df_info = self._prepare_dataframe_summary(df)
            
            # Create prompt for AI analysis
            prompt = f"""
    You are a data analyst. Analyze the following dataset and identify meaningful patterns and relationships that can be visualized in charts.

    Dataset Information:
    - Shape: {df.shape[0]} rows, {df.shape[1]} columns
    - Columns: {list(df.columns)}
    - Data types: {df_info['dtypes']}
    - Sample data (first 5 rows): {df_info['sample_data']}
    - Basic statistics: {df_info['statistics']}

    Your task:
    1. Identify 2-3 meaningful patterns or relationships in this data
    2. For each pattern, specify what type of chart would be best (bar, pie, line, scatter, etc.)
    3. IMPORTANT: You can create new calculated columns when they provide business value. For example:
    - If you see 'price' and 'quantity', create 'total_sales' = price * quantity
    - If you see 'revenue' and 'cost', create 'profit' = revenue - cost
    - If you see date columns, create time-based groupings (month, quarter, year)
    - Any other meaningful calculations that reveal business insights
    4. Return the analysis in the following JSON format:

    {{
    "analyses": [
        {{
        "title": "Clear descriptive title for the pattern",
        "description": "Brief explanation of what this pattern shows",
        "chart_type": "bar|pie|line|scatter|histogram",
        "data_transformation": "Describe how to group/aggregate the data",
        "x_column": "column name for x-axis (if applicable)",
        "y_column": "column name for y-axis (if applicable)", 
        "group_by": "column to group by (if applicable)",
        "aggregation": "sum|count|mean|median (if applicable)",
        "calculated_column": {{
            "name": "new_column_name",
            "formula": "description of calculation (e.g., price * quantity)",
            "columns_used": ["col1", "col2"]
        }}
        }}
    ]
    }}

    Focus on patterns that would be interesting to business stakeholders. Avoid overly technical analysis.
    Think creatively about calculated columns that could reveal hidden insights.
    Ensure each analysis can be implemented with the available columns or calculated columns.
    """

            # Get AI analysis
            response = self.open_ai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3
            )
            
            ai_response = response.choices[0].message.content
            
            # Parse AI response (try to extract JSON)
            analyses = self._parse_ai_analysis_response(ai_response)
            
            # Generate actual DataFrames based on AI recommendations
            chart_dataframes = {}
            
            for i, analysis in enumerate(analyses.get('analyses', [])):
                try:
                    # Create calculated column if specified
                    analysis_df = df.copy()  # Work with a copy
                    if 'calculated_column' in analysis and analysis['calculated_column']:
                        analysis_df = self._add_calculated_column(analysis_df, analysis['calculated_column'])
                    
                    chart_df = self._create_chart_dataframe(analysis_df, analysis)
                    if not chart_df.empty:
                        chart_key = f"chart_{i+1}_{analysis['chart_type']}"
                        chart_dataframes[chart_key] = {
                            'dataframe': chart_df,
                            'metadata': {
                                'title': analysis['title'],
                                'description': analysis['description'],
                                'chart_type': analysis['chart_type'],
                                'x_label': analysis.get('x_column', 'Category'),
                                'y_label': analysis.get('y_column', 'Value'),
                                'calculated_column': analysis.get('calculated_column')
                            }
                        }
                except Exception as e:
                    print(f"Error creating chart dataframe for analysis {i+1}: {str(e)}")
                    continue
            
            return chart_dataframes
            
        except Exception as e:
            print(f"Error in ai_analyse_df: {str(e)}")
            return {"error": f"Analysis failed: {str(e)}"}

    def _add_calculated_column(self, df, calc_config):
        """Add a calculated column to the DataFrame based on AI recommendations"""
        try:
            if not calc_config or 'name' not in calc_config or 'columns_used' not in calc_config:
                return df
            
            col_name = calc_config['name']
            columns_used = calc_config['columns_used']
            formula = calc_config.get('formula', '').lower()
            
            # Verify all required columns exist
            if not all(col in df.columns for col in columns_used):
                print(f"Missing columns for calculation: {columns_used}")
                return df
            
            # Perform common calculations based on formula description
            if len(columns_used) == 2:
                col1, col2 = columns_used[0], columns_used[1]
                
                if any(word in formula for word in ['multiply', '*', 'total', 'sales']):
                    # Multiplication (e.g., price * quantity = total_sales)
                    df[col_name] = df[col1] * df[col2]
                elif any(word in formula for word in ['subtract', '-', 'profit', 'difference']):
                    # Subtraction (e.g., revenue - cost = profit)
                    df[col_name] = df[col1] - df[col2]
                elif any(word in formula for word in ['add', '+', 'sum', 'total']):
                    # Addition
                    df[col_name] = df[col1] + df[col2]
                elif any(word in formula for word in ['divide', '/', 'ratio', 'rate']):
                    # Division (avoid division by zero)
                    df[col_name] = df[col1] / df[col2].replace(0, np.nan)
                else:
                    # Default to multiplication if unclear
                    df[col_name] = df[col1] * df[col2]
                    
            elif len(columns_used) == 1:
                col1 = columns_used[0]
                
                if any(word in formula for word in ['percentage', 'percent', '%']):
                    # Convert to percentage
                    df[col_name] = df[col1] * 100
                elif any(word in formula for word in ['square', 'squared']):
                    # Square the value
                    df[col_name] = df[col1] ** 2
                elif any(word in formula for word in ['absolute', 'abs']):
                    # Absolute value
                    df[col_name] = df[col1].abs()
                else:
                    # Default: just copy the column
                    df[col_name] = df[col1]
            
            print(f"Added calculated column '{col_name}' using formula: {formula}")
            return df
            
        except Exception as e:
            print(f"Error adding calculated column: {str(e)}")
            return df

    def _prepare_dataframe_summary(self, df):
        """Prepare a summary of the DataFrame for AI analysis"""
        # Get basic info
        summary = {
            'dtypes': df.dtypes.to_dict(),
            'sample_data': df.head(5).to_dict('records'),
            'statistics': {}
        }
        
        # Get statistics for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            summary['statistics']['numeric'] = df[numeric_cols].describe().to_dict()
        
        # Get value counts for categorical columns (top 5 values)
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        summary['statistics']['categorical'] = {}
        for col in categorical_cols[:3]:  # Limit to first 3 categorical columns
            value_counts = df[col].value_counts().head(5).to_dict()
            summary['statistics']['categorical'][col] = value_counts
        
        return summary

    def _parse_ai_analysis_response(self, response_text):
        """Parse the AI response to extract analysis recommendations"""
        try:
            # Try to find JSON in the response
            import re
            json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                return json.loads(json_str)
            else:
                # Fallback: create a basic analysis structure
                return {
                    "analyses": [
                        {
                            "title": "Data Overview",
                            "description": "Basic data distribution",
                            "chart_type": "bar",
                            "data_transformation": "Group by first categorical column",
                            "aggregation": "count"
                        }
                    ]
                }
        except Exception as e:
            print(f"Error parsing AI response: {str(e)}")
            # Return fallback analysis
            return {
                "analyses": [
                    {
                        "title": "Data Summary",
                        "description": "Basic data analysis",
                        "chart_type": "bar", 
                        "aggregation": "count"
                    }
                ]
            }

    def _create_chart_dataframe(self, df, analysis):
        """Create a DataFrame suitable for charting based on AI analysis"""
        try:
            chart_type = analysis.get('chart_type', 'bar')
            
            if chart_type == 'pie':
                return self._create_pie_chart_data(df, analysis)
            elif chart_type == 'bar':
                return self._create_bar_chart_data(df, analysis)
            elif chart_type == 'line':
                return self._create_line_chart_data(df, analysis)
            elif chart_type == 'scatter':
                return self._create_scatter_chart_data(df, analysis)
            elif chart_type == 'histogram':
                return self._create_histogram_data(df, analysis)
            else:
                # Default to bar chart
                return self._create_bar_chart_data(df, analysis)
                
        except Exception as e:
            print(f"Error creating chart dataframe: {str(e)}")
            return pd.DataFrame()

    def _create_pie_chart_data(self, df, analysis):
        """Create data suitable for pie charts"""
        group_col = analysis.get('group_by') or analysis.get('x_column')
        
        # Find a suitable categorical column if not specified
        if not group_col:
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns
            if len(categorical_cols) > 0:
                group_col = categorical_cols[0]
            else:
                return pd.DataFrame()
        
        if group_col not in df.columns:
            return pd.DataFrame()
        
        # Create pie chart data (value counts)
        pie_data = df[group_col].value_counts().reset_index()
        pie_data.columns = ['category', 'value']
        
        return pie_data

    def _create_bar_chart_data(self, df, analysis):
        """Create data suitable for bar charts"""
        group_col = analysis.get('group_by') or analysis.get('x_column')
        value_col = analysis.get('y_column')
        aggregation = analysis.get('aggregation', 'count')
        
        # Auto-select columns if not specified
        if not group_col:
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns
            group_col = categorical_cols[0] if len(categorical_cols) > 0 else df.columns[0]
        
        if not value_col and aggregation != 'count':
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            value_col = numeric_cols[0] if len(numeric_cols) > 0 else None
        
        if group_col not in df.columns:
            return pd.DataFrame()
        
        # Create aggregated data
        if aggregation == 'count':
            bar_data = df[group_col].value_counts().reset_index()
            bar_data.columns = ['category', 'value']
        elif value_col and value_col in df.columns:
            if aggregation == 'sum':
                bar_data = df.groupby(group_col)[value_col].sum().reset_index()
            elif aggregation == 'mean':
                bar_data = df.groupby(group_col)[value_col].mean().reset_index()
            elif aggregation == 'median':
                bar_data = df.groupby(group_col)[value_col].median().reset_index()
            else:
                bar_data = df.groupby(group_col)[value_col].count().reset_index()
            
            bar_data.columns = ['category', 'value']
        else:
            # Fallback to count
            bar_data = df[group_col].value_counts().reset_index()
            bar_data.columns = ['category', 'value']
        
        return bar_data

    def _create_line_chart_data(self, df, analysis):
        """Create data suitable for line charts"""
        x_col = analysis.get('x_column')
        y_col = analysis.get('y_column')
        
        # Auto-select columns
        if not x_col:
            # Look for date columns first, then numeric
            date_cols = df.select_dtypes(include=['datetime64']).columns
            if len(date_cols) > 0:
                x_col = date_cols[0]
            else:
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                x_col = numeric_cols[0] if len(numeric_cols) > 0 else df.columns[0]
        
        if not y_col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            y_col = numeric_cols[0] if len(numeric_cols) > 0 else None
        
        if x_col not in df.columns or (y_col and y_col not in df.columns):
            return pd.DataFrame()
        
        # Create line chart data
        if y_col:
            line_data = df[[x_col, y_col]].dropna()
            line_data = line_data.sort_values(x_col)
            line_data.columns = ['x', 'y']
        else:
            # Count occurrences over x_col
            line_data = df[x_col].value_counts().sort_index().reset_index()
            line_data.columns = ['x', 'y']
        
        return line_data

    def _create_scatter_chart_data(self, df, analysis):
        """Create data suitable for scatter plots"""
        x_col = analysis.get('x_column')
        y_col = analysis.get('y_column')
        
        # Auto-select numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if not x_col and len(numeric_cols) > 0:
            x_col = numeric_cols[0]
        if not y_col and len(numeric_cols) > 1:
            y_col = numeric_cols[1]
        
        if (x_col not in df.columns or y_col not in df.columns or 
            x_col not in numeric_cols or y_col not in numeric_cols):
            return pd.DataFrame()
        
        # Create scatter plot data
        scatter_data = df[[x_col, y_col]].dropna()
        scatter_data.columns = ['x', 'y']
        
        return scatter_data

    def _create_histogram_data(self, df, analysis):
        """Create data suitable for histograms"""
        col = analysis.get('x_column') or analysis.get('y_column')
        
        # Auto-select numeric column
        if not col:
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            col = numeric_cols[0] if len(numeric_cols) > 0 else None
        
        if not col or col not in df.columns:
            return pd.DataFrame()
        
        # Create histogram data (binned counts)
        hist_data, bin_edges = np.histogram(df[col].dropna(), bins=10)
        
        # Create bin labels
        bin_labels = [f"{bin_edges[i]:.1f}-{bin_edges[i+1]:.1f}" 
                    for i in range(len(bin_edges)-1)]
        
        histogram_df = pd.DataFrame({
            'category': bin_labels,
            'value': hist_data
        })
        
        return histogram_df
    

# test
"""
api_key = os.getenv('OPEN_AI_TEST_KEY')
test = AnalAI(api_key)
import os

file_path = "messy_sales_data.csv"  # replace with your file name

if os.path.exists(file_path):
    print("File exists")
else:
    print("File does not exist")


cleaned = test.clean_file("messy_sales_data.csv")
print(test.ai_analyse_df(cleaned))
"""


