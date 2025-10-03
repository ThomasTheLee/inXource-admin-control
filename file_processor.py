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
from clients import Clients


from businesses import Businesses
from settings import SettingsManager




load_dotenv()  # loads the .env file

business_manager = Businesses()
settings_manager = SettingsManager()



class FileCleaner(Clients):
    """Manages the user's data in the inXource platform with minimal cleaning for AI analysis"""

    def __init__(self):
        super().__init__()

    def detect_dates(self, df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
        """Detects obvious date columns and converts them to datetime."""
        date_cols = []
        for col in df.columns:
            # Only convert if it's very obviously dates (95% success rate)
            if pd.api.types.is_string_dtype(df[col]) or pd.api.types.is_object_dtype(df[col]):
                converted = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
                if converted.notna().sum() / len(df) > 0.95:
                    df[col] = converted
                    date_cols.append(col)
        return df, date_cols

    def light_cleaning(self, df: pd.DataFrame) -> pd.DataFrame:
        """Very light cleaning - just basic standardization without removing data."""
        df, date_cols = self.detect_dates(df)

        # Only do very basic string cleanup - preserve the actual content
        for col in df.columns:
            if col not in date_cols and (pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object'):
                # Just strip whitespace, don't change case or remove characters
                df[col] = df[col].astype(str).str.strip()
                # Replace empty strings with NaN for consistency
                df[col] = df[col].replace('', np.nan)

        return df

    def handle_only_extreme_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Only handle completely empty columns or columns with 95%+ missing data.
        For AI analysis, we want to preserve as much data as possible.
        """
        df, date_cols = self.detect_dates(df)

        for col in df.columns:
            missing_ratio = pd.isna(df[col]).mean()
            
            # Only drop if 95% or more is missing
            if missing_ratio >= 0.95:
                df.drop(columns=[col], inplace=True)
                print(f"Dropped column '{col}' - {missing_ratio:.1%} missing data")
            else:
                # For columns with some missing data, just fill strategically
                if col in date_cols or pd.api.types.is_datetime64_any_dtype(df[col]):
                    # For dates, keep NaT as is - AI can handle it
                    continue
                elif pd.api.types.is_numeric_dtype(df[col]):
                    # Keep NaN as is for numeric - AI can handle it
                    continue
                else:
                    # For text, only fill if there are just a few missing values
                    if missing_ratio < 0.1:  # Less than 10% missing
                        df[col].fillna('Unknown', inplace=True)

        return df

    def basic_duplicate_removal(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove only exact duplicates across all columns."""
        initial_count = len(df)
        df = df.drop_duplicates()
        removed = initial_count - len(df)
        if removed > 0:
            print(f"Removed {removed} exact duplicate rows")
        return df

    def preserve_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Light data type optimization without losing information.
        Only converts obviously numeric columns that are stored as strings.
        """
        df, date_cols = self.detect_dates(df)

        for col in df.columns:
            if col in date_cols:
                continue

            # Only convert if it's clearly numeric but stored as string
            if pd.api.types.is_string_dtype(df[col]):
                # Try to convert to numeric, but only if most values are numeric
                numeric_converted = pd.to_numeric(df[col], errors='coerce')
                non_null_ratio = numeric_converted.notna().sum() / df[col].notna().sum()
                
                # Only convert if 90%+ of non-null values are numeric
                if non_null_ratio > 0.9:
                    df[col] = numeric_converted
                    print(f"Converted '{col}' to numeric (was string)")

        return df

    def final_check(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Final gentle check - only handle completely problematic data.
        """
        # Check for columns that are entirely null/empty
        completely_empty = df.isnull().all()
        if completely_empty.any():
            empty_cols = completely_empty[completely_empty].index.tolist()
            df = df.drop(columns=empty_cols)
            print(f"Removed completely empty columns: {empty_cols}")

        # Report final status
        missing_summary = df.isnull().sum()
        if missing_summary.sum() > 0:
            print("\nFinal missing data summary:")
            for col, missing_count in missing_summary[missing_summary > 0].items():
                missing_pct = (missing_count / len(df)) * 100
                print(f"  {col}: {missing_count} missing ({missing_pct:.1f}%)")
        else:
            print("\n✓ No missing values in final dataset")

        return df

    def clean_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Minimal cleaning approach optimized for AI analysis.
        Preserves original data structure and content as much as possible.
        """
        print("Starting minimal data cleaning for AI analysis...")
        print(f"Original dataset: {len(df)} rows × {len(df.columns)} columns")
        
        # 1. Very light cleaning
        print("\n1. Basic cleaning (whitespace, empty strings)...")
        df = self.light_cleaning(df)

        # 2. Only handle extreme missing data
        print("2. Handling only extreme missing data (95%+ missing)...")
        df = self.handle_only_extreme_missing(df)

        # 3. Remove exact duplicates only
        print("3. Removing exact duplicate rows...")
        df = self.basic_duplicate_removal(df)

        # 4. Light data type optimization
        print("4. Basic data type optimization...")
        df = self.preserve_data_types(df)

        # 5. Final check
        print("5. Final validation...")
        df = self.final_check(df)

        print(f"\n✓ Cleaning completed!")
        print(f"Final dataset: {len(df)} rows × {len(df.columns)} columns")
        print("Dataset is ready for AI analysis with minimal data loss.")
        
        return df