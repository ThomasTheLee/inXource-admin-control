from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from clients import Clients

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
class Auth(Clients):
    """Manages the users data in the inXource platform"""

    def __init__(self):
        super().__init__()

    def login(self, user, password):
        """Returns the user information after an attempt to login"""
        try:
            response = (
                self.supabase_client.table('staff')
                .select('id, user_name, email, password, role')
                .or_(f"user_name.eq.{user},email.eq.{user}")
                .execute()
            )

            data = response.data

            if not data:
                # No user found
                return {'logged': False}

            staff_details = data[0]

            if staff_details['password'] == password:
                staff_details['logged'] = True
                staff_details['staff_id'] = staff_details['id']
            else:
                staff_details['logged'] = False

            return staff_details

        except Exception as e:
            print(f"Error logging in: {e}")
            return {'logged': False}
        
    
    
    def edit_profile(self, staff_id, username=None, email=None):
        """Updates the profile details for a specific user by staff_id.
        Only updates fields that are provided (not None)."""

        # Prepare dictionary with only the fields to update
        update_data = {}
        if username is not None:
            update_data['user_name'] = username
        if email is not None:
            update_data['email'] = email

        if not update_data:
            print("Nothing to update")
            return None  # No fields provided

        try:
            response = (
                self.supabase_client.table('staff')
                .update(update_data)
                .eq('id', staff_id)  # target the correct user
                .execute()
            )

            if response.data:
                print('Profile updated successfully')
                return response.data[0]  # return the updated record

            print('No changes made or user not found')
            return {}

        except Exception as e:
            print(f'Exception: {e}')
            return {}


    

    def edit_password(self, staff_id, current_password=None, new_password=None, confirm_password=None):
        """Updates the user's password after verifying the current password"""

        if not current_password:
            print("Current password is required")
            return None

        if not new_password or not confirm_password:
            print("Both new password and confirmation are required")
            return None

        if new_password != confirm_password:
            print("New passwords do not match")
            return None

        try:
            # Fetch the current password from the database
            user_data = (
                self.supabase_client.table('staff')
                .select('password')
                .eq('id', staff_id)
                .execute()
            )

            if not user_data.data:
                print("User not found")
                return None

            stored_password = user_data.data[0]['password']

            if current_password != stored_password:
                print("Current password is incorrect")
                return None

            # Update the password
            response = (
                self.supabase_client.table('staff')
                .update({'password': new_password})
                .eq('id', staff_id)
                .execute()
            )

            if response.data:
                print('Password updated successfully')
                return response.data[0]

            print('No changes made or user not found')
            return {}

        except Exception as e:
            print(f'Exception: {e}')
            return {}

    def add_staff(self, username, email, password, role, nrc):
        """Adds a new staff member to the database"""
        try:
            new_staff = {
                'user_name': username,
                'email': email,
                'password': password,
                'role': role,
                'nrc_number': nrc,
                'is_active': True,
            }

            response = (
                self.supabase_client.table('staff')
                .insert(new_staff)
                .execute()
            )

            if response.data:
                print('New staff member added successfully')
                return response.data[0]

            print('Failed to add new staff member')
            return {}

        except Exception as e:
            print(f'Exception: {e}')
            return {}
        
    def load_staff(self):
        """Fetches all staff members from the database"""
        try:
            response = (
                self.supabase_client.table('staff')
                .select('*')
                .execute()
            )

            if response.data:
                return response.data

            print('No staff members found')
            return []

        except Exception as e:
            print(f'Exception: {e}')
            return []
        

    def delete_staff(self, staff_id):
            """Deletes a staff member by their ID"""
            SUPER_ID = os.getenv('SUPER_ID')
            if staff_id == SUPER_ID:
                print("Cannot delete the super admin account")
                return False
            try:
                response = (
                    self.supabase_client.table('staff')
                    .delete()
                    .eq('id', staff_id)
                    .execute()
                )

                if response.data:
                    print('Staff member deleted successfully')
                    return True

                print('Staff member not found or already deleted')
                return False

            except Exception as e:
                print(f'Exception: {e}')
                return False
            
    
    def edit_staff_user(self, staff_id, username=None, email=None, role=None, nrc=None, is_active=None):
        """Updates the details of a staff member by their ID"""
        update_data = {}
        if username is not None:
            update_data['user_name'] = username
        if email is not None:
            update_data['email'] = email
        if role is not None:
            update_data['role'] = role
        if nrc is not None:
            update_data['nrc_number'] = nrc
        if is_active is not None:
            update_data['is_active'] = is_active

        if not update_data:
            print("Nothing to update")
            return None  # No fields provided

        try:
            response = (
                self.supabase_client.table('staff')
                .update(update_data)
                .eq('id', staff_id)
                .execute()
            )

            if response.data:
                print('Staff member updated successfully')
                return response.data[0]

            print('No changes made or staff member not found')
            return {}

        except Exception as e:
            print(f'Exception: {e}')
            return {}
        
    
        
test = Auth()
print(test.load_staff())