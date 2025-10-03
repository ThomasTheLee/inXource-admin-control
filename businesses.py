# %%
from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd


from settings import SettingsManager
settings_manager = SettingsManager()

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
class Businesses(Clients):
    """Manages the businesses data in the inXource platform"""

    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        super().__init__()

    def get_business_deatils(self, business_id):
        """Returns the business details using the business ID"""
        business_details = {}

        try:
            # Fetch business details
            business_response = (
                self.supabase_client.table('businesses')
                .select('*')
                .eq('id', business_id)
                .execute()
            )

            if not business_response.data:
                return {}

            business = business_response.data[0]  # first matching business
            business_details['business_id'] = business.get('id')
            business_details['business_name'] = business.get('business_name')
            business_details['indsutry'] = business.get('industry')
            business_details['wallet_balance'] = business.get('wallet_balance')
            business_details['phone'] = business.get('phone')
            business_details['is_active'] = business.get('is_active')
            business_details['is_deleted'] = business.get('is_deleted')

            # Fetch business owner
            owner_response = (
                self.supabase_client.table('business_owners')
                .select('user_id')
                .eq('business_id', business_id)
                .execute()
            )

            if not owner_response.data:
                print("No owner found for this business.")
                return business_details  # no owner found

            owner = owner_response.data[0]

            # Fetch user details
            user_response = (
                self.supabase_client.table('users')
                .select('*')
                .eq('id', owner['user_id'])
                .execute()
            )

            if user_response.data:
                user = user_response.data[0]
                business_details['user_id'] = user.get('id')
                business_details['user_name'] = user.get('name')
                business_details['user_email'] = user.get('email')

            return business_details

        except Exception as e:
            print(f"Error fetching business details: {e}")
            return {}
   

    def total_businesses(self):
        """Returns the total number of users on the platform"""
        try:
            response = self.supabase_client.table("businesses").select("*").execute()
            if response.data:
                return len(response.data or []) 
            return 0
        except Exception as e:
            print(f"Error fetching users: {e}")
            return 0
        

    def total_businesses_growth_rate(self):
        """Returns the growth rate of businesses on the platform."""
        try:
            # Fetch all businesses once
            response = self.supabase_client.table("businesses").select("*").execute()
            businesses = response.data or []

            # Current total
            current_total = len(businesses)

            # Cutoff date
            one_month_ago = datetime.now() - timedelta(days=30)

            # Count businesses that existed 30 days ago
            businesses_30_days_ago = 0
            for biz in businesses:
                created_at = biz.get("created_at")
                if created_at:
                    try:
                        created_dt = datetime.fromisoformat(created_at)
                        if created_dt <= one_month_ago:
                            businesses_30_days_ago += 1
                    except ValueError:
                        continue

            # Avoid division by zero
            if businesses_30_days_ago == 0:
                return 0.0

            # Growth rate formula
            growth_rate = ((current_total - businesses_30_days_ago) / businesses_30_days_ago) * 100
            return round(growth_rate, 2)

        except Exception as e:
            print(f"Error calculating business growth rate: {e}")
            return 0.0
        
    def new_businesses_registrations(self, days=30):
        """Returns the number of new businesses registered in the last 'days' days."""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            response = self.supabase_client.table("businesses").select("*").execute()
            if response.data:
                new_businesses = [
                    biz for biz in response.data
                    if biz.get("created_at") and datetime.fromisoformat(biz["created_at"]) >= cutoff_date
                ]
                return len(new_businesses)
            return 0
        except Exception as e:
            print(f"Error fetching new businesses: {e}")
            return 0


    def new_businesses_registrations_rate(self, days=30):
        """Returns the growth rate of new business registrations over the last 'days' days."""
        try:
            # Current period
            current_period_count = self.new_businesses_registrations(days)

            # Previous period
            previous_period_count = self.new_businesses_registrations(days * 2) - current_period_count

            # Avoid division by zero
            if previous_period_count == 0:
                return 0.0

            # Growth rate formula
            growth_rate = ((current_period_count - previous_period_count) / previous_period_count) * 100
            return round(growth_rate, 2)

        except Exception as e:
            print(f"Error calculating new business registrations growth rate: {e}")
            return 0.0
        
    def total_active_businesses(self, days=30):
        """returns the total active businesses based on the number on the withdraws i the last 30 days"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            response = self.supabase_client.table("withdrawals").select("business_id, requested_at").execute()
            if response.data:
                active_business_ids = {
                    wd["business_id"] for wd in response.data
                    if wd.get("requested_at") and datetime.fromisoformat(wd["requested_at"]) >= cutoff_date
                }
                return len(active_business_ids)
            return 0
        except Exception as e:
            print(f"Error fetching active businesses: {e}")
            return 0
        
    
    def total_active_businesses_growth_rate(self, days=30):
        """Returns the growth rate of active businesses over the last 'days' days."""
        try:
            # Current period
            current_period_count = self.total_active_businesses(days)

            # Previous period
            previous_period_count = self.total_active_businesses(days * 2) - current_period_count

            # Avoid division by zero
            if previous_period_count == 0:
                return 0.0

            # Growth rate formula
            growth_rate = ((current_period_count - previous_period_count) / previous_period_count) * 100
            return round(growth_rate, 2)

        except Exception as e:
            print(f"Error calculating active business growth rate: {e}")
            return 0.0


    def retrieve_business_information(self, query):
        try:
            print(f"\n[DEBUG] Starting business info retrieval for query: {query}")

            # 1. Search directly in businesses
            business_response = (
                self.supabase_client.table("businesses")
                .select("*")
                .or_(f"business_name.ilike.%{query}%,industry.ilike.%{query}%,company_alias.ilike.%{query}%")
                .execute()
            )
            #print(f"[DEBUG] Business response raw: {business_response}")
            results = business_response.data or []
            #print(f"[DEBUG] Businesses found directly: {len(results)}")

            # 2. Search for matching users (owners)
            user_response = (
                self.supabase_client.table("users")
                .select("id")
                .or_(f"name.ilike.%{query}%,email.ilike.%{query}%,phone.ilike.%{query}%")
                .execute()
            )
            #print(f"[DEBUG] User response raw: {user_response}")
            users = user_response.data or []
            #print(f"[DEBUG] Users matched: {len(users)}")

            if users:
                user_ids = [u["id"] for u in users]
                #print(f"[DEBUG] User IDs: {user_ids}")

                # 3. Get businesses owned by those users
                owner_response = (
                    self.supabase_client.table("business_owners")
                    .select("business_id")
                    .in_("user_id", user_ids)
                    .execute()
                )
                #print(f"[DEBUG] Owner response raw: {owner_response}")
                business_ids = [o["business_id"] for o in (owner_response.data or [])]
                #print(f"[DEBUG] Business IDs from owners: {business_ids}")

                if business_ids:
                    owner_businesses = (
                        self.supabase_client.table("businesses")
                        .select("*")
                        .in_("id", business_ids)
                        .execute()
                    )
                    print(f"[DEBUG] Owner businesses raw: {owner_businesses}")
                    results.extend(owner_businesses.data or [])
                    print(f"[DEBUG] Total results after merging owner businesses: {len(results)}")

           #print(f"[DEBUG] Final results count: {len(results)}")
            return results

        except Exception as e:
            print(f"[ERROR] Exception while searching businesses: {e}")
            return []

     
    def top_performing_categories(self, limit=5):
        """Returns the top performing business categories based on total approved withdrawals."""
        try:
            print(f"[DEBUG] Fetching top performing categories with limit={limit}")

            # Step 1: Fetch withdrawals
            response = (
                self.supabase_client.table("withdrawals")
                .select("business_id, amount")
                .eq("status", "approved")
                .execute()
            )
            withdrawals = response.data or []
            #print(f"[DEBUG] Retrieved {len(withdrawals)} withdrawals")

            # Step 2: Sum per business_id
            sums = defaultdict(float)
            for w in withdrawals:
                sums[w["business_id"]] += w["amount"]
            #print(f"[DEBUG] Summed withdrawals by business_id: {dict(sums)}")

            # Step 3: Sort by total amount
            sorted_sums = sorted(sums.items(), key=lambda x: x[1], reverse=True)
            #print(f"[DEBUG] Sorted businesses by total amount: {sorted_sums}")

            # Step 4: Top business ids
            top_businesses = [bid for bid, _ in sorted_sums[:limit]]
            print(f"[DEBUG] Top business IDs: {top_businesses}")

            # Step 5: Get their industries
            industries_response = (
                self.supabase_client.table("businesses")
                .select("id, industry")
                .in_("id", top_businesses)
                .execute()
            )
            industries = industries_response.data or []
            #print(f"[DEBUG] Retrieved industries: {industries}")

            # Build final list
            result = [
                {
                    "business_id": bid,
                    "total": total,
                    "industry": next(
                        (i["industry"] for i in industries if i["id"] == bid),  # <-- fixed lookup
                        None,
                    ),
                }
                for bid, total in sorted_sums[:limit]
            ]
            #print(f"[DEBUG] Top businesses with industries: {result}")

            # Add "Others"
            if len(sorted_sums) > limit:
                others_total = sum(total for _, total in sorted_sums[limit:])
                result.append({"business_id": "Others", "total": others_total, "industry": "Others"})
                print(f"[DEBUG] Added 'Others' category with total={others_total}")

            #print(f"[DEBUG] Final result: {result}")
            return result

        except Exception as e:
            print(f"Error fetching top performing categories: {e}")
            return []


    def load_business_activity(self, days=settings_manager.business_activity_days):
        """returns information of business activity in the last 'days' days"""
        
        # Initialize business_activity with all expected keys
        business_activity = {
            'period_days': days,
            'new_registrations': 0,
            'new_business_registrations': 0,
            'deactivated_businesses': 0,
            'deleted_businesses': 0,
            'total_withdraws': 0
        }

        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            
            # Get user registrations in the last 'days' days
            users_reg_response = (
                self.supabase_client.table("users")
                .select("*")
                .gte("created_at", cutoff_date.isoformat())
                .execute()
            )

            if users_reg_response.data:
                new_registration_number = len(users_reg_response.data)
                business_activity['new_registrations'] = new_registration_number
                print(f"[DEBUG] New registrations in last {days} days: {new_registration_number}")
            else:
                print(f"[DEBUG] No new user registrations in the last {days} days.")

            # Get new business registrations in the last 'days' days that are active and not deleted
            businesses_reg_response = (
                self.supabase_client.table("businesses")
                .select("*")
                .gte("created_at", cutoff_date.isoformat())
                .eq("is_deleted", False)
                .eq("is_active", True)
                .execute()
            )
            
            if businesses_reg_response.data:
                new_business_registration_number = len(businesses_reg_response.data)
                business_activity['new_business_registrations'] = new_business_registration_number
                print(f"[DEBUG] New business registrations in last {days} days: {new_business_registration_number}")
            else:
                print(f"[DEBUG] No new business registrations in the last {days} days.")

            # Get businesses that are deactivated in the last 'days' days
            deactivated_businesses_response = (
                self.supabase_client.table("businesses")
                .select("*")
                .gte("created_at", cutoff_date.isoformat())  # when the updated at field is made change to updated at
                .eq("is_active", False)
                .eq("is_deleted", False)
                .execute()
            )

            if deactivated_businesses_response.data:
                deactivated_business_number = len(deactivated_businesses_response.data)
                business_activity['deactivated_businesses'] = deactivated_business_number
                print(f"[DEBUG] Deactivated businesses in last {days} days: {deactivated_business_number}")
            else:
                print(f"[DEBUG] No deactivated businesses in the last {days} days.")

            # Get businesses that are deleted in the last 'days' days
            deleted_businesses_response = (
                self.supabase_client.table("businesses")
                .select("*")
                .gte("deleted_date", cutoff_date.isoformat())
                .eq("is_deleted", True)
                .execute()
            )
            
            if deleted_businesses_response.data:
                deleted_business_number = len(deleted_businesses_response.data)
                business_activity['deleted_businesses'] = deleted_business_number
                print(f"[DEBUG] Deleted businesses in last {days} days: {deleted_business_number}")
            else:
                print(f"[DEBUG] No deleted businesses in the last {days} days.")

            # Get withdraws made according to the days input
            withdraws_response = (
                self.supabase_client.table("withdrawals")
                .select("*")
                .gte("requested_at", cutoff_date.isoformat())
                .execute()
            )

            if withdraws_response.data:
                total_withdraws_number = len(withdraws_response.data)
                business_activity['total_withdraws'] = total_withdraws_number
                print(f"[DEBUG] Total withdraws in last {days} days: {total_withdraws_number}")
            else:
                print(f"[DEBUG] No withdraws in the last {days} days.")

            return business_activity

        except Exception as e:
            print(f"Error fetching business activity: {e}")
            # Return the initialized dictionary even on error
            return business_activity
            

    def monthly_business_trend(self):
        """returns a dataframe of monthly businesses registered per month"""
        try:
            response = self.supabase_client.table('businesses').select('*').execute()
            all_businesses = response.data

            # convert to a pandas data frame
            df = pd.DataFrame(all_businesses)

            if df.empty or 'created_at' not in df.columns:
                # Return empty DataFrame if no data
                return pd.DataFrame(columns=['month', 'business_count'])
                        
            #  Ensure created_at is datetime
            df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
            df = df.dropna(subset=['created_at'])

            #  Filter last 12 months
            one_year_ago = pd.Timestamp.now() - pd.DateOffset(months=12)
            df_last_12 = df[df['created_at'] >= one_year_ago]

            #  Extract month and group
            df_last_12['month'] = df_last_12['created_at'].dt.to_period('M')
            monthly_counts = df_last_12.groupby('month').size().reset_index(name='business_count')

            #  Sort by month
            monthly_counts = monthly_counts.sort_values('month').reset_index(drop=True)

            return monthly_counts

        except Exception as e:
            print(f"Exception: {e}")
            # Return empty DataFrame instead of None
            return pd.DataFrame(columns=['month', 'business_count'])


         
    def get_top_performing_industries(self):
        """returns the top 4 performing industries and bundles the rest under others"""

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

        # Step 4: Sort industries by total amount
        sorted_industries = sorted(industry_totals.items(), key=lambda x: x[1], reverse=True)

        # Step 5: Top 4 + bundle rest as "Others"
        top_4 = sorted_industries[:4]
        others = sum([x[1] for x in sorted_industries[4:]])
        if others > 0:
            top_4.append(("Others", others))

        return top_4
    





    


        


