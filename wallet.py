from supabase import create_client, Client
from dotenv import load_dotenv
import os
import datetime

# custom modules
from businesses import Businesses

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
class Wallet:
    """Manages the wallet data in the inXource platform"""

    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key = os.getenv('SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")

        self.client: Client = create_client(self.supabase_url, self.supabase_service_role_key) 

    def total_withdrawal_requests(self):
        """returns a total of pending withdrawal requests"""

        try:
            response = (
                self.client.table('withdrawals')
                .select('*')
                .eq('status', 'pending')
                .execute()
            )

            return len(response.data or [])


        except Exception as e:
            print(f"Exception: {e}")

    def total_inhouse_money(self):
        """Returns the total amount of money that is in inXource"""

        # get the total of all completed orders
        try:
            response = (
                self.client.table('orders')
                .select('partialAmountTotal')
                .eq('order_payment_status', 'completed')
                .execute()
            )

            # Sum the 'partialAmountTotal' from each row
            total_amount_ordered = sum(item.get('partialAmountTotal', 0) for item in response.data)
            
            # get the total of all withdrawals that have been approved
            withdrawal_response = (
                self.client.table('withdrawals')
                .select('amount')
                .eq('status', 'approved')
                .execute()
            )

            total_amount_withdrawn = sum(item.get('amount', 0) for item in withdrawal_response.data)

            # inhouse money is total orders - total withdrawals
            inhouse_money = total_amount_ordered - total_amount_withdrawn
            return inhouse_money

        except Exception as e:
            print(f"Exception: {e}")
            return 0    

    def get_withdrawal_ids(self):
        """Returns a list of withdrawal records with business_id, id, and status"""

        try:
            withdrawal_response = (
                self.client.table('withdrawals')
                .select('*')
                .execute()
            )

            if not withdrawal_response.data:
                print("No pending withdrawal requests found.")
                return []

            return withdrawal_response.data  # return list of dicts

        except Exception as e:
            print(f"Exception: {e}")
            return []


    def load_pending_withdrawals(self):
        """Loads all pending withdrawal requests with business and owner details"""

        withdrawals = self.get_withdrawal_ids()
        business_manager = Businesses()

        withdrawal_details = [] 
        for withdrawal in withdrawals:
            business_details = business_manager.get_business_deatils(withdrawal['business_id'])
            if business_details:
                # add withdrawal-specific fields
                business_details['withdrawal_id'] = withdrawal['id']
                business_details['withdrawal_status'] = withdrawal['status']
                business_details['requested_at'] = withdrawal['requested_at']
                business_details['amount'] = withdrawal['amount']
                business_details['method'] = withdrawal['method']
                business_details['payout_url'] = withdrawal['proof_of_payment'] 
                business_details['processed_at'] = withdrawal['processed_at']
                withdrawal_details.append(business_details) 

        return withdrawal_details
    
    def reduce_wallet_balance(self, business_id, withdraw_amount):
        """Reduces the wallet balance of a business by a specified amount"""

        try:
            # Fetch current wallet balance
            response = (
                self.client.table('businesses')
                .select('wallet_balance')
                .eq('id', business_id)
                .single()
                .execute()
            )

            if not response.data:
                print(f"Business with ID {business_id} not found.")
                return False

            current_balance = response.data.get('wallet_balance', 0)

            if current_balance < withdraw_amount:
                print(f"Insufficient funds in business ID {business_id}'s wallet.")
                return False

            new_balance = current_balance - withdraw_amount

            # Update the wallet balance
            update_response = (
                self.client.table('businesses')
                .update({'wallet_balance': new_balance})
                .eq('id', business_id)
                .execute()
            )

            if update_response.data is None or len(update_response.data) == 0:
                print("Error updating wallet balance: No rows were updated")
                return False

            return True

        except Exception as e:
            print(f"Exception: {e}")
            return False


    def aprove_withdrawal(self, withdrawal_id):
        """Approves a withdrawal request by updating its status to 'approved'"""

        try:
            response = (
                self.client.table('withdrawals')
                .update({'status': 'approved', 'processed_at': datetime.datetime.utcnow().isoformat()})
                .eq('id', withdrawal_id)
                .execute()
            )

            # Check if the update affected any rows
            if response.data is None or len(response.data) == 0:
                print("Error approving withdrawal: No rows were updated")
                return False

            return True

        except Exception as e:
            print(f"Exception: {e}")
            return False


    def upload_payout_proof(self, file_object, withdrawal_id):
        """Upload payout proof file to Supabase storage bucket and return the public URL"""
        try:
            # Validate filename
            filename = file_object.filename
            if not filename:
                print("No filename provided")
                return None

            # Extract extension
            file_extension = filename.rsplit('.', 1)[1].lower() if '.' in filename else ''

            # Create unique filename
            import time
            timestamp = int(time.time())
            unique_filename = f"payout_proof_{withdrawal_id}_{timestamp}.{file_extension}"

            # Read file content
            file_content = file_object.read()

            # Upload to Supabase Storage (raises exception on failure)
            self.client.storage.from_("pay-outs").upload(
                unique_filename,
                file_content,
                {"content-type": file_object.content_type or "application/octet-stream"}
            )

            # Get public URL
            public_url = self.client.storage.from_("pay-outs").get_public_url(unique_filename)

            if public_url:
                print(f"File uploaded successfully: {public_url}")
                return public_url
            else:
                print("Error: could not generate public URL")
                return None

        except Exception as e:
            print(f"Exception uploading file: {e}")
            return None
        

    def update_proof_of_payment(self, withdrawal_id, proof_url):
        """Update the proof_of_payment field in withdrawals table with the uploaded file URL"""
        try:
            response = (
                self.client.table("withdrawals")
                .update({"proof_of_payment": proof_url})
                .eq("id", withdrawal_id)
                .execute()
            )

            # supabase-py usually returns a dict with `data` and maybe `error`
            if not response or not getattr(response, "data", None):
                print(f"Error: no rows updated for withdrawal {withdrawal_id}")
                return False

            print(f"Successfully updated proof of payment for withdrawal {withdrawal_id}")
            return True

        except Exception as e:
            print(f"Exception updating proof of payment: {e}")
            return False

    

    def reject_withdrawal(self, withdrawal_id):
        """rejects a withdrawal request by updating its status to 'rejected'"""

        try:
            response = (
                self.client.table('withdrawals')
                .update({'status': 'rejected'})
                .eq('id', withdrawal_id)
                .execute()
            )

            # Check if the update affected any rows
            if response.data is None or len(response.data) == 0:
                print("Error approving withdrawal: No rows were updated")
                return False

            return True

        except Exception as e:
            print(f"Exception: {e}")
            return False





test = Wallet()




