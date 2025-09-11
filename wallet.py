from supabase import create_client, Client
from dotenv import load_dotenv
import os

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
        self.supabase_url: str = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key: str = os.getenv('SERVICE_ROLE_KEY')

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
        try:
            response = (
                self.client.table('orders')
                .select('partialAmountTotal')
                .eq('order_payment_status', 'completed')
                .execute()
            )

            # Sum the 'partialAmountTotal' from each row
            total = sum(item.get('partialAmountTotal', 0) for item in response.data)
            return total

        except Exception as e:
            print(f"Exception: {e}")
            return 0    

    def get_pending_withdrawal_ids(self):
        """Returns a list of withdrawal records with business_id, id, and status"""

        try:
            withdrawal_response = (
                self.client.table('withdrawals')
                .select('business_id', 'id', 'status', 'requested_at','amount','method')
                .eq('status', 'pending')
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

        pending_withdrawals = self.get_pending_withdrawal_ids()
        business_manager = Businesses()

        withdrawal_details = [] 
        for withdrawal in pending_withdrawals:
            business_details = business_manager.get_business_deatils(withdrawal['business_id'])
            if business_details:
                # add withdrawal-specific fields
                business_details['withdrawal_id'] = withdrawal['id']
                business_details['withdrawal_status'] = withdrawal['status']
                business_details['requested_at'] = withdrawal['requested_at']
                business_details['amount'] = withdrawal['amount']
                business_details['method'] = withdrawal['method']
                withdrawal_details.append(business_details) 

        return withdrawal_details


    def aprove_withdrawal(self, withdrawal_id):
        """Approves a withdrawal request by updating its status to 'approved'"""

        try:
            response = (
                self.client.table('withdrawals')
                .update({'status': 'approved'})
                .eq('id', withdrawal_id)
                .execute()
            )

            if response.error:
                print(f"Error approving withdrawal: {response.error.message}")
                return False

            return True

        except Exception as e:
            print(f"Exception: {e}")
            return False



test = Wallet()
print(test.get_pending_withdrawal_ids())
print(test.load_pending_withdrawals())



