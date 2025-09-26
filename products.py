from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta


from settings import SettingsManager
settings_manager = SettingsManager()

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
class Products:
    """Manages the products data in the inXource platform"""

    def __init__(self):
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_service_role_key = os.getenv('SERVICE_ROLE_KEY')

        if not self.supabase_url or not self.supabase_service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")

        self.supabase_client: Client = create_client(self.supabase_url, self.supabase_service_role_key) 

    def total_products(self):
        """Returns the total number of products from businesses that are active"""
        try:
            response = (
                self.supabase_client.table("products")
                .select("id, business:business_id (is_active)")  # join on business_id
                .execute()
            )

            if not response.data:
                return 0

            # Filter only products where business.is_active == True
            active_products = [p for p in response.data if p["business"]["is_active"]]

            return len(active_products)

        except Exception as e:
            print(f"Exception: {e}")
            return 0
        


    def total_products_growth(self):
        """Returns the growth rate of how products have grown in the last 30 days"""

        month_ago_date = datetime.utcnow() - timedelta(days=30)

        try:
            # Products created in the last 30 days
            new_response = (
                self.supabase_client.table('products')
                .select('*')
                .gte('created_at', month_ago_date.isoformat())
                .execute()
            )
            new_products = new_response.data

            # Products created before 30 days ago
            old_response = (
                self.supabase_client.table('products')
                .select('*')
                .lte('created_at', month_ago_date.isoformat())
                .execute()
            )
            old_products = old_response.data

            if len(old_products) == 0:  # Avoid division by zero
                return None  

            growth_rate = ((len(new_products) - len(old_products)) / len(old_products)) * 100
            return growth_rate

        except Exception as e:
            print(f"Exception: {e}")
            return None
        
    def low_stock_count(self):
        """returns the total number of products with a low stock"""
        try:
            response = (
                self.supabase_client.table('stock_table')
                .select('*')
                .lt('quantity', settings_manager.low_stock_count)
                .execute()
            )

            return len(response.data)
        except Exception as e:
            print(f"Exception: {e}")
            return None
        
    def low_stock_percent(self):
        """Gives a percentage of low stock products out of all products"""
        try:
            response = self.supabase_client.table('stock_table').select('quantity').execute()

            if not response.data:
                return 0

            low_stock = [
                stock.get('quantity', 0)
                for stock in response.data
                if stock.get('quantity', 0) < settings_manager.low_stock_count
            ]

            low_percent_stock = (len(low_stock) / len(response.data)) * 100
            return round(low_percent_stock, 2)

        except Exception as e:
            print(f"Exception: {e}")
            return None
        
    
    def total_revenue(self):
        """returns the total revenue for sales with complete orders"""
        try:
            response = (
                self.supabase_client.table('orders')
                .select('total_amount')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .execute()
                )

            total_amount = round(float(sum([amount['total_amount'] for amount in response.data])),2)

            return total_amount

        except Exception as e:
            print(f"Exception: {e}")
            return None
        
    

    def total_revenue_growth(self):
        """Returns the growth rate of total revenue from last month"""

        month_ago_date = datetime.utcnow() - timedelta(days=30)

        try:
            # Orders in the last 30 days
            new_response = (
                self.supabase_client.table('orders')
                .select('total_amount')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .gte('created_at', month_ago_date.isoformat())
                .execute()
            )
            new_orders = new_response.data or []

            # Orders before 30 days ago
            old_response = (
                self.supabase_client.table('orders')
                .select('total_amount')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .lte('created_at', month_ago_date.isoformat())
                .execute()
            )
            old_orders = old_response.data or []

            old_revenue_total = sum([float(order['total_amount']) for order in old_orders])
            new_revenue_total = sum([float(order['total_amount']) for order in new_orders])

            if old_revenue_total == 0:
                return None  # Avoid division by zero

            rate = ((new_revenue_total - old_revenue_total) / old_revenue_total) * 100
            return round(rate, 2)

        except Exception as e:
            print(f"Exception: {e}")
            return None
        
    
    def product_ranking(self, method=settings_manager.product_performance_by):
        """
        Returns a dictionary of product performance ranked by the specified method in settings.
        method can be either 'volume' (total quantity sold) or 'revenue' (total sales amount).
        """
        order_response = (
            self.supabase_client.table('orders')
            .select('product_id, quantity, total_amount, products(name, category)')
            .eq('order_payment_status', 'completed')
            .eq('order_status', 'completed')
            .execute()
        )

        data = order_response.data

        performance = {}

        for order in data:
            product_id = order["product_id"]
            product_name = order["products"]["name"] if "products" in order else None
            product_category = order["products"]["category"] if "products" in order else "unknown"

            if product_id not in performance:
                performance[product_id] = {
                    "name": product_name,
                    "quantity": 0,
                    "revenue": 0.0,
                    "category": product_category,
                }

            performance[product_id]["quantity"] += order.get("quantity", 0)
            performance[product_id]["revenue"] += order.get("total_amount", 0.0)

        # Choose sorting method
        if method == "volume":
            ranked = dict(
                sorted(performance.items(), key=lambda x: x[1]["quantity"], reverse=True)
            )
        else:  # default to revenue
            ranked = dict(
                sorted(performance.items(), key=lambda x: x[1]["revenue"], reverse=True)
            )

        return ranked



        
        



# {'c1db8d27-b4fe-4f8b-845c-6c0aeb33ce3b': {'name': 'ok ok', 'quantity': 2, 'revenue': 2.0, 'category': 'Cakes'}}     
      
test = Products()
print(test.product_ranking())