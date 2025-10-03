from supabase import create_client, Client
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
from analysis import AnalAI
import numpy as np
from typing import Dict

from settings import SettingsManager
settings_manager = SettingsManager()

from clients import Clients
from product_classifier import ProductClassifier


load_dotenv()  # loads the .env file
ai_key = os.getenv('OPEN_AI_TEST_KEY')



def singleton(cls):
    """Decorator to ensure only one instance of a class is created"""
    instances = {}
    
    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    
    return wrapper

@singleton
class Products(Clients):
    """Manages the products data in the inXource platform"""

    def __init__(self):
        super().__init__()

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
            return 0
        
    

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
    


    def ai_product_naming(self, name, description, category):
        """Returns a generic, searchable product type for classification"""
        
        # First, try keyword-based classification (fast and free!)
        product_type = ProductClassifier.classify_from_text(name, description)
        if product_type:
            print(f"  ✓ Matched via keywords: '{product_type}'")
            return product_type
        
        # If keyword matching fails, use AI
        try:
            ai_examples = ProductClassifier.get_ai_examples()
            
            ai_response = self.open_ai_client.chat.completions.create(
                model=settings_manager.open_ai_modal,
                messages=[
                    {
                        "role": "system", 
                        "content": (
                            "You are a product identifier. Your job is to look at a product's name and description, "
                            "then tell me what specific thing it is in ONE word. "
                            "Examples: if it's a phone, say 'phone'. If it's shoes, say 'shoes'. "
                            "DO NOT just repeat the category."
                        )
                    },
                    *ai_examples,  # type: ignore
                    {
                        "role": "user", 
                        "content": f"Product name: {name}\nDescription: {description}\n\nIn one word, what specific product is this?"
                    }
                ],
                temperature=0,
                max_completion_tokens=10
            )

            if ai_response.choices and len(ai_response.choices) > 0:
                message_obj = ai_response.choices[0].message
                if message_obj and message_obj.content:
                    # Clean the response
                    product_type = message_obj.content.strip().lower()
                    product_type = ''.join(c for c in product_type if c.isalpha() or c.isspace())
                    product_type = product_type.split()[0] if product_type.split() else product_type
                    
                    # Validate it's not a broad category
                    if not ProductClassifier.is_valid_product_type(product_type):
                        print(f"  ⚠ AI returned invalid category: '{product_type}'")
                        return None
                    
                    return product_type

        except Exception as e:
            print(f"AI naming error: {e}")

        return None


    def normalize_new_products(self):
        """Normalizes new products and updates Supabase with AI-generated searchable product types"""
        try:
            from datetime import timezone
            
            # Fetch products that haven't been processed yet
            response = (
                self.supabase_client.table("products")
                .select("id, name, description, category, ai_name, ai_name_updated_at")
                .execute()
            )

            if not response.data:
                print("No products found in table.")
                return

            # Filter for products where ai_name is None or empty
            products_to_normalize = [
                row for row in response.data 
                if not row.get("ai_name")
            ]

            if not products_to_normalize:
                print("No new products to normalize.")
                return

            print(f"Found {len(products_to_normalize)} products to normalize\n")
            print("=" * 60)

            success_count = 0
            fail_count = 0

            for row in products_to_normalize:
                print(f"\nProduct: {row['name']}")
                print(f"  Category: {row.get('category', 'N/A')}")
                print(f"  Description: {(row.get('description') or 'N/A')[:60]}...")
                
                # Generate product type (tries keywords first, then AI)
                ai_product_type = self.ai_product_naming(
                    row["name"], 
                    row.get("description") or "", 
                    row.get("category") or ""
                )

                # Fallback if both keyword and AI fail
                if not ai_product_type:
                    print(f"  ⚠ All classification methods failed, using fallback")
                    # Use first word of category as last resort
                    ai_product_type = (row.get("category") or row["name"]).lower().split()[0]

                print(f"  → Final Type: '{ai_product_type}'")

                # Update Supabase
                try:
                    self.supabase_client.table("products").update({
                        "ai_name": str(ai_product_type).strip().lower(),
                        "ai_name_updated_at": datetime.now(timezone.utc).isoformat()
                    }).eq("id", row["id"]).execute()
                    
                    print(f"  ✓ Updated in database")
                    success_count += 1
                        
                except Exception as update_error:
                    print(f"  ✗ Database update failed: {update_error}")
                    fail_count += 1

            print("\n" + "=" * 60)
            print(f"Normalization complete!")
            print(f"  ✓ Success: {success_count}")
            print(f"  ✗ Failed: {fail_count}")

        except Exception as e:
            print(f"Exception: {e}")
            import traceback
            traceback.print_exc()

    def product_by_business(self, product_query):
        """Returns the number of businesses selling that product"""
        try:
            response = (
                self.supabase_client.table('products')
                .select('business_id')
                .ilike('ai_name', f'%{product_query}%')  # case-insensitive partial match
                .execute()
            )

            if not response.data:
                return 0

            # Count unique business_ids
            businesses = {row['business_id'] for row in response.data}
            return len(businesses)

        except Exception as e:
            print(f"Exception: {e}")
            return 0
        

    def average_product_price(self, product_query):
        """Returns the average price for the product queried using Supabase aggregation."""
        try:
            # Use `.select('price')` and `.maybe_aggregate()` is not directly supported
            # Instead, fetch all prices and filter None
            response = (
                self.supabase_client.table('products')
                .select('price')
                .ilike('ai_name', f'%{product_query}%')
                .execute()
            )

            if not response.data:
                return 0

            # Filter out None prices
            prices = [row['price'] for row in response.data if row['price'] is not None]
            if not prices:
                return 0

            return sum(prices) / len(prices)

        except Exception as e:
            print(f"Exception: {e}")
            return 0



    def product_sales_volume(self, product_query, period=30):
        """Returns the total volume of units sold for that product in the last specified period (days)."""
        today = datetime.now()
        period_start = today - timedelta(days=period)
        print(f"[DEBUG] Checking sales volume for product: '{product_query}'")
        print(f"[DEBUG] Period start: {period_start}, Today: {today}")

        try:
            response = (
                self.supabase_client.table('products')
                .select('id, ai_name, orders(quantity, order_status, order_payment_status, created_at)')
                .ilike('ai_name', f'%{product_query}%')
                .execute()
            )

            print(f"[DEBUG] Raw response: {response}")

            if not response.data:
                print("[DEBUG] No products found matching query.")
                return 0

            total = 0
            for i, product in enumerate(response.data, start=1):
                print(f"[DEBUG] Processing product {i}: {product.get('ai_name')}")
                for j, order in enumerate(product.get('orders', []), start=1):
                    print(f"  [DEBUG] Order {j}: {order}")

                    try:
                        order_date = datetime.fromisoformat(order['created_at'])
                    except Exception as e:
                        print(f"  [DEBUG] Skipping order {j}: Invalid date format ({order.get('created_at')}) - {e}")
                        continue

                    if (
                        order['order_status'] == 'completed'
                        and order['order_payment_status'] == 'completed'
                        and order_date >= period_start
                    ):
                        qty = order.get('quantity', 0) or 0
                        print(f"  [DEBUG] Counting order {j}, quantity={qty}")
                        total += qty
                    else:
                        print(f"  [DEBUG] Skipping order {j}: Not completed/paid or outside period.")

            print(f"[DEBUG] Total sales volume: {total}")
            return total

        except Exception as e:
            print(f"[DEBUG] Exception: {e}")
            return 0

        
    def total_product_revenue(self, product_query, period=30):
        """returns te total revenue generated by that product in the specifed period"""
        today = datetime.now()
        period_start = today - timedelta(days=period)

        try:
            response = (
                self.supabase_client.table('products')
                .select('id, ai_name, orders(total_amount, order_status, order_payment_status, created_at)')
                .ilike('ai_name', f'%{product_query}%')
                .execute()
            )

            if not response.data:
                return 0

            total = 0
            for product in response.data:
                for order in product.get('orders', []):
                    if (
                        order['order_status'] == 'completed' and
                        order['order_payment_status'] == 'completed' and
                        datetime.fromisoformat(order['created_at']) >= period_start
                    ):
                        total += order['total_amount']

            return total

        except Exception as e:
            print(f"Exception: {e}")
            return 0
        
    

    def top_location(self, product_query):
        """
        Returns the location with the highest sales (by quantity ordered)
        for the given product (matching products.ai_name).
        """
        try:
            print(f"[DEBUG] Querying top location for product: {product_query}")

            response = (
                self.supabase_client.table('orders')
                .select('quantity, order_status, order_payment_status, products(ai_name), customers(location)')
                .eq('order_status', 'completed')
                .eq('order_payment_status', 'completed')
                .ilike('products.ai_name', f'%{product_query}%')  # filter by ai_name in products
                .execute()
            )

            print(f"[DEBUG] Raw response: {response}")

            if not response.data:
                print("[DEBUG] No matching orders found.")
                return None

            location_totals: Dict[str, int] = {}

            for i, order in enumerate(response.data, start=1):
                print(f"[DEBUG] Processing order {i}: {order}")

                customer = order.get('customers')
                product = order.get('products')

                if not customer:
                    print(f"[DEBUG] Skipping order {i}: No customer data.")
                    continue
                if not product:
                    print(f"[DEBUG] Skipping order {i}: Product join failed.")
                    continue

                location = customer.get('location')
                if not location:
                    print(f"[DEBUG] Skipping order {i}: No location info.")
                    continue

                qty = order.get('quantity', 0) or 0
                print(f"[DEBUG] Order {i}: location={location}, qty={qty}")

                location_totals[location] = location_totals.get(location, 0) + qty

            print(f"[DEBUG] Aggregated location totals: {location_totals}")

            if not location_totals:
                print("[DEBUG] No quantities aggregated.")
                return None

            top_location = max(location_totals, key=lambda loc: location_totals[loc])
            print(f"[DEBUG] Top location: {top_location}, Qty={location_totals[top_location]}")

            return top_location, location_totals[top_location]

        except Exception as e:
            print(f"[DEBUG] Exception: {e}")
            return None


    def product_sales_growth(self, product_query):
        """Returns the sales growth (%) for a product comparing this month vs last month."""

        try:
            # Current 30 days
            current_revenue = self.total_product_revenue(product_query, period=30)

            # Previous 30 days (days 31–60 ago)
            today = datetime.now()
            sixty_days_ago = today - timedelta(days=60)
            thirty_days_ago = today - timedelta(days=30)

            response = (
                self.supabase_client.table('products')
                .select('id, ai_name, orders(total_amount, order_status, order_payment_status, created_at)')
                .ilike('ai_name', f'%{product_query}%')
                .execute()
            )

            if not response.data:
                return 0

            previous_revenue = 0
            for product in response.data:
                for order in product.get('orders', []):
                    if (
                        order['order_status'] == 'completed' and
                        order['order_payment_status'] == 'completed'
                    ):
                        order_date = datetime.fromisoformat(order['created_at'])
                        if sixty_days_ago <= order_date < thirty_days_ago:
                            previous_revenue += order['total_amount']

            if previous_revenue == 0:
                return None  # avoid division by zero

            growth = ((current_revenue - previous_revenue) / previous_revenue) * 100
            return round(growth, 2)

        except Exception as e:
            print(f"Exception in product_sales_growth: {e}")
            return None

    def product_market_share(self, product_query):
        """Returns the market share (%) of a product in InXource."""

        try:
            response = (
                self.supabase_client.table('products')
                .select('id, ai_name, orders(total_amount, order_status, order_payment_status, created_at)')
                .ilike('ai_name', f'%{product_query}%')
                .execute()
            )

            if not response.data:
                return 0

            product_total = 0
            for product in response.data:
                for order in product.get('orders', []):
                    if (
                        order['order_status'] == 'completed' and
                        order['order_payment_status'] == 'completed'
                    ):
                        product_total += order['total_amount']

            grand_total = self.total_revenue()

            if grand_total == 0:
                return 0  # avoid division by zero

            market_share = (product_total / grand_total) * 100
            return round(market_share, 2)

        except Exception as e:
            print(f"Exception in product_market_share: {e}")
            return 0

    def product_information_summary(self, product_query):
        """returns a dictionary of th product summary of the queried product"""

        product_summary = {}

        product_summary['product_business_number'] = self.product_by_business(product_query)
        product_summary['average_price'] = self.average_product_price(product_query)
        product_summary['product_sales_volume'] = self.product_sales_volume(product_query)
        product_summary['total_product_revenue'] = self.total_product_revenue(product_query)
        product_summary['top_product_location'] = (self.top_location(product_query) or (None, None))[0]
        product_summary['product_sales_growth'] = self.product_sales_growth(product_query)
        product_summary['product_market_share'] = self.product_market_share(product_query)

        return product_summary
 
      
"""
test = Products()
print(test.product_information_summary('cakes'))
"""

