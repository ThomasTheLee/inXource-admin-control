from flask import Flask, request, render_template, redirect, url_for, session, make_response, jsonify, flash
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from typing import Dict, Any, cast
import logging
# custom imports
from users import Users
from businesses import Businesses
from products import Products
from wallet import Wallet
from industries import Industry
import os
from dotenv import load_dotenv
from analysis import AnalAI
import json
from datetime import datetime, timedelta
from settings import SettingsManager
from subscriptions import Subscriptions
from auth import Auth
from activites import Activites
from referrals import Referrals

from clients import Clients

Client_manager = Clients()

load_dotenv() 

# Create the Flask app
app = Flask(__name__)


app.secret_key = os.getenv('APP_SECRET_KEY') 
test_key = os.getenv('OPEN_AI_TEST_KEY')

# tools
users_manager = Users()
business_manager = Businesses()
products_manager = Products()
wallet_manager = Wallet()
industry_manager = Industry()
ai_manager = AnalAI()
settings_manager = SettingsManager()
Subscription_manager = Subscriptions()
auth_manager = Auth()
activity_manager = Activites()
referrals_manager = Referrals()

# Configure logger with environment-based control
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Capture all levels

# Create formatters
detailed_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
simple_formatter = logging.Formatter('%(levelname)s - %(message)s')

# File handler - only WARNING and above (production-ready)
file_handler = logging.FileHandler('app.log', mode='w')
file_handler.setLevel(logging.WARNING)  # Only important logs to file
file_handler.setFormatter(detailed_formatter)

# Console handler - respects LOG_LEVEL environment variable
console_handler = logging.StreamHandler()
console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
console_handler.setFormatter(simple_formatter)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.info("Application started successfully")
logger.info(f"Flask app initialized with secret key: {'Set' if app.secret_key else 'Not Set'}")


#routes
@app.route('/', methods=['POST', 'GET'])
def auth():
    # Debug: print current session at start
    print("Session at start:", dict(session))

    # Check if user is already logged in via session
    if session.get('logged_in'):
        print("User already logged in, staff_id:", session.get('staff_id'))
        return redirect(url_for('index'))
    
    # Check if "Remember Me" cookies exist and auto-login
    if request.cookies.get('user_name') and not session.get('logged_in'):
        # Restore session from cookies
        session['user_name'] = request.cookies.get('user_name')
        session['email'] = request.cookies.get('email')
        session['role'] = request.cookies.get('role')
        session['staff_id'] = request.cookies.get('staff_id')
        session['logged_in'] = True

        # Debug: print restored session
        print("Restored session from cookies:", dict(session))
        return redirect(url_for('index'))
    
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        remember = request.form.get('remember', '').strip()


        # Debug: log login attempt
        print(f"Login attempt for username: {username}")

        user_info = auth_manager.login(user=username, password=password)

        # Debug: print returned user info
        print("Login returned user_info:", user_info)

        if user_info.get('logged'):
            session['user_name'] = user_info['user_name']
            session['email'] = user_info['email']
            session['role'] = user_info['role']
            session['staff_id'] = user_info['staff_id']
            session['logged_in'] = user_info['logged']

            # Debug: print session after login
            print("Session after login:", dict(session))

            response = make_response(redirect(url_for('index')))

            if remember == 'on':
                expires = datetime.now() + timedelta(days=30)
                response.set_cookie('user_name', str(user_info['user_name']), expires=expires, httponly=True, secure=True, samesite='Lax')
                response.set_cookie('email', str(user_info['email']), expires=expires, httponly=True, secure=True, samesite='Lax')
                response.set_cookie('role', str(user_info['role']), expires=expires, httponly=True, secure=True, samesite='Lax')
                response.set_cookie('staff_id', str(user_info['staff_id']), expires=expires, httponly=True, secure=True, samesite='Lax')

                # Debug: print cookies being set
                print("Cookies set for 'remember me':", {
                    'user_name': user_info['user_name'],
                    'email': user_info['email'],
                    'role': user_info['role'],
                    'staff_id': user_info['staff_id']
                })

            return response
        else:
            flash('Invalid credentials. Please try again.', 'error')
            return render_template('auth.html', error="Invalid credentials")
    
    # Debug: print session before rendering login page
    print("Session before rendering login page:", dict(session))
    return render_template('auth.html')

@app.route('/forgot_password', methods=['POST', 'GET'])
def forgot_password():
    # get email from form
    if request.method == "POST":
        email = request.form.get('email')

        try:
            response = auth_manager.forgot_password(email=email)

            if response:
                logger.info(f"Password sent to email: {email}")
                flash("Password has been sent to your email!", "success")
            else:
                flash("Failed to send password. Please check the email and try again.", "error")
        except Exception as e:
            logger.error(f"Exception while sending password to email {email}: {e}")
            flash("An error occurred while sending password to your email.", "error")
        
        # Redirect after flashing the message
        return redirect(url_for('forgot_password'))
    
    return render_template('auth.html')

@app.route("/index")
def index(): 
    """loads the overview dashboard"""
    logger.info("Index page accessed")
    try:
        total_users = users_manager.total_users()
        logger.debug(f"Total users: {total_users}")
        
        total_businesses = business_manager.total_businesses()
        logger.debug(f"Total businesses: {total_businesses}")
        
        total_products = products_manager.total_products()
        logger.debug(f"Total products: {total_products}")
        
        total_pending_withdraws = wallet_manager.total_withdrawal_requests()
        logger.debug(f"Total pending withdrawals: {total_pending_withdraws}")
        
        revenues = Subscription_manager.total_revenue()
        logger.debug(f"Total revenue: {revenues}")
        
        # Get revenue period data
        logger.info("Fetching revenue period data")
        revenue_data = Subscription_manager.revenue_period_data()
        
        # Convert DataFrames to JSON-serializable format
        revenue_json = {}
        for period, df in revenue_data.items():
            if not df.empty:
                # Convert datetime to string format
                df_copy = df.copy()
                df_copy['created_at'] = df_copy['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
                revenue_json[period] = df_copy.to_dict('records')
                logger.debug(f"Revenue data for {period}: {len(df_copy)} records")
            else:
                revenue_json[period] = []
                logger.debug(f"No revenue data for {period}")

        all_activities = activity_manager.get_recent_activities() or []
        index = session.get('activity_index', 0)

        # Make sure index is within range
        if index >= len(all_activities):
            index = 0

        # Show only 4 activities
        recent_activities = all_activities[index:index+4]

        # Update index for next refresh
        new_index = index + 4
        if new_index >= len(all_activities):
            new_index = 0  # loop back to start

        session['activity_index'] = new_index


        logger.info("Index page rendered successfully")
        return render_template('index.html',
                               total_users=total_users,
                               total_businesses=total_businesses,
                               total_products=total_products,
                               total_pending_withdraws=total_pending_withdraws,
                               revenues=revenues,
                               revenue_data=revenue_json,
                               recent_activities = recent_activities
                               )
    except Exception as e:
        logger.error(f"Error loading index page: {str(e)}", exc_info=True)
        raise


@app.route('/wallet')
def wallet():
    """loads the wallet page"""
    # Check if user is logged in
    if not session.get('logged_in'):
        flash("Please login to access this page.", "warning")
        return redirect(url_for('auth'))
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin', 'finance']:
        flash("You don't have permission to access the wallet page.", "danger")
        logger.warning(f"Unauthorized access attempt to wallet page by user with role: {user_role}")
        return redirect(url_for('index'))
    
    logger.info("Wallet page accessed")
    try:
        total_pending_withdraws = wallet_manager.total_withdrawal_requests()
        logger.debug(f"Total pending withdrawals: {total_pending_withdraws}")
        
        toal_inhouse_money = wallet_manager.total_inhouse_money()
        logger.debug(f"Total in-house money: {toal_inhouse_money}")

        # card variables
        withdraw_requests = wallet_manager.load_pending_withdrawals()
        logger.debug(f"Loaded {len(withdraw_requests) if withdraw_requests else 0} withdrawal requests")

        logger.info("Wallet page rendered successfully")
        return render_template('wallet.html',
                               total_pending_withdraws = total_pending_withdraws,
                               toal_inhouse_money = toal_inhouse_money,
                               withdraw_requests = withdraw_requests  
                               )
    except Exception as e:
        logger.error(f"Error loading wallet page: {str(e)}", exc_info=True)
        raise

@app.route('/approve_withdrawal_with_proof', methods=['POST'])
def approve_withdrawal_with_proof():
    """Approve a withdrawal request with proof of payout file upload"""
    # Check if user is logged in
    if not session.get('logged_in'):
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin', 'finance']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})
    
    logger.info("Approve withdrawal with proof endpoint called")
    
    try:
        # Get form data
        withdrawal_id = request.form.get('withdrawal_id')
        business_id = request.form.get('business_id')
        amount = float(request.form.get('amount', 0))
        notes = request.form.get('notes', '')  # Optional notes
        
        logger.info(f"Processing withdrawal approval - ID: {withdrawal_id}, Business: {business_id}, Amount: {amount}")
        
        # Get uploaded file
        if 'proof_file' not in request.files:
            logger.warning("No file uploaded in request")
            return jsonify({"success": False, "message": "No file uploaded"})
        
        file_object = request.files['proof_file']
        if file_object.filename == '':
            logger.warning("Empty filename in uploaded file")
            return jsonify({"success": False, "message": "No file selected"})
        
        logger.info(f"File uploaded: {file_object.filename}")
        
        # Upload file to Supabase bucket and get URL
        logger.info("Uploading proof file to Supabase")
        proof_url = wallet_manager.upload_payout_proof(file_object, withdrawal_id)
        if not proof_url:
            logger.error("Failed to upload proof file to Supabase")
            return jsonify({"success": False, "message": "Failed to upload proof file"})
        
        logger.info(f"Proof file uploaded successfully: {proof_url}")
        
        # Update the proof_of_payment field in withdrawals table
        logger.info("Updating proof of payment in database")
        proof_update_success = wallet_manager.update_proof_of_payment(withdrawal_id, proof_url)
        if not proof_update_success:
            logger.error("Failed to update proof of payment in database")
            return jsonify({"success": False, "message": "Failed to update proof of payment in database"})
        
        logger.info("Proof of payment updated successfully")
        
        # Approve the withdrawal (same as before)
        logger.info("Approving withdrawal")
        approval_success = wallet_manager.aprove_withdrawal(withdrawal_id)
        if not approval_success:
            logger.error("Failed to approve withdrawal")
            return jsonify({"success": False, "message": "Failed to approve withdrawal"})
        
        logger.info("Withdrawal approved successfully")
        
        # Reduce the business wallet balance (same as before)
        logger.info(f"Reducing wallet balance for business {business_id} by {amount}")
        wallet_balance_result = wallet_manager.reduce_wallet_balance(
            business_id=business_id,
            withdraw_amount=amount
        )
        if not wallet_balance_result:
            logger.error("Failed to reduce wallet balance")
            return jsonify({"success": False, "message": "Failed to reduce wallet balance"})
        
        logger.info(f"Wallet balance reduced successfully for business {business_id}")
        logger.info(f"Withdrawal {withdrawal_id} processed completely")
        
        # Success - return JSON response for JavaScript
        return jsonify({
            "success": True, 
            "message": f"Successfully approved withdrawal {withdrawal_id} with proof of payout"
        })
        
    except Exception as e:
        logger.error(f"Exception in approve_withdrawal_with_proof: {str(e)}", exc_info=True)
        print(f"Exception in approve_withdrawal_with_proof: {e}")
        return jsonify({"success": False, "message": "An error occurred while processing the withdrawal"})


@app.route('/reject_withdrawal', methods=['POST'])
def reject_withdrawal():
    """Reject a withdrawal request by its ID"""
    # Check if user is logged in
    if not session.get('logged_in'):
        flash("Please login to perform this action.", "warning")
        return redirect(url_for('auth'))
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin', 'finance']:
        logger.warning(f"Unauthorized rejection attempt by user with role: {user_role}")
        flash("You don't have permission to reject withdrawals.", "danger")
        return redirect(url_for('index'))
    
    withdrawal_id = request.form.get('withdrawal_id')
    logger.info(f"Reject withdrawal endpoint called for ID: {withdrawal_id}")

    success = wallet_manager.reject_withdrawal(withdrawal_id)

    if success:
        logger.info(f"Successfully rejected withdrawal {withdrawal_id}")
        flash(f"Successfully rejected withdrawal {withdrawal_id}.", "success")
    else:
        logger.warning(f"Failed to reject withdrawal {withdrawal_id}")
        flash(f"Failed to reject withdrawal {withdrawal_id}.", "danger")

    return redirect(url_for('wallet'))

@app.route("/users")
def users():
    """Loads the users management page"""
    logger.info("Users page accessed")
    
    try:
        # Existing variables
        total_users = users_manager.total_users()
        logger.debug(f"Total users: {total_users}")
        
        total_user_growth_rate = users_manager.total_user_growth_rate()
        logger.debug(f"User growth rate: {total_user_growth_rate}")
        
        total_new_registrations = users_manager.total_new_registrations()
        logger.debug(f"New registrations: {total_new_registrations}")
        
        new_registrations_rate = users_manager.new_registrations_rate()
        logger.debug(f"New registrations rate: {new_registrations_rate}")
        
        total_active_users = users_manager.total_active_users()
        logger.debug(f"Active users: {total_active_users}")
        
        active_users_growth_rate = users_manager.active_users_growh_rate()
        logger.debug(f"Active users growth rate: {active_users_growth_rate}")
        
        users_per_location = users_manager.users_per_location()
        logger.debug(f"Users per location calculated: {len(users_per_location) if users_per_location else 0} locations")
        
        # Get monthly users chart data
        logger.info("Fetching monthly user trend data")
        monthly_user_trend_df = users_manager.monthly_user_trend()
        
        # Convert DataFrame to JSON-serializable format
        monthly_users_chart_data = {
            'labels': [],
            'data': []
        }
        
        if not monthly_user_trend_df.empty:
            # Convert Period to string for JSON serialization
            monthly_users_chart_data['labels'] = [str(month) for month in monthly_user_trend_df['month'].tolist()]
            monthly_users_chart_data['data'] = monthly_user_trend_df['user_count'].tolist()
            logger.debug(f"Monthly user trend: {len(monthly_users_chart_data['labels'])} data points")

        # Get chart data for user activity
        logger.info("Fetching monthly activity trend data")
        monthly_activity_df = users_manager.monthly_activity_trend()
        activity_chart_data = {
            'labels': [],
            'data': []
        }
        if not monthly_activity_df.empty:
            activity_chart_data['labels'] = [str(month) for month in monthly_activity_df['month'].tolist()]
            activity_chart_data['data'] = monthly_activity_df['active_user_count'].tolist()
            logger.debug(f"Monthly activity trend: {len(activity_chart_data['labels'])} data points")
        
        logger.info("Users page rendered successfully")
        return render_template(
            "users.html",
            total_users=total_users,
            total_user_growth_rate=total_user_growth_rate,
            total_new_registrations=total_new_registrations,
            new_registrations_rate=new_registrations_rate,
            total_active_users=total_active_users,
            active_users_growth_rate=active_users_growth_rate,
            users_per_location=users_per_location,
            monthly_trend_data=monthly_users_chart_data,
            monthly_activity_data=activity_chart_data
        )
    except Exception as e:
        logger.error(f"Error loading users page: {str(e)}", exc_info=True)
        raise

@app.route('/search_users', methods=['POST'])
def search_users():
    """Search users based on a query"""
    query = request.form.get('query', '').strip()
    logger.info(f"User search requested with query: '{query}'")
    
    if not query:
        logger.warning("Empty search query provided")
        return jsonify({
            'success': False,
            'message': 'Please enter a search query.',
            'users': []
        }), 400
    
    try:
        # Retrieve users matching the query
        logger.info(f"Searching for users matching: {query}")
        users = users_manager.retrieve_users_information(query)
        
        if not users:
            logger.info(f"No users found for query: {query}")
            return jsonify({
                'success': True,
                'message': 'No users found matching the query.',
                'users': []
            }), 200
        
        logger.info(f"Found {len(users)} user(s) matching query: {query}")
        return jsonify({
            'success': True,
            'message': f'Found {len(users)} user(s) matching the query.',
            'users': users
        }), 200
        
    except Exception as e:
        logger.error(f"Error in search_users route: {str(e)}", exc_info=True)
        print(f"Error in search_users route: {e}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while searching for users.',
            'users': []
        }), 500


@app.route("/businesses")
def businesses():
    """loads the businesses management page"""
    logger.info("Businesses page accessed")
    
    try:
        total_businesses = business_manager.total_businesses()
        logger.debug(f"Total businesses: {total_businesses}")
        
        business_growth_rate = business_manager.total_businesses_growth_rate()
        logger.debug(f"Business growth rate: {business_growth_rate}")
        
        new_businesses = business_manager.new_businesses_registrations()
        logger.debug(f"New businesses: {new_businesses}")
        
        new_businesses_rate = business_manager.new_businesses_registrations_rate()
        logger.debug(f"New businesses rate: {new_businesses_rate}")
        
        total_active_businesses = business_manager.total_active_businesses()
        logger.debug(f"Active businesses: {total_active_businesses}")
        
        total_active_businesses_growth_rate = business_manager.total_active_businesses_growth_rate()
        logger.debug(f"Active businesses growth rate: {total_active_businesses_growth_rate}")
        
        # Get category data
        logger.info("Fetching top performing categories")
        top_categories = business_manager.top_performing_categories()
        
        # Calculate percentages for progress bars
        if top_categories:
            max_total = max(category['total'] for category in top_categories)
            for category in top_categories:
                category['width'] = int((category['total'] / max_total) * 100) if max_total > 0 else 0
            logger.debug(f"Processed {len(top_categories)} categories")
        
        # Get business activity data
        logger.info("Loading business activity data")
        business_activity = business_manager.load_business_activity()
        logger.debug(f"Business activity records: {len(business_activity) if business_activity else 0}")
        
        # Get the monthly business trend
        logger.info("Fetching monthly business trend")
        monthly_business_trend_df = business_manager.monthly_business_trend()
        
        # Convert dataframe to json
        monthly_business_chart_data = {
            'labels': [],
            'data': []
        }
        
        if not monthly_business_trend_df.empty:
            # Convert Period to string for JSON serialization
            monthly_business_chart_data['labels'] = [str(month) for month in monthly_business_trend_df['month'].tolist()]
            monthly_business_chart_data['data'] = monthly_business_trend_df['business_count'].tolist()
            logger.debug(f"Monthly business trend: {len(monthly_business_chart_data['labels'])} data points")

        # get the top performing industries
        logger.info("Fetching top performing industries")
        top_performing_industries = business_manager.get_top_performing_industries()
        
        # Format the industries data for the chart
        industries_chart_data = {
            'labels': [],
            'data': []
        }
        
        if top_performing_industries:
            for industry, total in top_performing_industries:
                industries_chart_data['labels'].append(industry.title())
                industries_chart_data['data'].append(float(total))
            logger.debug(f"Top performing industries: {len(industries_chart_data['labels'])} industries")
        
        logger.info("Businesses page rendered successfully")
        return render_template('bussinesses.html',
                               total_businesses=total_businesses,
                               business_growth_rate=business_growth_rate,
                               new_businesses=new_businesses,
                               new_businesses_rate=new_businesses_rate,
                               total_active_businesses=total_active_businesses,
                               total_active_businesses_growth_rate=total_active_businesses_growth_rate,
                               top_categories=top_categories,
                               business_activity=business_activity,
                               monthly_business_trend_data=monthly_business_chart_data,
                               industries_chart_data=industries_chart_data
                               )
    except Exception as e:
        logger.error(f"Error loading businesses page: {str(e)}", exc_info=True)
        raise




@app.route('/search_businesses', methods=['POST'])
def search_businesses():
    """Search businesses based on a query"""
    query = request.form.get('query', '').strip()
    logger.info(f"Business search requested with query: '{query}'")
    
    if not query:
        logger.warning("Empty search query provided")
        return jsonify({
            'success': False,
            'message': 'Please enter a search query.',
            'businesses': []
        }), 400
    
    try:
        # Retrieve businesses matching the query
        logger.info(f"Searching for businesses matching: {query}")
        businesses = business_manager.retrieve_business_information(query)
        
        if not businesses:
            logger.info(f"No businesses found for query: {query}")
            return jsonify({
                'success': True,
                'message': 'No businesses found matching the query.',
                'businesses': []
            }), 200
        
        logger.info(f"Found {len(businesses)} business(es) matching query: {query}")
        return jsonify({
            'success': True,
            'message': f'Found {len(businesses)} business(es) matching the query.',
            'businesses': businesses
        }), 200
        
    except Exception as e:
        logger.error(f"Error in search_businesses route: {str(e)}", exc_info=True)
        print(f"Error in search_businesses route: {e}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while searching for businesses.',
            'businesses': []
        }), 500


@app.route('/products')
def products():

    # Check if user is logged in
    if not session.get('logged_in'):
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})
    
    logger.info("Products page accessed")
    
    try:
        total_products = products_manager.total_products()
        logger.debug(f"Total products: {total_products}")
        
        total_products_gr = products_manager.total_products_growth()
        logger.debug(f"Products growth rate: {total_products_gr}")
        
        low_stock_count = products_manager.low_stock_count()
        logger.debug(f"Low stock count: {low_stock_count}")
        
        low_stock_percent = products_manager.low_stock_percent()
        logger.debug(f"Low stock percentage: {low_stock_percent}")
        
        total_revenue = products_manager.total_revenue()
        logger.debug(f"Total revenue: {total_revenue}")
        
        total_revenue_growth = products_manager.total_revenue_growth()
        logger.debug(f"Revenue growth: {total_revenue_growth}")
        
        logger.info(f"Fetching product rankings by: {settings_manager.product_performance_by}")
        ranking_products = products_manager.product_ranking(settings_manager.product_performance_by)
        logger.debug(f"Retrieved {len(ranking_products) if ranking_products else 0} ranked products")

        logger.info("Products page rendered successfully")
        return render_template(
            'products.html',
            total_products = total_products,
            total_products_gr = total_products_gr,
            low_stock_count = low_stock_count,
            low_stock_percent = low_stock_percent,
            total_revenue = total_revenue,
            total_revenue_growth = total_revenue_growth,
            ranking_products = ranking_products
        )
    except Exception as e:
        logger.error(f"Error loading products page: {str(e)}", exc_info=True)
        raise



@app.route('/search_product', methods=['GET', 'POST'])
def search_product():
    """Search for a product and return its summary information"""

    # Check if user is logged in
    if not session.get('logged_in'):
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})

    try:
        # Get the queried product
        if request.method == 'POST':
            product_query = request.form.get('query', '').strip()
        else:
            product_query = request.args.get('query', '').strip()
        
        logger.info(f"Product search requested with query: '{product_query}' via {request.method}")
        
        if not product_query:
            logger.warning("Empty product search query provided")
            return jsonify({
                "error": "Please provide a product name to search"
            }), 400
        
        # Get product summary using your existing method
        logger.info(f"Fetching product information summary for: {product_query}")
        result = products_manager.product_information_summary(product_query)
        
        logger.info(f"Product search completed for: {product_query}")
        # Return the summary data
        return jsonify({
            "success": True,
            "product_name": product_query,
            "summary": result
        }), 200
        
    except Exception as e:
        logger.error(f"Search error in search_product: {str(e)}", exc_info=True)
        print(f"Search error: {e}")
        return jsonify({
            "error": "An error occurred during search",
            "details": str(e)
        }), 500



@app.route('/industry_analysis')
def industry_analysis():

    # Check if user is logged in
    if not session.get('logged_in'):
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})

    logger.info("Industry analysis page accessed")
    
    try:
        total_industries = industry_manager.total_industries()
        logger.debug(f"Total industries: {len(total_industries)}")
        
        new_industries = industry_manager.check_new_industries()
        logger.debug(f"New industries: {len(new_industries)}")
        
        industries_total = industry_manager.get_industries_total()
        logger.debug(f"Industries total value: {industries_total}")
        
        industry_revenue_growth_rate = industry_manager.total_industry_revenue_rate()
        logger.debug(f"Industry revenue growth rate: {industry_revenue_growth_rate}")
        
        logger.info("Calculating top performing industry")
        top_performing_industry = max(
            business_manager.get_top_performing_industries(),
            key=lambda x: x[1]
        )[0].capitalize()
        logger.info(f"Top performing industry: {top_performing_industry}")
        
        industry_market_share = industry_manager.industry_market_share()
        logger.debug(f"Industry market share calculated: {len(industry_market_share) if industry_market_share else 0} industries")
        
        industry_average_growth_rate = industry_manager.industry_average_growth_rate()
        logger.debug(f"Industry average growth rate: {industry_average_growth_rate}")
        
        yearly_industry_growth_rate = industry_manager.industry_average_growth_rate(days=365)
        logger.debug(f"Yearly industry growth rate: {yearly_industry_growth_rate}")
        
        logger.info("Industry analysis page rendered successfully")
        return render_template('industries.html',
                              total_industries=len(total_industries),
                              new_industries=len(new_industries),
                              industries_total=industries_total,
                              industry_revenue_growth_rate=industry_revenue_growth_rate,
                              top_performing_industry=top_performing_industry,
                              industry_market_share=industry_market_share,
                              industry_average_growth_rate=industry_average_growth_rate,
                              yearly_industry_growth_rate=yearly_industry_growth_rate
                              )
    except Exception as e:
        logger.error(f"Error loading industry analysis page: {str(e)}", exc_info=True)
        raise


@app.route('/search_industry', methods=['POST'])
def search_industry():

    # Check if user is logged in
    if not session.get('logged_in'):
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})
    
    # Get the industry from the request - handle both JSON and form data
    industry = None
    
    logger.info("Industry search endpoint called")
    
    # Try to get from JSON first
    if request.is_json and request.json:
        industry = request.json.get('industry', '').strip()
        logger.debug("Industry extracted from JSON request")
    # Fallback to form data
    elif request.form:
        industry = request.form.get('industry', '').strip()
        logger.debug("Industry extracted from form data")
    # Fallback to raw data if it's a simple string
    else:
        try:
            data = request.get_data(as_text=True)
            if data:
                industry = data.strip()
                logger.debug("Industry extracted from raw data")
        except:
            pass
    
    logger.info(f"Industry search requested for: '{industry}'")
    
    # Validate input
    if not industry:
        logger.warning("Empty industry search query provided")
        return jsonify({'error': 'Please provide an industry'}), 400
    
    try:
        # Get the revenue trend data
        logger.info(f"Fetching revenue trend data for industry: {industry}")
        revenue_trend_data_df = industry_manager.industry_revenue_trend(industry)
        
        # Get the customer growth trend data
        logger.info(f"Fetching customer growth trend for industry: {industry}")
        customer_trend_data_df = industry_manager.customer_growth_trend(industry)
        
        # Get the average order trend data
        logger.info(f"Fetching average order trend for industry: {industry}")
        average_order_trend_data_df = industry_manager.industry_average_order_trend(industry)
        
        # Get the seasonal performance trend data
        logger.info(f"Fetching seasonal performance trend for industry: {industry}")
        seasonal_performance_trend_data_df = industry_manager.industry_seasonal_performance_trend(industry)
        
        # Check if all dataframes have data
        if revenue_trend_data_df.empty and customer_trend_data_df.empty and average_order_trend_data_df.empty and seasonal_performance_trend_data_df.empty:
            logger.warning(f"No data found for industry: {industry}")
            return jsonify({
                'error': 'No data found for this industry',
                'industry': industry
            }), 404
        
        # Convert revenue trend data to json
        monthly_industry_revenue_chart_data = {
            'labels': [],
            'data': [],
            'industry': industry.capitalize()
        }
        
        if not revenue_trend_data_df.empty:
            # Convert Period to string for JSON serialization
            monthly_industry_revenue_chart_data['labels'] = [str(month) for month in revenue_trend_data_df['month'].tolist()]
            # Use the correct column name 'amount' from your DataFrame
            monthly_industry_revenue_chart_data['data'] = revenue_trend_data_df['amount'].tolist()
            logger.debug(f"Revenue trend data: {len(monthly_industry_revenue_chart_data['labels'])} data points")
        
        # Convert customer trend data to json
        monthly_industry_customer_chart_data = {
            'labels': [],
            'data': []
        }
        
        if not customer_trend_data_df.empty:
            # Convert Period to string for JSON serialization
            monthly_industry_customer_chart_data['labels'] = [str(month) for month in customer_trend_data_df['month'].tolist()]
            # Use the correct column name 'customers' from your DataFrame
            monthly_industry_customer_chart_data['data'] = customer_trend_data_df['customers'].tolist()
            logger.debug(f"Customer trend data: {len(monthly_industry_customer_chart_data['labels'])} data points")
        
        # Convert average order trend data to json
        monthly_industry_average_order_chart_data = {
            'labels': [],
            'data': []
        }
        
        if not average_order_trend_data_df.empty:
            # Convert Period to string for JSON serialization
            monthly_industry_average_order_chart_data['labels'] = [str(month) for month in average_order_trend_data_df['month'].tolist()]
            # Use the correct column name 'average_order_size' from your DataFrame
            monthly_industry_average_order_chart_data['data'] = average_order_trend_data_df['average_order_size'].tolist()
            logger.debug(f"Average order trend data: {len(monthly_industry_average_order_chart_data['labels'])} data points")
        
        # Convert seasonal performance trend data to json
        seasonal_performance_chart_data = {
            'labels': [],
            'data': []
        }
        
        if not seasonal_performance_trend_data_df.empty:
            # For seasonal data, the column names are 'season' and 'total_sales'
            seasonal_performance_chart_data['labels'] = seasonal_performance_trend_data_df['season'].tolist()
            seasonal_performance_chart_data['data'] = seasonal_performance_trend_data_df['total_sales'].tolist()
            logger.debug(f"Seasonal performance data: {len(seasonal_performance_chart_data['labels'])} data points")
        
        # Combine all datasets in the response
        response_data = {
            'industry': industry.capitalize(),
            'revenue_trend': monthly_industry_revenue_chart_data,
            'customer_trend': monthly_industry_customer_chart_data,
            'average_order_trend': monthly_industry_average_order_chart_data,
            'seasonal_performance': seasonal_performance_chart_data
        }
        
        logger.info(f"Industry search completed successfully for: {industry}")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error in search_industry for '{industry}': {str(e)}", exc_info=True)
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500
    


@app.route('/analysis', methods=['POST', 'GET'])
def analysis():

    # Check if user is logged in
    if not session.get('logged_in'):
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})
    
    logger.info("Analysis page accessed")
    try:
        # Get the most recent weekly and monthly analytics data from database
        logger.info("Fetching latest weekly and monthly insights from database")
        latest_weekly_record = ai_manager.grab_weekly_insights()
        latest_monthly_record = ai_manager.grab_monthly_insights()
        
        # Initialize variables for weekly
        weekly_data = None
        weekly_insights_count = 0
        weekly_tables_analyzed = 0
        weekly_last_updated = 'Never'
        
        # Initialize variables for monthly
        monthly_data = None
        monthly_insights_count = 0
        monthly_tables_analyzed = 0
        monthly_last_updated = 'Never'
        
        # Process weekly data
        if latest_weekly_record:
            logger.info("Latest weekly insight record found")
            try:
                weekly_data = json.loads(latest_weekly_record['insight'])
                weekly_insights_count = len(weekly_data) if weekly_data else 0
                weekly_tables_analyzed = len([k for k, v in weekly_data.items() if v]) if weekly_data else 0
                
                if 'created_at' in latest_weekly_record:
                    weekly_last_updated = datetime.fromisoformat(latest_weekly_record['created_at']).date().isoformat()
                else:
                    weekly_last_updated = 'Recently'
                    
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing weekly insights JSON: {str(e)}", exc_info=True)
                weekly_data = None
        else:
            logger.info("No previous weekly insights found in database")
        
        # Process monthly data
        if latest_monthly_record:
            logger.info("Latest monthly insight record found")
            try:
                monthly_data = json.loads(latest_monthly_record['insight'])
                monthly_insights_count = len(monthly_data) if monthly_data else 0
                monthly_tables_analyzed = len([k for k, v in monthly_data.items() if v]) if monthly_data else 0
                
                if 'created_at' in latest_monthly_record:
                    monthly_last_updated = datetime.fromisoformat(latest_monthly_record['created_at']).date().isoformat()
                else:
                    monthly_last_updated = 'Recently'
                    
            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Error parsing monthly insights JSON: {str(e)}", exc_info=True)
                monthly_data = None
        else:
            logger.info("No previous monthly insights found in database")
        
        logger.info("Analysis page rendered successfully")
        return render_template('analysis.html', 
                             weekly_data=weekly_data,
                             weekly_insights_count=weekly_insights_count,
                             weekly_tables_analyzed=weekly_tables_analyzed,
                             weekly_last_updated=weekly_last_updated,
                             monthly_data=monthly_data,
                             monthly_insights_count=monthly_insights_count,
                             monthly_tables_analyzed=monthly_tables_analyzed,
                             monthly_last_updated=monthly_last_updated)
    
    except Exception as e:
        logger.error(f"Error in analysis route: {str(e)}", exc_info=True)
        return render_template('analysis.html', 
                             weekly_data=None,
                             weekly_insights_count=0,
                             weekly_tables_analyzed=0,
                             weekly_last_updated='Error',
                             monthly_data=None,
                             monthly_insights_count=0,
                             monthly_tables_analyzed=0,
                             monthly_last_updated='Error')
    
    
@app.route('/generate-insights', methods=['POST'])
def generate_insights():

    # Check if user is logged in
    if not session.get('logged_in'):
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})
    
    logger.info("Generate insights endpoint called")
    try:
        # Get the period from request (default to weekly)
        period = 'weekly'
        if request.is_json and request.json:
            period = request.json.get('period', 'weekly')
        
        if period == 'weekly':
            return generate_weekly_insights()
        elif period == 'monthly':
            return generate_monthly_insights()
        else:
            return jsonify({
                'success': False,
                'message': 'Invalid period. Use "weekly" or "monthly".',
                'generated': False
            })
    
    except Exception as e:
        logger.error(f"Error in generate_insights: {str(e)}", exc_info=True)
        return jsonify({
            'success': False, 
            'message': f'Error: {str(e)}',
            'generated': False
        })
    

def generate_weekly_insights():
    """Generate weekly insights if 7+ days have passed"""
    
    # Get the most recent weekly insight from database
    logger.info("Checking for existing weekly insights")
    latest_record = ai_manager.grab_weekly_insights()
    
    should_generate = True
    days_difference = 0
    
    if latest_record and 'created_at' in latest_record:
        created_at_str = latest_record['created_at']
        
        try:
            if 'T' in created_at_str:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            else:
                created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
            
            now = datetime.utcnow()
            if created_at.tzinfo is None:
                now = now.replace(tzinfo=None)
            
            days_difference = (now - created_at).days
            
            logger.info(f"Last weekly insight was generated {days_difference} days ago")
            should_generate = days_difference >= 7
            
        except ValueError as e:
            logger.warning(f"Could not parse timestamp: {created_at_str}, error: {str(e)}")
            should_generate = True
    else:
        logger.info("No previous weekly insights found or no timestamp available")
        should_generate = True
    
    if should_generate:
        logger.info("Generating new weekly insights (7+ days have passed or no previous insights)")
        ai_manager.generate_weekly_insights()
        data = ai_manager.weekly_insights
        logger.info("Storing weekly report in database")
        ai_manager.store_weekly_report(data)
        logger.info("New weekly insights generated and stored successfully")
        
        return jsonify({
            'success': True, 
            'message': 'New weekly insights generated successfully',
            'generated': True
        })
    else:
        logger.info(f"Weekly insights are still valid. {7 - days_difference} days remaining")
        return jsonify({
            'success': True, 
            'message': 'Recent weekly insights are still valid. New insights will be generated after 7 days.',
            'generated': False,
            'days_remaining': 7 - days_difference
        })


def generate_monthly_insights():
    """Generate monthly insights if 30 days have passed since the 1st of the month"""
    
    # Get the most recent monthly insight from database
    logger.info("Checking for existing monthly insights")
    latest_record = ai_manager.grab_monthly_insights()
    
    should_generate = True
    days_difference = 0
    
    if latest_record and 'created_at' in latest_record:
        created_at_str = latest_record['created_at']
        
        try:
            if 'T' in created_at_str:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            else:
                created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
            
            now = datetime.utcnow()
            if created_at.tzinfo is None:
                now = now.replace(tzinfo=None)
            
            # Calculate days difference
            days_difference = (now - created_at).days
            
            # Check if we should generate based on 30-day cycle from creation date
            should_generate = days_difference >= 30
            
            logger.info(f"Last monthly insight was generated {days_difference} days ago")
            
        except ValueError as e:
            logger.warning(f"Could not parse timestamp: {created_at_str}, error: {str(e)}")
            should_generate = True
    else:
        logger.info("No previous monthly insights found or no timestamp available")
        should_generate = True
    
    if should_generate:
        logger.info("Generating new monthly insights (30+ days have passed or no previous insights)")
        ai_manager.generate_monthly_insights()
        data = ai_manager.monthly_insights
        logger.info("Storing monthly report in database")
        ai_manager.store_monthly_report(data)
        logger.info("New monthly insights generated and stored successfully")
        
        return jsonify({
            'success': True, 
            'message': 'New monthly insights generated successfully',
            'generated': True
        })
    else:
        logger.info(f"Monthly insights are still valid. {30 - days_difference} days remaining")
        return jsonify({
            'success': True, 
            'message': 'Recent monthly insights are still valid. New insights will be generated after 30 days.',
            'generated': False,
            'days_remaining': 30 - days_difference
        })
    

@app.route('/custom_analysis', methods=['POST'])
def custom_analysis():
    import time
    start_time = time.time()
    
    print("\n" + "="*50)
    print("CUSTOM ANALYSIS STARTED")
    print("="*50)

    # Check if user is logged in
    if not session.get('logged_in'):
        print("ERROR: User not logged in")
        return jsonify({"success": False, "message": "Please login to perform this action"})
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        print(f"ERROR: Unauthorized role: {user_role}")
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})
    
    print(f"✓ User authenticated - Role: {user_role}")
    logger.info("Custom analysis endpoint called")
    
    try:
        print(f"\n1. REQUEST DETAILS:")
        print(f"   - Method: {request.method}")
        print(f"   - Content-Type: {request.content_type}")
        print(f"   - Content-Length: {request.environ.get('CONTENT_LENGTH', 'Not set')}")
        print(f"   - Files keys: {list(request.files.keys())}")
        print(f"   - Form keys: {list(request.form.keys())}")
        
        # Check for the file in request
        if not request.files:
            print("ERROR: request.files is empty")
            return jsonify({'error': 'No files received in request'}), 400
        
        # Check if 'file' key exists
        if 'file' not in request.files:
            available_keys = list(request.files.keys())
            print(f"ERROR: 'file' key not found. Available keys: {available_keys}")
            return jsonify({
                'error': f'No file uploaded with key "file". Available keys: {available_keys}'
            }), 400
        
        file = request.files['file']
        print(f"✓ File received: {file.filename}")
        
        # Check if file is empty
        if not file.filename:
            print("ERROR: File has no filename")
            return jsonify({'error': 'No file selected or file has no name'}), 400
        
        filename = file.filename
        file_ext = os.path.splitext(filename)[1].lower()
        print(f"   - Filename: {filename}")
        print(f"   - Extension: {file_ext}")
        
        # Validate file extension
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        if file_ext not in allowed_extensions:
            print(f"ERROR: Invalid file extension: {file_ext}")
            return jsonify({'error': f'Invalid file format: {file_ext}. Please upload CSV or Excel files only.'}), 400
        
        print(f"✓ File extension valid")
        
        # Start file cleaning process
        print(f"\n2. CLEANING FILE...")
        clean_start = time.time()
        
        try:
            uploaded_dataframe = ai_manager.clean_file(file)
            clean_time = time.time() - clean_start
            print(f"✓ File cleaned in {clean_time:.2f}s")
        except Exception as e:
            print(f"ERROR during clean_file: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Failed to clean file: {str(e)}'}), 400
        
        if uploaded_dataframe is None:
            print("ERROR: clean_file returned None")
            return jsonify({'error': 'Failed to process the uploaded file'}), 400
            
        if uploaded_dataframe.empty:
            print("ERROR: DataFrame is empty after cleaning")
            return jsonify({'error': 'No valid data found in the uploaded file'}), 400
        
        print(f"   - DataFrame shape: {uploaded_dataframe.shape}")
        print(f"   - Columns: {list(uploaded_dataframe.columns)[:5]}...")  # First 5 columns
        print(f"   - Data types: {uploaded_dataframe.dtypes.to_dict()}")
        
        # Start AI analysis
        print(f"\n3. STARTING AI ANALYSIS...")
        analysis_start = time.time()
        
        try:
            analysis_result = ai_manager.ai_analyse_df(uploaded_dataframe)
            analysis_time = time.time() - analysis_start
            print(f"✓ AI analysis completed in {analysis_time:.2f}s")
        except Exception as e:
            print(f"ERROR during ai_analyse_df: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({'error': f'Analysis failed: {str(e)}'}), 500
        
        # Check if analysis returned an error
        if isinstance(analysis_result, dict) and "error" in analysis_result and len(analysis_result) == 1:
            print(f"WARNING: Analysis returned error: {analysis_result['error']}")
            return jsonify(analysis_result), 400
        
        print(f"   - Number of charts generated: {len(analysis_result)}")
        print(f"   - Chart keys: {list(analysis_result.keys())}")
        
        # Process results into JSON
        print(f"\n4. PROCESSING RESULTS...")
        process_start = time.time()
        
        chart_json = {}
        for chart_key, chart_data in analysis_result.items():
            print(f"   Processing {chart_key}...")
            
            if isinstance(chart_data, dict) and 'dataframe' in chart_data:
                df = chart_data['dataframe']
                
                if df is not None and not df.empty:
                    print(f"     - DataFrame shape: {df.shape}")
                    print(f"     - Columns: {list(df.columns)}")
                    
                    chart_json[chart_key] = {
                        'data': df.to_dict('records'),
                        'metadata': chart_data.get('metadata', {}),
                        'columns': df.columns.tolist()
                    }
                    print(f"     ✓ Chart data prepared ({len(df)} rows)")
                else:
                    print(f"     ⚠ Empty or None dataframe for {chart_key}")
            else:
                print(f"     ⚠ Invalid chart_data structure for {chart_key}")
        
        process_time = time.time() - process_start
        print(f"✓ Results processed in {process_time:.2f}s")
        
        if not chart_json:
            print("ERROR: No chart data could be processed")
            return jsonify({'error': 'Failed to process chart data for visualization'}), 500
        
        total_time = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"SUCCESS - Total time: {total_time:.2f}s")
        print(f"{'='*50}\n")
        
        return jsonify(chart_json)
    
    except Exception as e:
        error_time = time.time() - start_time
        print(f"\n{'='*50}")
        print(f"UNEXPECTED ERROR after {error_time:.2f}s")
        print(f"Error: {str(e)}")
        print(f"{'='*50}")
        
        logger.error(f"Unexpected error in custom_analysis: {str(e)}", exc_info=True)
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500


@app.route('/settings')
def settings():
    # Check if user is logged in
    if not session.get('logged_in'):
        return redirect(url_for('auth'))
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized access attempt by user with role: {user_role}")
        return redirect(url_for('index'))

    username = session.get('user_name', '')
    email = session.get('email', '')
    staff_id = session.get('staff_id')
    SUPER_ID = os.getenv('SUPER_ID')
    
    # Check if current user is super
    is_user_super = (staff_id == SUPER_ID)
    
    staff = auth_manager.load_staff()

    exisitng_referrals = referrals_manager.load_active_referrals() 

    return render_template(
        "settings.html",
        username=username,
        email=email,
        staff=staff,
        is_user_super=is_user_super, 
        current_referrals=exisitng_referrals 
    )

@app.route('/update_profile', methods=["POST","GET"])
def update_profile():
    # Check if user is logged in
    if not session.get('logged_in'):
        return redirect(url_for('auth'))
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})

    staff_id = session['staff_id']
    username = request.form.get('username')
    email = request.form.get('email')

    try:
        response = auth_manager.edit_profile(staff_id, username, email)
        if response:
            logger.info(f"Successfully updated staff id {staff_id}")

            # Update session so the placeholders reflect the new values
            session['user_name'] = response.get('user_name', session['user_name'])
            session['email'] = response.get('email', session['email'])

    except Exception as e:
        logger.info(f"Exception: {e}")

    # Pass session values to template
    return render_template(
        'settings.html',
        username=session.get('user_name', ''),
        email=session.get('email', '')
    )

@app.route('/update_password', methods=["POST","GET"])
def update_password():
    # Check if user is logged in
    if not session.get('logged_in'):
        return redirect(url_for('auth'))
    
    # Check user role
    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to approve withdrawals"})
    
    staff_id = session.get('staff_id')
    if not staff_id:
        flash("Session expired. Please log in again.", "error")
        return redirect(url_for('auth'))

    if request.method == "POST":
        current_password = request.form.get('current_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        try:
            # Attempt to update the password
            response = auth_manager.edit_password(
                staff_id=staff_id,
                current_password=current_password,
                new_password=new_password,
                confirm_password=confirm_password
            )

            if response:
                logger.info(f"Successfully updated password for staff id {staff_id}")
                flash("Password updated successfully!", "success")

                # Optional: update session or keep session same
                session['user_name'] = response.get('user_name', session.get('user_name'))
                session['email'] = response.get('email', session.get('email'))

            else:
                flash("Password update failed. Check your current password or new passwords.", "error")

        except Exception as e:
            logger.error(f"Exception while updating password: {e}")
            flash("An error occurred while updating password.", "error")

    # Pass session values to template
    return render_template(
        'settings.html',
        username=session.get('user_name', ''),
        email=session.get('email', '')
    )

@app.route('/add_staff', methods=['POST', 'GET'])
def add_staff():
    if not session.get('logged_in'):
        return redirect(url_for('auth'))

    user_role = session.get('role')
    if user_role not in ['super', 'admin']:
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to add staff"}), 403

    staff_id = session.get('staff_id')
    if not staff_id:
        flash("Session expired. Please log in again.", "error")
        return redirect(url_for('auth'))

    if request.method == "POST":
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        nrc = request.form.get('nrc')
        role = request.form.get('role')

        try:
            response = auth_manager.add_staff(
                username=username,
                email=email,
                password=password,
                nrc=nrc,
                role=role
            )

            if response:
                logger.info(f"Successfully added new staff: {username}")
                flash("New staff added successfully!", "success")
            else:
                flash("Failed to add new staff. Please check the details and try again.", "error")

        except Exception as e:
            logger.error(f"Exception while adding new staff: {e}")
            flash("An error occurred while adding new staff.", "error")

        # Redirect back to the same page (or staff list)
        return redirect(url_for('add_staff'))

    # For GET request, render the page
    return render_template('settings.html')

@app.route('/delete_staff/<staff_id>', methods=['POST'])
def delete_staff(staff_id):
    if not session.get('logged_in'):
        return redirect(url_for('auth'))

    user_role = session.get('role')
    if user_role != 'super':
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to delete staff"}), 403

    current_staff_id = session.get('staff_id')
    if not current_staff_id:
        flash("Session expired. Please log in again.", "error")
        return redirect(url_for('auth'))

    try:
        success = auth_manager.delete_staff(staff_id) 
        if success:
            logger.info(f"Successfully deleted staff id: {staff_id}")
            flash("Staff member deleted successfully!", "success")
        else:
            flash("Failed to delete staff member. You might be trying to delete the super admin or an invalid ID.", "error")

    except Exception as e:
        logger.error(f"Exception while deleting staff id {staff_id}: {e}")
        flash("An error occurred while deleting staff member.", "error")

    return redirect(url_for('settings'))


@app.route('/edit_staff/<staff_id>', methods=['POST'])
def edit_staff(staff_id):
    if not session.get('logged_in'):
        return redirect(url_for('auth'))

    user_role = session.get('role')
    if user_role != 'super':
        logger.warning(f"Unauthorized approval attempt by user with role: {user_role}")
        return jsonify({"success": False, "message": "You don't have permission to edit staff"}), 403

    current_staff_id = session.get('staff_id')
    if not current_staff_id:
        flash("Session expired. Please log in again.", "error")
        return redirect(url_for('auth'))

    username = request.form.get('username')
    email = request.form.get('email')
    nrc = request.form.get('nrc')
    role = request.form.get('role')
    is_active = request.form.get('is_active')
    is_active_bool = True if is_active == 'True' else False

    try:
        response = auth_manager.edit_staff_user(
            staff_id=staff_id,
            username=username,
            email=email,
            nrc=nrc,
            role=role,
            is_active=is_active_bool
        )

        if response:
            logger.info(f"Successfully updated staff id {staff_id}")
            flash("Staff member updated successfully!", "success")
        else:
            flash("Failed to update staff member. Please check the details and try again.", "error")

    except Exception as e:
        logger.error(f"Exception while updating staff id {staff_id}: {e}")
        flash("An error occurred while updating staff member.", "error")

    return redirect(url_for('settings'))


@app.route('/referrals')
def referrals():
    # load exisiting referalls
    exisitng_referrals = referrals_manager.load_active_referrals()

    return render_template('settings.html', 
                    active_tab='referrals',  
                    username=session.get('username'),
                    current_referrals = exisitng_referrals,
                    email=session.get('email')
                )


@app.route('/referrals/search_user', methods=['GET', 'POST'])
def search_user():
    exisitng_referrals = referrals_manager.load_active_referrals()
    if request.method == 'POST':
        query = request.form.get('query')
        try:
            user_details = referrals_manager.search_user(query)
            if user_details:
                return render_template('settings.html', 
                    search_results=user_details,
                    search_query=query,
                    active_tab='referrals',  
                    username=session.get('username'),
                    current_referrals = exisitng_referrals,
                    email=session.get('email')
                )
            else:
                return render_template('settings.html',
                    search_message='No user found matching the query',
                    search_query=query,
                    active_tab='referrals',  
                    username=session.get('username'),
                    current_referrals = exisitng_referrals,
                    email=session.get('email')
                )
        except Exception as e:
            logger.error(f"Error searching for user '{query}': {str(e)}", exc_info=True)
            return render_template('settings.html',
                search_error=f'An error occurred: {str(e)}',
                search_query=query,
                active_tab='referrals',  
                username=session.get('username'),
                current_referrals = exisitng_referrals,
                email=session.get('email')
            )
    
    return redirect(url_for('settings'))


@app.route('/referrals/assign_referral', methods=['POST', 'GET'])
def assign_referral():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        ref_code = request.form.get('ref_code')
        percentage = request.form.get('percentage')
        
        logger.info(f"Attempting to assign referral - user_id: {user_id}, ref_code: {ref_code}, percentage: {percentage}")
        
        if not user_id or not ref_code or not percentage:
            flash("Missing required fields. Please fill in all information.", "error")
            return redirect(url_for('settings') + '#referrals')
        
        try:
            result = referrals_manager.assign_referral(user_id, ref_code, percentage)
            if result:
                flash(f"Referral code '{ref_code}' assigned successfully!", "success")
            else:
                flash("Failed to assign referral code. It might already exist.", "error")
        except Exception as e:
            logger.error(f"Error assigning referral code '{ref_code}' to user '{user_id}': {str(e)}", exc_info=True)
            flash(f"An error occurred: {str(e)}", "error")
                
        return redirect(url_for('settings') + '#referrals')
         
    return redirect(url_for('settings'))



# other routes and methods in between
#
#
#
#
#
# here


@app.route('/logout')
def logout():
    """
    Logs the user out by clearing the session and removing relevant cookies.
    Redirects to the auth page.
    """
    # Clear all session data
    session.clear()

    # Remove cookies if used for login persistence
    response = make_response(redirect(url_for('auth')))  # redirect to your auth route
    #response.set_cookie('user_name', '', expires=0)
    #response.set_cookie('session_id', '', expires=0)  # example if you used a session cookie

    return response




# Run the app
if __name__ == "__main__":
    logger.info("Starting Flask application in debug mode")
    app.run(debug=True)
    products_manager = Products()
    logger.info("Starting bulk normalization...")
    print("Starting bulk normalization...")
    products_manager.normalize_new_products()
    logger.info("Bulk normalization completed")
    print("Done!")