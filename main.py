from flask import Flask, render_template, flash, redirect, url_for, request, jsonify
from werkzeug.utils import secure_filename
from typing import Dict, Any, cast

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
ai_manager = AnalAI(test_key)
settings_manager = SettingsManager()


# Define a route
@app.route("/")
def index(): 
    """loads the overview dashboard"""
    total_users = users_manager.total_users()
    total_businesses = business_manager.total_businesses()
    total_products = products_manager.total_products()
    total_pending_withdraws = wallet_manager.total_withdrawal_requests()

    return render_template('index.html',
                           total_users = total_users,
                           total_businesses = total_businesses,
                           total_products = total_products,
                           total_pending_withdraws = total_pending_withdraws 
                           )


@app.route('/wallet')
def wallet():
    """loads the wallet page"""

    total_pending_withdraws = wallet_manager.total_withdrawal_requests()
    toal_inhouse_money = wallet_manager.total_inhouse_money()

    # card variables
    withdraw_requests = wallet_manager.load_pending_withdrawals()



    return render_template('wallet.html',
                           total_pending_withdraws = total_pending_withdraws,
                           toal_inhouse_money = toal_inhouse_money,
                           withdraw_requests = withdraw_requests  
                           )

@app.route('/approve_withdrawal_with_proof', methods=['POST'])
def approve_withdrawal_with_proof():
    """Approve a withdrawal request with proof of payout file upload"""
    
    try:
        # Get form data
        withdrawal_id = request.form.get('withdrawal_id')
        business_id = request.form.get('business_id')
        amount = float(request.form.get('amount', 0))
        notes = request.form.get('notes', '')  # Optional notes
        
        # Get uploaded file
        if 'proof_file' not in request.files:
            return jsonify({"success": False, "message": "No file uploaded"})
        
        file_object = request.files['proof_file']
        if file_object.filename == '':
            return jsonify({"success": False, "message": "No file selected"})
        
        # Upload file to Supabase bucket and get URL
        proof_url = wallet_manager.upload_payout_proof(file_object, withdrawal_id)
        if not proof_url:
            return jsonify({"success": False, "message": "Failed to upload proof file"})
        
        # Update the proof_of_payment field in withdrawals table
        proof_update_success = wallet_manager.update_proof_of_payment(withdrawal_id, proof_url)
        if not proof_update_success:
            return jsonify({"success": False, "message": "Failed to update proof of payment in database"})
        
        # SApprove the withdrawal (same as before)
        approval_success = wallet_manager.aprove_withdrawal(withdrawal_id)
        if not approval_success:
            return jsonify({"success": False, "message": "Failed to approve withdrawal"})
        
        # Reduce the business wallet balance (same as before)
        wallet_balance_result = wallet_manager.reduce_wallet_balance(
            business_id=business_id,
            withdraw_amount=amount
        )
        if not wallet_balance_result:
            return jsonify({"success": False, "message": "Failed to reduce wallet balance"})
        
        # Success - return JSON response for JavaScript
        return jsonify({
            "success": True, 
            "message": f"Successfully approved withdrawal {withdrawal_id} with proof of payout"
        })
        
    except Exception as e:
        print(f"Exception in approve_withdrawal_with_proof: {e}")
        return jsonify({"success": False, "message": "An error occurred while processing the withdrawal"})


@app.route('/reject_withdrawal', methods=['POST'])
def reject_withdrawal():
    """Reject a withdrawal request by its ID"""
    withdrawal_id = request.form.get('withdrawal_id')

    success = wallet_manager.reject_withdrawal(withdrawal_id)

    if success:
        flash(f"Successfully rejected withdrawal {withdrawal_id}.", "success")
    else:
        flash(f"Failed to reject withdrawal {withdrawal_id}.", "danger")

    return redirect(url_for('wallet'))

@app.route("/users")
def users():
    """Loads the users management page"""
    
    # Existing variables
    total_users = users_manager.total_users()
    total_user_growth_rate = users_manager.total_user_growth_rate()
    total_new_registrations = users_manager.total_new_registrations()
    new_registrations_rate = users_manager.new_registrations_rate()
    total_active_users = users_manager.total_active_users()
    active_users_growth_rate = users_manager.active_users_growh_rate()
    users_per_location = users_manager.users_per_location()
    
    # Get monthly users chart data
    monthly_user_trend_df = users_manager.monthly_user_trend()
    
    # Convert DataFrame to JSON-serializable formatt
    monthly_users_chart_data = {
        'labels': [],
        'data': []
    }
    
    if not monthly_user_trend_df.empty:
        # Convert Period to string for JSON serialization
        monthly_users_chart_data['labels'] = [str(month) for month in monthly_user_trend_df['month'].tolist()]
        monthly_users_chart_data['data'] = monthly_user_trend_df['user_count'].tolist()


    # Get chart data for user activity
    monthly_activity_df = users_manager.monthly_activity_trend()
    activity_chart_data = {
        'labels': [],
        'data': []
    }
    if not monthly_activity_df.empty:
        activity_chart_data['labels'] = [str(month) for month in monthly_activity_df['month'].tolist()]
        activity_chart_data['data'] = monthly_activity_df['active_user_count'].tolist()
    
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

@app.route('/search_users', methods=['POST'])
def search_users():
    """Search users based on a query"""
    query = request.form.get('query', '').strip()
    
    if not query:
        return jsonify({
            'success': False,
            'message': 'Please enter a search query.',
            'users': []
        }), 400
    
    try:
        # Retrieve users matching the query
        users = users_manager.retrieve_users_information(query)
        
        if not users:
            return jsonify({
                'success': True,
                'message': 'No users found matching the query.',
                'users': []
            }), 200
        
        return jsonify({
            'success': True,
            'message': f'Found {len(users)} user(s) matching the query.',
            'users': users
        }), 200
        
    except Exception as e:
        print(f"Error in search_users route: {e}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while searching for users.',
            'users': []
        }), 500


@app.route("/businesses")
def businesses():
    """loads the businesses management page"""
    
    total_businesses = business_manager.total_businesses()
    business_growth_rate = business_manager.total_businesses_growth_rate()
    new_businesses = business_manager.new_businesses_registrations()
    new_businesses_rate = business_manager.new_businesses_registrations_rate()
    total_active_businesses = business_manager.total_active_businesses()
    total_active_businesses_growth_rate = business_manager.total_active_businesses_growth_rate()
    
    # Get category data
    top_categories = business_manager.top_performing_categories()
    
    # Calculate percentages for progress bars
    if top_categories:
        max_total = max(category['total'] for category in top_categories)
        for category in top_categories:
            category['width'] = int((category['total'] / max_total) * 100) if max_total > 0 else 0
    
    # Get business activity data
    business_activity = business_manager.load_business_activity()
    
    # Get the monthly business trend
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

    # get the top performing industries
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




@app.route('/search_businesses', methods=['POST'])
def search_businesses():
    """Search businesses based on a query"""
    query = request.form.get('query', '').strip()
    
    if not query:
        return jsonify({
            'success': False,
            'message': 'Please enter a search query.',
            'businesses': []
        }), 400
    
    try:
        # Retrieve businesses matching the query
        businesses = business_manager.retrieve_business_information(query)
        
        if not businesses:
            return jsonify({
                'success': True,
                'message': 'No businesses found matching the query.',
                'businesses': []
            }), 200
        
        return jsonify({
            'success': True,
            'message': f'Found {len(businesses)} business(es) matching the query.',
            'businesses': businesses
        }), 200
        
    except Exception as e:
        print(f"Error in search_businesses route: {e}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while searching for businesses.',
            'businesses': []
        }), 500


@app.route('/products')
def products():

    total_products = products_manager.total_products()
    total_products_gr = products_manager.total_products_growth()
    low_stock_count = products_manager.low_stock_count()
    low_stock_percent = products_manager.low_stock_percent()
    total_revenue = products_manager.total_revenue()
    total_revenue_growth = products_manager.total_revenue_growth()
    ranking_products = products_manager.product_ranking(settings_manager.product_performance_by)

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


@app.route('/industry_analysis')
def industry_analysis():
    total_industries = industry_manager.total_industries()
    new_industries = industry_manager.check_new_industries()
    industries_total = industry_manager.get_industries_total()
    industry_revenue_growth_rate = industry_manager.total_industry_revenue_rate()
    top_performing_industry = max(
        business_manager.get_top_performing_industries(),
        key=lambda x: x[1]
    )[0].capitalize()
    
    industry_market_share = industry_manager.industry_market_share()
    industry_average_growth_rate = industry_manager.industry_average_growth_rate()
    yearly_industry_growth_rate = industry_manager.industry_average_growth_rate(days=365)
    
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


@app.route('/search_industry', methods=['POST'])
def search_industry():
    # Get the industry from the request - handle both JSON and form data
    industry = None
    
    # Try to get from JSON first
    if request.is_json and request.json:
        industry = request.json.get('industry', '').strip()
    # Fallback to form data
    elif request.form:
        industry = request.form.get('industry', '').strip()
    # Fallback to raw data if it's a simple string
    else:
        try:
            data = request.get_data(as_text=True)
            if data:
                industry = data.strip()
        except:
            pass
    
    # Validate input
    if not industry:
        return jsonify({'error': 'Please provide an industry'}), 400
    
    try:
        # Get the revenue trend data
        revenue_trend_data_df = industry_manager.industry_revenue_trend(industry)
        
        # Get the customer growth trend data
        customer_trend_data_df = industry_manager.customer_growth_trend(industry)
        
        # Get the average order trend data
        average_order_trend_data_df = industry_manager.industry_average_order_trend(industry)
        
        # Get the seasonal performance trend data
        seasonal_performance_trend_data_df = industry_manager.industry_seasonal_performance_trend(industry)
        
        # Check if all dataframes have data
        if revenue_trend_data_df.empty and customer_trend_data_df.empty and average_order_trend_data_df.empty and seasonal_performance_trend_data_df.empty:
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
        
        # Convert seasonal performance trend data to json
        seasonal_performance_chart_data = {
            'labels': [],
            'data': []
        }
        
        if not seasonal_performance_trend_data_df.empty:
            # For seasonal data, the column names are 'season' and 'total_sales'
            seasonal_performance_chart_data['labels'] = seasonal_performance_trend_data_df['season'].tolist()
            seasonal_performance_chart_data['data'] = seasonal_performance_trend_data_df['total_sales'].tolist()
        
        # Combine all datasets in the response
        response_data = {
            'industry': industry.capitalize(),
            'revenue_trend': monthly_industry_revenue_chart_data,
            'customer_trend': monthly_industry_customer_chart_data,
            'average_order_trend': monthly_industry_average_order_chart_data,
            'seasonal_performance': seasonal_performance_chart_data
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        return jsonify({'error': f'An error occurred: {str(e)}'}), 500
    


@app.route('/analysis', methods=['POST', 'GET'])
def analysis():
    try:
        # Get the most recent analytics data from database
        latest_insight_record = ai_manager.grab_weekly_insights()
        
        # Initialize variables
        weekly_data = None
        insights_count = 0
        tables_analyzed = 0
        last_updated = 'Never'
        
        if latest_insight_record:
            # Parse the JSON data
            try:
                weekly_data = json.loads(latest_insight_record['insight'])
                insights_count = len(weekly_data) if weekly_data else 0
                tables_analyzed = len([k for k, v in weekly_data.items() if v]) if weekly_data else 0
                
                # Format the timestamp if available
                if 'created_at' in latest_insight_record:
                    last_updated = datetime.fromisoformat(latest_insight_record['created_at']).date().isoformat()

                else:
                    last_updated = 'Recently'
                
            except (json.JSONDecodeError, KeyError) as e:
                print(f"Error parsing insights JSON: {e}")
                weekly_data = None
        
        return render_template('analysis.html', 
                             weekly_data=weekly_data,
                             insights_count=insights_count,
                             tables_analyzed=tables_analyzed,
                             last_updated=last_updated)
    
    except Exception as e:
        print(f"Error in analysis route: {e}")
        return render_template('analysis.html', 
                             weekly_data=None,
                             insights_count=0,
                             tables_analyzed=0,
                             last_updated='Error')
    
    
@app.route('/generate-insights', methods=['POST'])
def generate_insights():
    try:
        
        # Get the most recent insight from database
        latest_record = ai_manager.grab_weekly_insights()
        
        should_generate = True
        days_difference = 0
        
        if latest_record and 'created_at' in latest_record:
            # Parse the timestamp
            created_at_str = latest_record['created_at']
            
            try:
                # Handle different timestamp formats
                if 'T' in created_at_str:
                    created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                else:
                    created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
                
                # Calculate days difference
                now = datetime.utcnow()
                if created_at.tzinfo is None:
                    now = now.replace(tzinfo=None)
                
                days_difference = (now - created_at).days
                
                print(f"Last insight was {days_difference} days ago")
                
                # Only generate if 7 or more days have passed
                should_generate = days_difference >= 7
                
            except ValueError as e:
                print(f"Could not parse timestamp: {created_at_str}, error: {e}")
                should_generate = True
        else:
            print("No previous insights found or no timestamp")
            should_generate = True
        
        if should_generate:
            print("Generating new insights...")
            ai_manager.generate_weekly_insights()
            data = ai_manager.weekly_insights
            ai_manager.store_weekly_report(data)
            
            return jsonify({
                'success': True, 
                'message': 'New insights generated successfully',
                'generated': True
            })
        else:
            return jsonify({
                'success': True, 
                'message': 'Recent insights are still valid. New insights will be generated after 7 days.',
                'generated': False,
                'days_remaining': 7 - days_difference
            })
    
    except Exception as e:
        print(f"Error in generate_insights: {e}")
        return jsonify({
            'success': False, 
            'message': f'Error: {str(e)}',
            'generated': False
        })
    

@app.route('/custom_analysis', methods=['POST'])
def custom_analysis():
    try:
        print("=== CUSTOM ANALYSIS DEBUG START ===")
        print(f"Request method: {request.method}")
        print(f"Request files: {dict(request.files)}")
        print(f"Request form: {dict(request.form)}")
        print(f"Request content type: {request.content_type}")
        print(f"Request content length: {request.environ.get('CONTENT_LENGTH', 'Not set')}")
        
        # Check for the file in request
        if not request.files:
            print("ERROR: request.files is completely empty")
            return jsonify({'error': 'No files received in request'}), 400
        
        # Debug all files in the request
        for key in request.files:
            print(f"Found file key: {key}, value: {request.files[key]}")
        
        # Check if 'file' key exists
        if 'file' not in request.files:
            available_keys = list(request.files.keys())
            print(f"ERROR: 'file' key not found. Available keys: {available_keys}")
            return jsonify({
                'error': f'No file uploaded with key "file". Available keys: {available_keys}'
            }), 400
        
        file = request.files['file']
        print(f"File object: {file}")
        print(f"File filename: {file.filename}")
        print(f"File content type: {getattr(file, 'content_type', 'Not available')}")
        
        # Check if file is empty
        if not file.filename:
            print("ERROR: File has no filename")
            return jsonify({'error': 'No file selected or file has no name'}), 400
            
        # Rest of your existing code...
        filename = file.filename
        print(f"Processing file: {filename}")
        
        # Validate file extension
        allowed_extensions = {'.csv', '.xlsx', '.xls'}
        file_ext = os.path.splitext(filename)[1].lower()
        print(f"File extension: {file_ext}")
        
        if file_ext not in allowed_extensions:
            print(f"ERROR: Invalid file extension: {file_ext}")
            return jsonify({'error': f'Invalid file format: {file_ext}. Please upload CSV or Excel files only.'}), 400
        
        # Continue with the rest of your processing...
        print("Starting file cleaning process...")
        uploaded_dataframe = ai_manager.clean_file(file)
        
        if uploaded_dataframe is None:
            print("ERROR: clean_file returned None")
            return jsonify({'error': 'Failed to process the uploaded file'}), 400
            
        if uploaded_dataframe.empty:
            print("ERROR: DataFrame is empty after cleaning")
            return jsonify({'error': 'No valid data found in the uploaded file'}), 400
        
        print(f"DataFrame shape: {uploaded_dataframe.shape}")
        
        # Continue with analysis...
        analysis_result = ai_manager.ai_analyse_df(uploaded_dataframe)
        
        if isinstance(analysis_result, dict) and "error" in analysis_result and len(analysis_result) == 1:
            return jsonify(analysis_result), 400
        
        # Process results...
        chart_json = {}
        for chart_key, chart_data in analysis_result.items():
            if isinstance(chart_data, dict) and 'dataframe' in chart_data:
                df = chart_data['dataframe']
                if df is not None and not df.empty:
                    chart_json[chart_key] = {
                        'data': df.to_dict('records'),
                        'metadata': chart_data.get('metadata', {}),
                        'columns': df.columns.tolist()
                    }
        
        if not chart_json:
            return jsonify({'error': 'Failed to process chart data for visualization'}), 500
        
        print(f"Successfully processed {len(chart_json)} charts")
        return jsonify(chart_json)
    
    except Exception as e:
        print(f"UNEXPECTED ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': f'Analysis failed: {str(e)}'}), 500


# Additional debugging function to test file processing independently
@app.route('/test_file_upload', methods=['POST'])
def test_file_upload():
    """Debug route to test just the file upload part"""
    try:
        print("=== FILE UPLOAD TEST ===")
        
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        
        if not file.filename:
            return jsonify({'error': 'No filename'}), 400
            
        # Try to read first few lines
        file_content = file.read(1000).decode('utf-8')  # Read first 1000 bytes
        file.seek(0)  # Reset file pointer
        
        return jsonify({
            'filename': file.filename,
            'content_preview': file_content,
            'success': True
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# Run the app
if __name__ == "__main__":
    app.run(debug=True)
