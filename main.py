from flask import Flask, render_template, flash, redirect, url_for, request, jsonify

# custom imports
from users import Users
from businesses import Businesses
from products import Products
from wallet import Wallet
import os
from dotenv import load_dotenv

load_dotenv() 

# Create the Flask app
app = Flask(__name__)


app.secret_key = os.getenv('APP_SECRET_KEY') 

# tools
users_manager = Users()
business_manager = Businesses()
products_manager = Products()
wallet_manager = Wallet()

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


# Run the app
if __name__ == "__main__":
    app.run(debug=True)
