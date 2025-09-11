from flask import Flask, render_template, flash, redirect, url_for

# custom imports
from users import Users
from businesses import Businesses
from products import Products
from wallet import Wallet

# Create the Flask app
app = Flask(__name__)

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

@app.route('/approve_withdrawal/<withdrawal_id>')
def approve_withdrawal(withdrawal_id):
    """Approve a withdrawal request by its ID"""
    success = wallet_manager.approve_withdrawal(withdrawal_id)

    if success:
        flash(f"Successfully approved withdrawal {withdrawal_id}.", "success")
    else:
        flash(f"Failed to approve withdrawal {withdrawal_id}.", "danger")

    # redirect back to wallet page (so page reloads properly)
    return redirect(url_for('wallet'))



# Run the app
if __name__ == "__main__":
    app.run(debug=True)
