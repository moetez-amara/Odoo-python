import xmlrpc.client
import pandas as pd
from collections import defaultdict
from datetime import datetime
import time
import plotly.express as px

# Odoo credentials
ODOO_URL = "https://kumulus.odoo.com"
ODOO_DB = "kumulus"
ODOO_USERNAME = "hmkaikia@kumuluswater.com"
ODOO_PASSWORD = "Hmk-KU*34"

# Connect to Odoo
try:
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {})
    if not uid:
        raise ValueError("Authentication failed. Check your credentials.")
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    print("Connected to Odoo successfully!")
except Exception as e:
    print(f"Error connecting to Odoo: {e}")
    exit(1)


# Retry logic for API calls
def fetch_with_retry(model, method, args, kwargs=None, retries=3, delay=5):
    for attempt in range(retries):
        try:
            return models.execute_kw(ODOO_DB, uid, ODOO_PASSWORD, model, method, args, kwargs or {})
        except xmlrpc.client.ProtocolError as e:
            if e.errcode == 429:  # Too Many Requests
                print(f"Rate limit hit. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                raise  # Re-raise other errors
    raise Exception("Failed to fetch data after multiple retries.")


# Fetch all products with their prices
def fetch_product_prices():
    fields = ['id', 'default_code', 'standard_price', 'name']
    products = fetch_with_retry('product.product', 'search_read', [[]], {'fields': fields})
    product_price_map = {}
    for product in products:
        product_price_map[product['id']] = {
            'standard_price': product['standard_price'],
            'default_code': product['default_code'],
            'name': product['name']
        }
    return product_price_map


# Fetch repair orders
def fetch_repair_orders():
    fields = ['id', 'name', 'create_date', 'move_ids', 'x_studio_catgorie_de_la_rparation']
    domain = [('state', '=', 'done')]  # Fetch only completed repairs
    return fetch_with_retry('repair.order', 'search_read', [domain], {'fields': fields})


# Fetch stock moves
def fetch_stock_move_lines(move_ids):
    fields = ['id', 'product_id', 'location_id', 'location_dest_id', 'qty_done']
    domain = [('move_id', 'in', move_ids)]
    return fetch_with_retry('stock.move.line', 'search_read', [domain], {'fields': fields})


# Process the data
def process_stock_moves(repair_orders, product_prices, movement_categories):
    all_moves = []
    repair_order_totals = defaultdict(float)  # Track total cost per repair order

    for repair in repair_orders:
        repair_ref = repair['name']
        repair_date = repair.get('create_date')
        repair_category = repair.get('x_studio_catgorie_de_la_rparation', 'Unknown')
        move_ids = repair['move_ids']

        if move_ids:
            stock_move_lines = fetch_stock_move_lines(move_ids)
            for move in stock_move_lines:
                product = move.get('product_id', [None, None])[0]
                product_info = product_prices.get(product, {})
                standard_price = product_info.get('standard_price', 0)

                qty_done = move.get('qty_done', 0)
                cost = standard_price * qty_done

                origin = move.get('location_id', [None, None])[1]
                destination = move.get('location_dest_id', [None, None])[1]
                category = next((cat for loc, cat in movement_categories.items() if loc in (destination or "")), "Unknown")

                # Update repair order total cost
                repair_order_totals[repair_ref] += cost

                # Store move details
                all_moves.append({
                    'repair_order': repair_ref,
                    'create_date': repair_date,
                    'repair_category': repair_category,
                    'product_id': product,
                    'product_name': product_info.get('name', 'Unknown'),
                    'origin': origin,
                    'destination': destination,
                    'category': category,
                    'quantity_done': qty_done,
                    'standard_price': standard_price,
                    'cost': cost
                })

    # Display totals for debugging
    for repair_order, total_cost in repair_order_totals.items():
        print(f"Repair Order: {repair_order}, Total Cost: {total_cost}")

    return pd.DataFrame(all_moves)


import os
from datetime import datetime

def generate_all_outputs(moves_df):
    # Define conversion multipliers
    currency_multipliers = {
        'TND': 1,
        'kTND': 0.001,
        'Euros': 1 / 3.2,  # Assuming 1 Euro = 3.2 TND; adjust as needed
        'kEuros': 0.001 / 3.2
    }

    # Create a dedicated folder with the session date
    session_date = datetime.now().strftime('%Y-%m-%d')
    output_dir = f"output_{session_date}"
    os.makedirs(output_dir, exist_ok=True)

    for unit, multiplier in currency_multipliers.items():
        # Adjust costs for the selected currency unit
        moves_df['adjusted_cost'] = moves_df['cost'] * multiplier

        # Handle missing or invalid data
        moves_df['adjusted_cost'] = moves_df['adjusted_cost'].fillna(0)  # Replace NaN values with 0
        moves_df = moves_df[moves_df['adjusted_cost'] >= 0]  # Remove rows with negative costs, if any

        # Save all data to Excel
        detailed_filename = os.path.join(output_dir, f"stock_moves_detailed_{unit}.xlsx")
        moves_df.to_excel(detailed_filename, index=False)

        # Summarize total costs by repair order
        summary_df = moves_df.groupby(['repair_order', 'repair_category']).agg(total_cost=('adjusted_cost', 'sum')).reset_index()
        summary_filename = os.path.join(output_dir, f"repair_order_summary_{unit}.xlsx")
        summary_df.to_excel(summary_filename, index=False)

        # Visualize quarterly costs
        moves_df['create_date'] = pd.to_datetime(moves_df['create_date'])
        moves_df['quarter'] = moves_df['create_date'].dt.to_period('Q').astype(str)
        quarterly_summary = moves_df.groupby(['quarter', 'category']).agg(total_cost=('adjusted_cost', 'sum')).reset_index()

        # Handle missing or invalid data in the graph summary
        quarterly_summary = quarterly_summary.dropna()  # Drop rows with NaN values
        quarterly_summary = quarterly_summary[quarterly_summary['total_cost'] >= 0]  # Remove negative costs, if any

        # Generate the plot with currency unit
        fig = px.bar(
            quarterly_summary,
            x='quarter',
            y='total_cost',
            color='category',
            title=f"Quarterly Costs by Category ({unit})",
            labels={'total_cost': f"Total Cost ({unit})"}
        )
        html_filename = os.path.join(output_dir, f"quarterly_costs_{unit}.html")
        fig.write_html(html_filename)

        print(f"Outputs generated successfully for {unit}! Stored in: {output_dir}")

# Main logic
if __name__ == "__main__":
    movement_categories = {
        "M-KH/Stock": "Retour vers le stock",
        "Virtual Locations/Réparation": "Consommation",
        "TMSS/Stock": "Retour vers le stock",
        "M-KH/Stock/Recyclage": "Retour vers le stock",
        "Virtual Locations/Scrap": "Rebut",
        "KHS/Stock": "Retour vers le stock",
        "TMSS/Stock/Recyclage": "Retour vers le stock",
        "Virtual Locations/Production": "Consommation",
        "Partners/Customers": "Retour vers le stock",
        "KHS/Alu technique": "Retour vers le stock",
        "TMSS/Pré-fabrication": "Retour vers le stock",
        "TMSS/Entrée": "Retour vers le stock",
        "KHS/TMS": "Retour vers le stock"
    }

    product_prices = fetch_product_prices()
    repair_orders = fetch_repair_orders()
    stock_moves_df = process_stock_moves(repair_orders, product_prices, movement_categories)
    generate_all_outputs(stock_moves_df)
