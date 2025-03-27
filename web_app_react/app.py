# custom_database-1\web_app\app.py (Modified)
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.socketClient import SocketClient
from flask import Flask, jsonify, request, session, redirect, url_for, send_from_directory
from flask_cors import CORS # Import CORS
from functools import wraps
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

app = Flask(__name__, static_folder='../web_app_react/dist', static_url_path='/') # Point to React build output
CORS(app, supports_credentials=True) # Enable CORS for all routes, allowing credentials

app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'your_default_secret_key_here') # Use environment variable for secret key

# --- Database Connection ---
# Encapsulate socket client logic for potential reconnection
def get_socket_client():
    # Simple implementation: create a new client each time.
    # In a real app, you might pool connections or handle errors better.
    try:
        client = SocketClient(host='127.0.0.1', port=65432)
        # Optional: Add a quick check like ping() here if available
        # response = client.ping()
        # if response.get('status') != 'success':
        #     raise ConnectionError("Failed to ping DB server")
        return client
    except Exception as e:
        log.error(f"Failed to connect to database socket: {e}")
        return None

# --- Authentication Decorator ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        log.debug(f"Checking session: {session}")
        if 'session_token' not in session:
            log.warning("Login required: No session token found.")
            return jsonify({'status': 'error', 'message': 'Authentication required'}), 401
        # Optional: Validate token with DB server on each request? Might be overkill.
        # client = get_socket_client()
        # if client: # Basic check if client could be created
        #     # Add a check_session method to SocketClient/Database if needed
        #     # response = client.check_session(session['session_token'])
        #     # if response.get('status') != 'success':
        #     #     session.pop('session_token', None) # Clear invalid token
        #     #     return jsonify({'status': 'error', 'message': 'Invalid session'}), 401
        # else:
        #      return jsonify({'status': 'error', 'message': 'Database connection error'}), 500
        return f(*args, **kwargs)
    return decorated_function

# --- Serve React App ---
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_react_app(path):
    # Handle API routes separately
    if path.startswith('api/'):
        log.warning(f"Attempted to serve API path as static file: {path}")
        # Find the correct API handler or return 404 if none match
        # This block might not be strictly necessary if API routes are defined before this catch-all
        return jsonify({"status": "error", "message": "API endpoint not found through static serving."}), 404

    # Serve static files if they exist
    static_file_path = os.path.join(app.static_folder, path)
    if path != "" and os.path.exists(static_file_path):
        log.info(f"Serving static file: {path}")
        return send_from_directory(app.static_folder, path)
    else:
        # Serve index.html for SPA routing fallback
        log.info(f"Serving React app index.html for path: {path}")
        index_path = os.path.join(app.static_folder, 'index.html')
        if not os.path.exists(index_path):
            log.error(f"React build not found at {app.static_folder}/index.html. Run 'npm run build' in web_app_react.")
            return jsonify({"status": "error", "message": "Frontend build not found."}), 404
        return send_from_directory(app.static_folder, 'index.html')


# --- API Endpoints ---

# Endpoint to check login status
@app.route('/api/check_auth', methods=['GET'])
def check_auth():
    if 'session_token' in session:
        # Optionally, you could add username info here if needed by the frontend
        # client = get_socket_client()
        # username = client.get_username_for_token(session['session_token']) # Needs implementation
        return jsonify({'status': 'success', 'logged_in': True})
    else:
        return jsonify({'status': 'success', 'logged_in': False})

@app.route('/api/login', methods=['POST'])
def login():
    client = get_socket_client()
    if not client:
        return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    username = request.json.get('username')
    password = request.json.get('password')
    if not username or not password:
        return jsonify({'status': 'error', 'message': 'Username and password required'}), 400

    response = client.login_user(username, password)
    if response.get('status') == 'success':
        session['session_token'] = response.get('session_token')
        log.info(f"Login successful for user: {username}. Session: {session}")
        return jsonify({'status': 'success', 'message': 'Logged in successfully'})
    else:
        log.warning(f"Login failed for user: {username}. Reason: {response.get('message')}")
        return jsonify({'status': 'error', 'message': response.get('message', 'Login failed')}), 401

@app.route('/api/logout', methods=['POST'])
@login_required # Ensure user is logged in to log out
def logout():
    session_token = session.pop('session_token', None)
    log.info(f"Logout requested. Cleared session token: {session_token is not None}")
    if session_token:
        client = get_socket_client()
        if client:
            # Best effort logout on the DB server side
            client.logout_user(session_token)
        else:
            log.warning("Could not connect to DB to perform server-side logout.")
    return jsonify({'status': 'success', 'message': 'Logged out successfully'})

# --- Table Endpoints ---
@app.route('/api/tables', methods=['GET'])
@login_required
def list_tables():
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    response = client.list_tables()
    # Ensure response has 'data' key even on failure for consistency
    if response.get('status') != 'success':
        response['data'] = []
    return jsonify(response)

@app.route('/api/tables', methods=['POST'])
@login_required
def create_table():
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    data = request.get_json()
    table_name = data.get('table_name')
    columns_str = data.get('columns') # Expecting a comma-separated string

    if not table_name or not columns_str:
        return jsonify({'status': 'error', 'message': 'Table name and columns required'}), 400

    try:
        # Basic validation/sanitization might be needed here
        columns = [col.strip() for col in columns_str.split(',') if col.strip()]
        if not columns:
             raise ValueError("Columns cannot be empty.")
    except Exception as e:
        return jsonify({'status': 'error', 'message': f'Invalid columns format: {e}'}), 400

    response = client.create_table(table_name, columns)
    return jsonify(response)

@app.route('/api/tables/<string:table_name>', methods=['DELETE'])
@login_required
def drop_table(table_name):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not table_name:
        return jsonify({'status': 'error', 'message': 'Table name required'}), 400

    response = client.drop_table(table_name)
    return jsonify(response)

@app.route('/api/tables/<string:table_name>/records', methods=['POST'])
@login_required
def insert_record(table_name):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not table_name:
        return jsonify({'status': 'error', 'message': 'Table name required'}), 400

    record_data = request.json.get('record')
    if not record_data or not isinstance(record_data, dict):
         return jsonify({'status': 'error', 'message': 'Record data (JSON object) required'}), 400

    response = client.insert_record(table_name, record_data)
    return jsonify(response)

@app.route('/api/tables/<string:table_name>/records/<string:record_id>', methods=['PUT'])
@login_required
def update_record(table_name, record_id):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not table_name or not record_id:
        return jsonify({'status': 'error', 'message': 'Table name and record ID required'}), 400

    updates = request.json.get('updates')
    if not updates or not isinstance(updates, dict):
         return jsonify({'status': 'error', 'message': 'Updates data (JSON object) required'}), 400

    # Note: record_id might need type conversion depending on how it's stored/expected
    try:
        # Example: if IDs are always integers
        # record_id_typed = int(record_id)
        record_id_typed = record_id # Assuming string or handled by client/db
    except ValueError:
         return jsonify({'status': 'error', 'message': 'Invalid record ID format'}), 400

    response = client.update_record(table_name, record_id_typed, updates)
    return jsonify(response)

@app.route('/api/tables/<string:table_name>/records/<string:record_id>', methods=['DELETE'])
@login_required
def delete_record(table_name, record_id):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not table_name or not record_id:
        return jsonify({'status': 'error', 'message': 'Table name and record ID required'}), 400

    # Note: record_id might need type conversion
    try:
        # record_id_typed = int(record_id)
        record_id_typed = record_id
    except ValueError:
         return jsonify({'status': 'error', 'message': 'Invalid record ID format'}), 400

    response = client.delete_record(table_name, record_id_typed)
    return jsonify(response)

@app.route('/api/tables/<string:table_name>/query', methods=['POST'])
@login_required
def query_table(table_name):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not table_name:
        return jsonify({'status': 'error', 'message': 'Table name required'}), 400

    filter_condition = request.json.get('filter', None) # Filter is optional

    # Security Warning: Directly evaluating filter strings like "lambda ..." is extremely risky
    # if sent from the client. The original code did this.
    # A safer approach is needed:
    # 1. Predefined filters on the backend.
    # 2. A structured query language/format (like SQL WHERE clauses or MongoDB-style queries).
    # 3. Careful sanitization and validation if allowing flexible filters.
    # For now, we pass it as is, assuming the SocketClient/Database handles it safely (or unsafely).
    # **THIS IS A MAJOR SECURITY CONCERN in the original design if the filter comes from user input.**
    # Consider disabling or heavily restricting the 'filter' parameter from the API.
    if filter_condition:
        log.warning(f"Executing query on '{table_name}' with potentially unsafe filter: {filter_condition}")

    response = client.query_table(table_name, filter_condition)
    return jsonify(response)

# --- View Endpoints --- START NEW ---
@app.route('/api/views', methods=['GET'])
@login_required
def list_views():
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    response = client.list_views()
    if response.get('status') != 'success':
        response['data'] = []
    return jsonify(response)

@app.route('/api/views/<string:view_name>/query', methods=['POST']) # POST for consistency, though GET might work
@login_required
def query_view(view_name):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not view_name:
        return jsonify({'status': 'error', 'message': 'View name required'}), 400

    # Views likely don't support arbitrary filters in the current backend implementation
    response = client.query_view(view_name)
    return jsonify(response)

# --- Materialized View Endpoints ---
@app.route('/api/materialized_views', methods=['GET'])
@login_required
def list_materialized_views():
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    response = client.list_materialized_views()
    if response.get('status') != 'success':
        response['data'] = []
    return jsonify(response)

@app.route('/api/materialized_views/<string:view_name>/query', methods=['POST']) # POST for consistency
@login_required
def query_materialized_view(view_name):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not view_name:
        return jsonify({'status': 'error', 'message': 'Materialized view name required'}), 400

    # Materialized views likely don't support arbitrary filters either
    response = client.query_materialized_view(view_name)
    return jsonify(response)
# --- View Endpoints --- END NEW ---


@app.route('/api/procedures', methods=['POST'])
@login_required
def create_procedure():
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    data = request.get_json()
    procedure_name = data.get('procedure_name')
    procedure_code = data.get('procedure_code')

    if not procedure_name or not procedure_code:
         return jsonify({'status': 'error', 'message': 'Procedure name and code required'}), 400

    # Security Warning: Executing arbitrary code sent via API ('procedure_code') is extremely dangerous.
    # This feature should likely be removed or heavily restricted (e.g., only allow predefined actions).
    log.warning(f"Creating procedure '{procedure_name}' with potentially unsafe code.")

    response = client.create_procedure(procedure_name, procedure_code)
    return jsonify(response)

@app.route('/api/procedures/<string:procedure_name>/execute', methods=['POST'])
@login_required
def execute_procedure(procedure_name):
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    if not procedure_name:
         return jsonify({'status': 'error', 'message': 'Procedure name required'}), 400

    procedure_params = request.json.get('params', {}) # Params are optional
    if not isinstance(procedure_params, dict):
        return jsonify({'status': 'error', 'message': 'Parameters must be a JSON object'}), 400

    response = client.execute_procedure(procedure_name, procedure_params)
    return jsonify(response)

@app.route('/api/db_info', methods=['GET'])
@login_required
def db_info():
    client = get_socket_client()
    if not client: return jsonify({'status': 'error', 'message': 'Database connection error'}), 500

    response = client.get_db_info()
    # Ensure 'data' key exists for consistency
    if response.get('status') != 'success':
        response['data'] = {}
    return jsonify(response)


if __name__ == '__main__':
    # Make sure the DB server (launch_server.py?) is running first
    log.info("Starting Flask API server for SegaDB.")
    log.info("Ensure the SegaDB socket server (launch_server.py?) is running.")
    log.info(f"React frontend expected in: {os.path.abspath(app.static_folder)}")
    # Use waitress or gunicorn for production instead of app.run
    app.run(debug=True, port=5000, host='0.0.0.0') # Listen on all interfaces