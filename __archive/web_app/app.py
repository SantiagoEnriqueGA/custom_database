import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from segadb.socketClient import SocketClient
from flask import Flask, jsonify, request, render_template, session, redirect, url_for
from functools import wraps

app = Flask(__name__)
app.secret_key = 'your_secret_key_here'  # Replace with a secure key in production
socket_client = SocketClient(host='127.0.0.1', port=65432)

# Decorator to require login for protected routes
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'session_token' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/')
def home():
    return render_template('index.html', logged_in='session_token' in session)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        response = socket_client.login_user(username, password)
        if response.get('status') == 'success':
            session['session_token'] = response.get('session_token')
            return jsonify({'status': 'success', 'message': 'Logged in successfully'})
        return jsonify({'status': 'error', 'message': response.get('message', 'Login failed')})
    return render_template('login.html')

@app.route('/logout', methods=['POST'])
@login_required
def logout():
    session_token = session.pop('session_token', None)
    if session_token:
        socket_client.logout_user(session_token)
    return redirect(url_for('home'))

@app.route('/list_tables', methods=['GET'])
@login_required
def list_tables():
    response = socket_client.list_tables()
    if response.get('status') == 'success':
        return render_template('tables.html', tables=response.get('data', []))
    return render_template('tables.html', error=response.get('message', 'Failed to list tables'))

@app.route('/create_table', methods=['GET', 'POST'])
@login_required
def create_table():
    if request.method == 'POST':
        table_name = request.form.get('table_name')
        columns = request.form.get('columns').split(',')
        response = socket_client.create_table(table_name, [col.strip() for col in columns])
        return jsonify(response)
    return render_template('create_table.html')

@app.route('/drop_table', methods=['POST'])
@login_required
def drop_table():
    table_name = request.form.get('table_name')
    response = socket_client.drop_table(table_name)
    return jsonify(response)

@app.route('/insert_record', methods=['GET', 'POST'])
@login_required
def insert_record():
    if request.method == 'POST':
        table = request.json.get('table')
        record = request.json.get('record')
        response = socket_client.insert_record(table, record)
        return jsonify(response)
    return render_template('insert_record.html')

@app.route('/update_record', methods=['POST'])
@login_required
def update_record():
    table = request.json.get('table')
    record_id = request.json.get('record_id')
    updates = request.json.get('updates')
    response = socket_client.update_record(table, record_id, updates)
    return jsonify(response)

@app.route('/delete_record', methods=['POST'])
@login_required
def delete_record():
    table = request.json.get('table')
    record_id = request.json.get('record_id')
    response = socket_client.delete_record(table, record_id)
    return jsonify(response)

@app.route('/query_table', methods=['GET', 'POST'])
@login_required
def query_table():
    if request.method == 'POST':
        table = request.json.get('table')
        filter_condition = request.json.get('filter', None)
        response = socket_client.query_table(table, filter_condition)
        return jsonify(response)
    return render_template('query_table.html')

@app.route('/create_procedure', methods=['GET', 'POST'])
@login_required
def create_procedure():
    if request.method == 'POST':
        procedure_name = request.json.get('procedure_name')
        procedure_code = request.json.get('procedure_code')
        response = socket_client.create_procedure(procedure_name, procedure_code)
        return jsonify(response)
    return render_template('create_procedure.html')

@app.route('/execute_procedure', methods=['POST'])
@login_required
def execute_procedure():
    procedure_name = request.json.get('procedure_name')
    procedure_params = request.json.get('procedure_params', {})
    response = socket_client.execute_procedure(procedure_name, procedure_params)
    return jsonify(response)

@app.route('/db_info', methods=['GET'])
@login_required
def db_info():
    response = socket_client.get_db_info()
    if response.get('status') == 'success':
        return render_template('db_info.html', info=response.get('data', {}))
    return render_template('db_info.html', error=response.get('message', 'Failed to get database info'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)