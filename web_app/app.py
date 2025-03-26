import sys
import os

# Change the working directory to the parent directory to allow importing the segadb package.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from segadb.socketClient import SocketClient
from flask import Flask, jsonify, request, render_template

app = Flask(__name__)
socket_client = SocketClient(host='127.0.0.1', port=65432)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/ping', methods=['GET'])
def ping():
    response = socket_client.ping()
    return jsonify(response)

@app.route('/list_tables', methods=['GET'])
def list_tables():
    response = socket_client.list_tables()
    return jsonify(response)

@app.route('/query_table', methods=['POST'])
def query_table():
    data = request.json
    table = data.get('table')
    filter_condition = data.get('filter', None)
    response = socket_client.query_table(table, filter_condition)
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, port=5000)