# custom_database-1/run_all.py (With DB Ping Check)
import subprocess
import sys
import time
import signal
import os
import argparse

# --- Attempt to import SocketClient ---
try:
    # Assuming run_all.py is in custom_database-1/, and segadb is a direct subfolder
    from segadb.socketClient import SocketClient
except ImportError:
    print("ERROR: Could not import SocketClient. Make sure 'segadb' directory is accessible.")
    print("Ensure you are running this script from the 'custom_database-1' directory.")
    sys.exit(1)


# --- Configuration ---
DEFAULT_DB_PATH = "example_storage/database.segadb"
DEFAULT_DB_USER = "admin"
DEFAULT_DB_PASS = "password123"
DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_DB_PORT = 65432
DEFAULT_WEB_HOST = "0.0.0.0" # Default host for Flask app
DEFAULT_WEB_PORT = 5000     # Default port for Flask app
EXAMPLE_DB = False          # Set to True to run example database

PYTHON_EXECUTABLE = sys.executable # Use the same python interpreter

# Prefixes for messages
LAUNCHER_PREFIX = "[Launcher]"
WEB_APP_PREFIX = "[Web App]"
DB_SERVER_PREFIX = "[DB Server]"

# Constants for DB Ping
DB_PING_TIMEOUT = 30  # Max seconds to wait for DB server
DB_PING_INTERVAL = 2  # Seconds between ping attempts

# --- Argument Parsing ---
parser = argparse.ArgumentParser(description="Run SegaDB server and Web App.")
parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Path to the .segadb file.")
parser.add_argument("--db-user", default=DEFAULT_DB_USER, help="Database username.")
parser.add_argument("--db-pass", default=DEFAULT_DB_PASS, help="Database password.")
parser.add_argument("--db-host", default=DEFAULT_DB_HOST, help="Database server host.")
parser.add_argument("--db-port", type=int, default=DEFAULT_DB_PORT, help="Database server port.")
parser.add_argument("--web-host", default=DEFAULT_WEB_HOST, help="Web application host interface.")
parser.add_argument("--web-port", type=int, default=DEFAULT_WEB_PORT, help="Web application port.")
parser.add_argument("--flask-debug", action='store_true', help="Run Flask app in debug mode.")
parser.add_argument("--example-db", action='store_true', help="Run example database instead.") # Use action='store_true'
parser.add_argument("--verbose", action='store_true', help="Show full commands being executed.")

args = parser.parse_args()

# --- Process Management ---
db_process = None
web_process = None

def signal_handler(sig, frame):
    print(f"\n{LAUNCHER_PREFIX} Shutdown signal received...")
    # (Keep the rest of your signal_handler function as defined in the previous formatted version)
    # Terminate web process first
    if web_process and web_process.poll() is None:
        print(f"{LAUNCHER_PREFIX} Terminating Web App process (PID: {web_process.pid})...")
        web_process.terminate()
        try:
            web_process.wait(timeout=5)
            print(f"{LAUNCHER_PREFIX} Web App process terminated.")
        except subprocess.TimeoutExpired:
            print(f"{LAUNCHER_PREFIX} Web App process did not terminate gracefully, killing...")
            web_process.kill()
    # Terminate DB process
    if db_process and db_process.poll() is None:
        print(f"{LAUNCHER_PREFIX} Terminating Database Server process (PID: {db_process.pid})...")
        db_process.terminate()
        try:
            db_process.wait(timeout=10)
            print(f"{LAUNCHER_PREFIX} Database Server process terminated.")
        except subprocess.TimeoutExpired:
            print(f"{LAUNCHER_PREFIX} Database Server process did not terminate gracefully, killing...")
            db_process.kill()
    print(f"{LAUNCHER_PREFIX} Shutdown complete.")
    sys.exit(0)


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

try:
    # --- Start Database Server ---
    print(f"{DB_SERVER_PREFIX} Preparing to start Database Server...")

    if args.example_db:
        print(f"{DB_SERVER_PREFIX}   Using Example DB Server.")
        db_command = [
            PYTHON_EXECUTABLE,
            os.path.join("examples", "example_dbServer.py"), # Ensure this path is correct
            "--host", args.db_host,
            "--port", str(args.db_port)
        ]
        db_command_display = db_command[1:3] # Script path
        db_args_display = db_command[3:]     # Args
    else:
        print(f"{DB_SERVER_PREFIX}   Using SegaDB Server: {args.db_path}")
        db_command = [
            PYTHON_EXECUTABLE,
            os.path.join("segadb", "launch_server.py"),
            args.db_path,
            "--user", args.db_user,
            "--password", args.db_pass,
            "--host", args.db_host,
            "--port", str(args.db_port)
        ]
        db_command_display = db_command[1:3] # Script path + db file
        db_args_display = db_command[3:]     # Args

    print(f"{DB_SERVER_PREFIX} Starting: {' '.join(db_command_display)}")

    if db_args_display: print(f"{DB_SERVER_PREFIX}      Args: {' '.join(db_args_display)}")
    if args.verbose:    print(f"{DB_SERVER_PREFIX} Full Command: {' '.join(db_command)}")

    db_process = subprocess.Popen(db_command, cwd=os.getcwd())
    print(f"{DB_SERVER_PREFIX}   Database Server process initiated (PID: {db_process.pid}).")


    # --- Wait for DB Server with Ping ---
    print(f"{DB_SERVER_PREFIX}   Waiting for DB server at {args.db_host}:{args.db_port} to respond (max {DB_PING_TIMEOUT}s)...")
    start_time = time.time()
    db_ready = False
    while time.time() - start_time < DB_PING_TIMEOUT:
        # Check if the database process died while we are waiting
        process_status = db_process.poll()
        if process_status is not None:
            raise RuntimeError(f"{DB_SERVER_PREFIX} Database server process (PID: {db_process.pid}) exited prematurely with code {process_status} while waiting for ping.")

        # Define client here to ensure it's available in finally
        client = None 
        try:
            # Add connection timeout to SocketClient
            if args.verbose: print(f"{DB_SERVER_PREFIX}     Attempting connection...")
            client = SocketClient(host=args.db_host, port=args.db_port)
            
            
            if args.verbose: print(f"{DB_SERVER_PREFIX}     Connected. Sending ping...")
            ping_command = {"action": "ping", "params": {}}
            
            response = client.send_command(ping_command)
            if args.verbose: print(f"{DB_SERVER_PREFIX}     Received response: {response}")

            # If response is not None, check if it's a success response
            if response and response.get('status') == 'success':
                print(f"{DB_SERVER_PREFIX}   DB server responded successfully!")
                db_ready = True
                break
            else:
                # Server connected but didn't respond success to ping (unexpected)
                print(f"{DB_SERVER_PREFIX}     DB server connected but ping response was not 'success': {response}. Retrying...")

        except ConnectionRefusedError:
            print(f"{DB_SERVER_PREFIX}     Connection refused. Server might still be starting. Retrying in {DB_PING_INTERVAL}s...")
        except Exception as e:
            # Catch other potential errors (e.g., network issues, client errors)
            print(f"{DB_SERVER_PREFIX}     An error occurred during ping: {type(e).__name__} - {e}. Retrying in {DB_PING_INTERVAL}s...")

        # Wait before the next attempt
        time.sleep(DB_PING_INTERVAL)

    # After the loop, check if the DB is ready
    if not db_ready:
        # If we exited the loop without success, it timed out
        raise TimeoutError(f"{DB_SERVER_PREFIX} Database server did not respond to ping within {DB_PING_TIMEOUT} seconds.")


    # --- Start Web App Server (only if DB is ready) ---
    web_command = [
        PYTHON_EXECUTABLE,
        os.path.join("web_app_react", "app.py"),
        "--host", args.web_host,
        "--port", str(args.web_port)
    ]
    if args.flask_debug:
        web_command.append("--debug")

    print(f"{WEB_APP_PREFIX} Starting: {' '.join(web_command[1:3])}") # Script path
    if len(web_command) > 3:
        print(f"{WEB_APP_PREFIX}      Args: {' '.join(web_command[3:])}") # Args
    if args.verbose:
         print(f"{WEB_APP_PREFIX} Full Command: {' '.join(web_command)}")

    web_process = subprocess.Popen(web_command, cwd=os.getcwd())
    print(f"{WEB_APP_PREFIX}   Web App process initiated (PID: {web_process.pid}).")

    # Brief check if web process died immediately
    time.sleep(2) # Give it slightly longer to potentially crash
    if web_process.poll() is not None:
         raise RuntimeError(f"{WEB_APP_PREFIX} Web app server process (PID: {web_process.pid}) failed to start or exited prematurely (code: {web_process.poll()}). Check Flask logs.")

    # --- Summary and Monitoring Loop ---
    print("\n" + "="*60)
    print(f"{LAUNCHER_PREFIX} Summary:")
    print(f"{LAUNCHER_PREFIX}   Database Server Target: {args.db_host}:{args.db_port}")
    print(f"{LAUNCHER_PREFIX}   Web App Target: http://{args.web_host}:{args.web_port}")
    print(f"{LAUNCHER_PREFIX}   Flask Debug Mode: {'Enabled' if args.flask_debug else 'Disabled'}")
    print(f"{LAUNCHER_PREFIX}   Watching Processes: DB (PID:{db_process.pid}), Web (PID:{web_process.pid})")
    print(f"{LAUNCHER_PREFIX} Press Ctrl+C to stop all servers.")
    print("="*60 + "\n")

    # Monitoring loop (keep as before)
    while True:
        db_status = db_process.poll()
        web_status = web_process.poll()
        if db_status is not None:
             print(f"\n{DB_SERVER_PREFIX} ERROR: Database server process (PID: {db_process.pid}) exited unexpectedly with code: {db_status}. Shutting down.")
             signal_handler(signal.SIGTERM, None)
             break
        if web_status is not None:
             print(f"\n{WEB_APP_PREFIX} ERROR: Web app process (PID: {web_process.pid}) exited unexpectedly with code: {web_status}. Shutting down.")
             signal_handler(signal.SIGTERM, None)
             break
        time.sleep(2)

except (RuntimeError, TimeoutError, Exception) as e: # Catch specific errors from startup + general exceptions
    print(f"\n{LAUNCHER_PREFIX} FATAL ERROR during startup or execution: {e}")
    print(f"{LAUNCHER_PREFIX} Initiating shutdown...")
    signal_handler(signal.SIGTERM, None) # Attempt cleanup

except KeyboardInterrupt:
      print(f"\n{LAUNCHER_PREFIX} Ctrl+C detected by launcher, initiating shutdown...")
      # Signal handler will manage the shutdown
      pass

finally:
     print(f"{LAUNCHER_PREFIX} Exiting launcher script.")