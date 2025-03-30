import zlib
import typer
import json
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
import traceback # For more detailed error logging if needed

# Ensure segadb package is importable
try:
    # Assumes segadb_cli.py is in the project root alongside the segadb folder
    # Or segadb is installed
    if __package__ is None and not hasattr(sys, 'frozen'):
        path = os.path.realpath(os.path.abspath(__file__))
        sys.path.insert(0, os.path.dirname(os.path.dirname(path)))
    from segadb import Storage, SocketClient, SocketUtilities, Database, Record
except ImportError as e:
    print(f"Error importing segadb: {e}")
    print("Make sure segadb is installed or accessible in your PYTHONPATH.")
    sys.exit(1)
except Exception as e:
    print(f"An unexpected error occurred during import: {e}")
    sys.exit(1)

# --- Typer App Initialization ---
app = typer.Typer(
    name="segadb-cli",
    help="Command-Line Interface for managing SegaDB databases.",
    no_args_is_help=True,
    context_settings={"help_option_names": ["-h", "--help"]},
)

# --- State Management ---
class CLIState:
    db_file: Optional[Path] = None
    host: Optional[str] = None
    port: Optional[int] = None
    user: Optional[str] = None          # User for local load OR remote login
    password: Optional[str] = None      # Password for local load OR remote login
    connection: Optional[object] = None # Holds Database or SocketClient
    connection_type: Optional[str] = None # 'local' or 'remote'
    session_token: Optional[str] = None # Store the token after remote login

state = CLIState()

# --- Helper Functions ---
def _ensure_remote(ctx: typer.Context) -> SocketClient:
    """Ensures connection is remote and returns the client."""
    conn, conn_type = get_connection(ctx) # Ensure connection is established
    if conn_type != 'remote':
        typer.secho("Error: This command requires a remote connection (--host/--port).", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if not isinstance(conn, SocketClient):
         # This case should ideally not happen if get_connection logic is correct
         typer.secho("Internal Error: Connection type is remote but connection object is not SocketClient.", fg=typer.colors.RED)
         raise typer.Exit(code=1)
    return conn

def _ensure_local(ctx: typer.Context) -> Database:
    """Ensures connection is local and returns the Database instance."""
    conn, conn_type = get_connection(ctx) # Ensure connection is established
    if conn_type != 'local':
        typer.secho("Error: This command requires a local file connection (--db-file).", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if not isinstance(conn, Database):
        # This case should ideally not happen if get_connection logic is correct
         typer.secho("Internal Error: Connection type is local but connection object is not Database.", fg=typer.colors.RED)
         raise typer.Exit(code=1)
    return conn


def _save_local_db(db: Database):
    """Helper to save the local database file, handling potential errors."""
    if not state.db_file:
         typer.secho("Internal Error: Cannot save local database, db_file path is missing.", fg=typer.colors.RED)
         raise typer.Exit(code=1)
    try:
        Storage.save(db, str(state.db_file))
    except Exception as e:
        typer.secho(f"Error saving changes to local file {state.db_file}: {e}", fg=typer.colors.RED)
        typer.secho("Database state in memory might be inconsistent with the file.", fg=typer.colors.YELLOW)
        raise typer.Exit(code=1)


def _send_authed_remote_command(client: SocketClient, action: str, params: Optional[Dict[str, Any]] = None) -> dict:
    """
    Sends a command to the remote server, automatically including the
    session token if the user is logged in via the CLI. Handles common errors.
    """
    command_params = params.copy() if params else {}

    # Inject session token if available in state
    if state.session_token:
        command_params['session_token'] = state.session_token
        # typer.echo(f"DEBUG: Sending with session token: {state.session_token[:8]}...") # Optional debug

    command = {"action": action, "params": command_params}
    try:
        result = client.send_command(command)

        # Check for permission errors specifically if possible
        if result.get("status") == "error" and "permission" in result.get("message", "").lower():
             typer.secho(f"Permission Denied: {result.get('message')}", fg=typer.colors.RED)
             typer.secho("Hint: Ensure you are logged in ('auth login') with sufficient privileges.", fg=typer.colors.YELLOW)
             raise typer.Exit(code=1)
        # Check for other errors
        elif result.get("status") == "error":
            typer.secho(f"Server Error: {result.get('message')}", fg=typer.colors.RED)
            raise typer.Exit(code=1)

        return result # Return successful result
    except typer.Exit:
        raise # Don't capture typer.Exit exceptions
    except Exception as e:
        # Handle unexpected communication errors
        typer.secho(f"Communication Error: Failed to send/receive command '{action}'. {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


def get_connection(ctx: typer.Context):
    """Gets or establishes the database connection based on CLI options."""
    # Avoid reconnecting if already connected
    if state.connection:
        return state.connection, state.connection_type

    if state.host and state.port:
        # --- Remote Connection ---
        typer.echo(f"Connecting to remote server {state.host}:{state.port}...")
        client = SocketClient(host=state.host, port=state.port)
        try:
            ping_result = client.ping() # Use the client's ping method
            if ping_result.get("status") != "success":
                 # Handle specific connection refused error
                 if "Connection refused" in ping_result.get("message", ""):
                      typer.secho(f"Error: Connection refused by server at {state.host}:{state.port}. Is the server running?", fg=typer.colors.RED)
                 else:
                      typer.secho(f"Error: Could not connect to server at {state.host}:{state.port}. "
                                  f"Message: {ping_result.get('message', 'Unknown error')}", fg=typer.colors.RED)
                 raise typer.Exit(code=1)

            typer.secho(f"Connected to server (Ping: {ping_result.get('response_time', 0)*1000:.2f}ms).", fg=typer.colors.GREEN)
            state.connection = client
            state.connection_type = 'remote'
        except Exception as e:
             typer.secho(f"Failed to connect or ping server at {state.host}:{state.port}: {e}", fg=typer.colors.RED)
             raise typer.Exit(code=1)


    elif state.db_file:
        # --- Local File Connection ---
        if not state.db_file.exists():
            typer.secho(f"Error: Database file not found: {state.db_file}", fg=typer.colors.RED)
            raise typer.Exit(code=1)
        try:
            typer.echo(f"Loading database from file: {state.db_file}...")
            # Pass user/pass for Storage.load which handles internal login/auth check
            db: Database = Storage.load(str(state.db_file), user=state.user, password=state.password)

            # **Crucial Check**: Verify if auth was needed and successful during load
            if db._is_auth_required() and not db.active_session:
                 # This means users exist, credentials *might* have been provided but were wrong,
                 # or credentials were *not* provided when they were needed.
                 if state.user:
                     # Credentials were provided but failed
                     typer.secho(f"Error: Failed to authenticate user '{state.user}' during database load. Check credentials.", fg=typer.colors.RED)
                 else:
                     # Credentials were required but not provided
                      typer.secho("Error: This database requires authentication. Please provide --user and --password.", fg=typer.colors.RED)
                 raise typer.Exit(code=1) # Exit because the DB state is not properly authenticated

            # If auth was required, state.user MUST have been provided and was correct
            auth_msg = f" (Authenticated as '{state.user}')" if db.active_session else ""
            typer.secho(f"Database loaded successfully.{auth_msg}", fg=typer.colors.GREEN)

            state.connection = db
            state.connection_type = 'local'
        except PermissionError as pe: # Catch specific permission errors during load
            typer.secho(f"Error loading database file {state.db_file}: Permission Denied.", fg=typer.colors.RED)
            typer.secho(f"Detail: {pe}", fg=typer.colors.RED)
            typer.secho("Hint: If this database has users, you must provide valid --user and --password.", fg=typer.colors.YELLOW)
            raise typer.Exit(code=1)
        except ValueError as ve: # Catch other load errors like bad passwords if Storage.load raises ValueError
             typer.secho(f"Error loading database file {state.db_file}: {ve}", fg=typer.colors.RED)
             if "password" in str(ve).lower() or "credentials" in str(ve).lower(): # Give a hint for password errors
                 typer.secho("Hint: Check user credentials (--user, --password).", fg=typer.colors.YELLOW)
             raise typer.Exit(code=1)
        except (json.JSONDecodeError, zlib.error) as decode_error: # Catch file format errors
             typer.secho(f"Error loading database file {state.db_file}: Invalid file format or corrupted data.", fg=typer.colors.RED)
             typer.secho(f"Detail: {decode_error}", fg=typer.colors.RED)
             typer.secho("Hint: Ensure the file is a valid .segadb file and check if compression/encryption keys (--key) are needed/correct.", fg=typer.colors.YELLOW)
             raise typer.Exit(code=1)
        except Exception as e: # Catch all other generic load errors
            typer.secho(f"Error loading database file {state.db_file}: An unexpected error occurred: {e}", fg=typer.colors.RED)
            # Optionally print traceback for debugging unexpected errors
            # traceback.print_exc()
            raise typer.Exit(code=1)
    else:
        # If no connection method is specified when a command needing one is run
        if ctx.invoked_subcommand:
             typer.secho("Error: No connection specified. Use --db-file or --host/--port.", fg=typer.colors.RED)
             raise typer.Exit(code=1)
        # If just running the base command (which shows help), don't exit
        return None, None


    return state.connection, state.connection_type

# --- Typer Callback for Global Options ---
@app.callback()
def main_callback(
    ctx: typer.Context,
    db_file: Optional[Path] = typer.Option(
        None, "--db-file", "-f",
        help="Path to the .segadb database file for local operations.",
        exists=False, # Let get_connection handle existence check for better error msg
        dir_okay=False,
        resolve_path=True,
    ),
    host: Optional[str] = typer.Option(None, "--host", "-H", help="Hostname or IP address of the running SegaDB server."),
    port: Optional[int] = typer.Option(None, "--port", "-P", help="Port number of the running SegaDB server."),
    user: Optional[str] = typer.Option(
        None, 
        "--user", 
        "-u", 
        help="Username for authentication (needed for local load if DB has users). Uses SEGADB_USER env var if set.",
        envvar="SEGADB_USER",
    ),
    password: Optional[str] = typer.Option(
        None,
        "--password",
        "-p",
        help="Password for authentication (needed for local load if DB has users). Uses SEGADB_PASSWORD env var if set.",
        envvar="SEGADB_PASSWORD",
        # hide_input=True, # Can cause issues in some terminals/scripts
    ),
):
    """
    SegaDB Command-Line Interface.

    Connect using --db-file for local operations OR --host and --port for remote.
    Use 'auth login' for remote server authentication before running restricted commands.
    """
    # Store initial options in state
    state.db_file = db_file
    state.host = host
    state.port = port
    state.user = user
    state.password = password

    # Basic validation
    if db_file and host:
        typer.secho("Error: Cannot use both --db-file and --host simultaneously.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    if (host and not port) or (port and not host):
        typer.secho("Error: Both --host and --port are required for remote connection.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

# --- Top-Level Commands ---
@app.command()
def info(ctx: typer.Context):
    """Display information about the connected database."""
    conn, conn_type = get_connection(ctx) # Establish connection if not already done

    if conn_type == 'local':
        db: Database = conn
        typer.echo("\n--- Database Information (Local) ---")
        typer.echo(f"Name: {db.name}")
        typer.echo(f"File Path: {state.db_file}")
        typer.echo(f"Size (MB): {db.get_db_size() / (1024 * 1024):.4f}")
        auth_req = db._is_auth_required()
        typer.echo(f"Auth Required: {auth_req}")
        typer.echo(f"Authenticated User: {state.user if db.active_session else ('N/A - Not Required' if not auth_req else 'N/A - Login Failed/Missing')}")
        typer.echo(f"Tables: {len(db.tables)}")
        typer.echo(f"Views: {len(db.views)}")
        typer.echo(f"Materialized Views: {len(db.materialized_views)}")
        typer.echo(f"Stored Procedures: {len(db.stored_procedures)}")
        typer.echo(f"Triggers: {len(db.triggers['before']) + len(db.triggers['after'])}")
        typer.echo("------------------------------------")
    elif conn_type == 'remote':
        client: SocketClient = conn
        result = _send_authed_remote_command(client, "get_db_info") # Handles errors and auth
        typer.echo("\n--- Database Information (Remote) ---")
        db_info = result.get("data", {})
        for key, value in db_info.items():
             # Provide more context for remote session
             if key == "active_user" and state.session_token:
                 val_str = f"{value} (CLI Session Active)"
             elif key == "session_id" and state.session_token:
                 val_str = f"{value} (CLI Token: {state.session_token[:8]}...)"
             else:
                 val_str = str(value)
             typer.echo(f"{key.replace('_', ' ').title()}: {val_str}")
        typer.echo("-------------------------------------")

# --- Authentication Commands (Remote Only) ---
auth_app = typer.Typer(help="Manage remote server authentication.")
app.add_typer(auth_app, name="auth")

@auth_app.command("login")
def auth_login(
    ctx: typer.Context,
    username: str = typer.Argument(..., help="Username for remote login."),
    password: str = typer.Argument(..., help="Password for remote login. Uses SEGADB_LOGIN_PASSWORD env var if set.", envvar="SEGADB_LOGIN_PASSWORD"),#, hide_input=True),
):
    """Login to the remote SegaDB server to get a session token."""
    client = _ensure_remote(ctx) # Ensures connection is remote

    if state.session_token:
        typer.secho(f"Already logged in (Token: {state.session_token[:8]}...). Use 'auth logout' first to change users.", fg=typer.colors.YELLOW)
        raise typer.Exit()

    typer.echo(f"Attempting login for user '{username}'...")
    # Directly use client method, no token needed for login itself
    try:
        result = client.login_user(username, password)
    except Exception as e:
         typer.secho(f"Failed to send login command: {e}", fg=typer.colors.RED)
         raise typer.Exit(code=1)


    if result.get("status") == "success":
        state.session_token = result.get("session_token")
        if not state.session_token:
             typer.secho("Login succeeded but no session token received from server.", fg=typer.colors.RED)
             raise typer.Exit(code=1)
        typer.secho(f"Login successful. Session token obtained: {state.session_token[:8]}...", fg=typer.colors.GREEN)
    else:
        typer.secho(f"Login failed: {result.get('message')}", fg=typer.colors.RED)
        state.session_token = None # Ensure no stale token
        raise typer.Exit(code=1)

@auth_app.command("logout")
def auth_logout(ctx: typer.Context):
    """Logout from the remote SegaDB server."""
    client = _ensure_remote(ctx)

    if not state.session_token:
        typer.secho("Not currently logged in.", fg=typer.colors.YELLOW)
        raise typer.Exit()

    typer.echo("Attempting logout...")
    # Use helper to send current token for logout - handles errors
    result = _send_authed_remote_command(client, "logout_user")

    # result message handled by helper unless it's a success message
    typer.secho(f"Logout command sent (Server message: {result.get('message', 'Success')}).", fg=typer.colors.GREEN if result.get("status") == "success" else typer.colors.YELLOW)

    state.session_token = None # Clear token regardless of server response


# --- Table Commands ---
table_app = typer.Typer(help="Manage database tables.")
app.add_typer(table_app, name="table")

@table_app.command("list")
def table_list(ctx: typer.Context):
    """List all tables in the database."""
    conn, conn_type = get_connection(ctx)
    typer.echo("\n--- Tables ---")
    if conn_type == 'local':
        db = _ensure_local(ctx)
        if not db.tables:
            typer.echo("No tables found.")
        else:
            # Sort table names for consistent output
            for table_name in sorted(db.tables.keys()):
                record_count = len(db.tables[table_name].records)
                typer.echo(f"- {table_name} ({record_count} record{'s' if record_count != 1 else ''})")
    elif conn_type == 'remote':
        client = _ensure_remote(ctx)
        result = _send_authed_remote_command(client, "list_tables") # Handles errors
        tables = result.get("data", [])
        if not tables:
            typer.echo("No tables found.")
        else:
             # Could add record count via separate queries if really needed, but often slow
             # Sorting remote list for consistency
            for table_name in sorted(tables):
                typer.echo(f"- {table_name}")
    typer.echo("--------------")


@table_app.command("create")
def table_create(
    ctx: typer.Context,
    table_name: str = typer.Argument(..., help="Name of the table to create."),
    columns: str = typer.Argument(..., help="Comma-separated list of column names (e.g., 'id,name,email').")
):
    """Create a new table."""
    conn, conn_type = get_connection(ctx)
    column_list = [col.strip() for col in columns.split(',') if col.strip()]
    if not column_list:
        typer.secho("Error: No valid column names provided.", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    typer.echo(f"Attempting to create table '{table_name}' with columns: {column_list}...")

    try:
        if conn_type == 'local':
            db = _ensure_local(ctx)
            if table_name in db.tables:
                 typer.secho(f"Error: Table '{table_name}' already exists.", fg=typer.colors.YELLOW)
                 raise typer.Exit(code=1)
            db.create_table(table_name, column_list)
            _save_local_db(db) # Save changes
            typer.secho(f"Table '{table_name}' created locally and file saved.", fg=typer.colors.GREEN)
        elif conn_type == 'remote':
            client = _ensure_remote(ctx)
            params = {"table_name": table_name, "columns": column_list}
            result = _send_authed_remote_command(client, "create_table", params) # Handles errors
            typer.secho(f"Table '{table_name}' created successfully on server.", fg=typer.colors.GREEN)

    except typer.Exit:
         raise # Propagate exits from helpers/validation
    except Exception as e: # Catch other unexpected errors
        typer.secho(f"An unexpected error occurred during table creation: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@table_app.command("drop")
def table_drop(
    ctx: typer.Context,
    table_name: str = typer.Argument(..., help="Name of the table to drop."),
    force: bool = typer.Option(False, "--force", "-y", help="Skip confirmation prompt.")
):
    """Drop an existing table."""
    conn, conn_type = get_connection(ctx)

    if not force:
        warning = ""
        if table_name == "_users":
             warning = " Warning: This will remove all users and authentication!"
        typer.confirm(f"Are you sure you want to drop table '{table_name}'?{warning} This cannot be undone.", abort=True)

    typer.echo(f"Attempting to drop table '{table_name}'...")

    try:
        if conn_type == 'local':
            db = _ensure_local(ctx)
            if table_name not in db.tables:
                 typer.secho(f"Error: Table '{table_name}' does not exist.", fg=typer.colors.RED)
                 raise typer.Exit(code=1)
            if table_name == "_users": # Prevent dropping _users locally via CLI
                typer.secho("Error: Cannot drop the internal '_users' table via CLI for local files.", fg=typer.colors.RED)
                raise typer.Exit(code=1)
            del db.tables[table_name]
            _save_local_db(db)
            typer.secho(f"Table '{table_name}' dropped locally and file saved.", fg=typer.colors.GREEN)
        elif conn_type == 'remote':
            client = _ensure_remote(ctx)
            params = {"table_name": table_name}
            result = _send_authed_remote_command(client, "drop_table", params) # Handles errors
            typer.secho(f"Table '{table_name}' dropped successfully on server.", fg=typer.colors.GREEN)
    except typer.Exit:
         raise
    except Exception as e:
        typer.secho(f"An unexpected error occurred during table drop: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@table_app.command("query")
def table_query(
    ctx: typer.Context,
    table_name: str = typer.Argument(..., help="Name of the table to query."),
    filter_condition: Optional[str] = typer.Option(None, "--filter", "-F", help="Filter condition (remote server only, e.g., 'lambda r: r.data[\"price\"] > 100'). USE WITH CAUTION."),
    limit: int = typer.Option(20, "--limit", "-l", help="Maximum number of records to display.")
):
    """Query records from a table."""
    conn, conn_type = get_connection(ctx)

    if conn_type == 'local' and filter_condition:
        typer.secho("Warning: Filtering (--filter) is only supported for remote connections via CLI due to security.", fg=typer.colors.YELLOW)
        filter_condition = None

    typer.echo(f"Querying table '{table_name}'" + (f" with limit {limit}" if limit else ""))
    if filter_condition:
         typer.secho("Applying remote filter (ensure query is safe!)...", fg=typer.colors.YELLOW)


    try:
        if conn_type == 'local':
            db = _ensure_local(ctx)
            if table_name not in db.tables:
                typer.secho(f"Error: Table '{table_name}' not found locally.", fg=typer.colors.RED)
                raise typer.Exit(code=1)
            table = db.tables[table_name]
            typer.echo(f"\n--- Records from '{table_name}' (Local) ---")
            if not table.records:
                 typer.echo("Table is empty.")
            else:
                 # Use the table's pretty print method
                 table.print_table(limit=limit, pretty=True) # Assuming print_table exists and works
            typer.echo("------------------------------------------")

        elif conn_type == 'remote':
            client = _ensure_remote(ctx)
            params = {"table": table_name, "filter": filter_condition}
            result = _send_authed_remote_command(client, "query", params) # Handles errors
            typer.echo(f"\n--- Records from '{table_name}' (Remote) ---")
            records = result.get("data", [])
            # Attempt to get columns, might not be present in older server versions or filter results
            columns = result.get("columns")
            if not records:
                 typer.echo("No records found" + (" matching filter." if filter_condition else "."))
            else:
                 # Try to determine columns from first record if not provided by server
                 if not columns and records and isinstance(records[0], dict):
                     columns = list(records[0].keys())
                     # Try to remove internal ID if present for cleaner display
                     if '_record_id' in columns: columns.remove('_record_id')

                 if columns:
                     SocketUtilities.print_results(records, columns, limit=limit, offset=0)
                 else: # Fallback basic print if columns cannot be determined
                     typer.secho("Warning: Could not determine columns for formatted output. Printing raw records.", fg=typer.colors.YELLOW)
                     count = 0
                     for record in records:
                         if count >= limit:
                             typer.echo(f"-- Displaying {limit} of {len(records)} records --")
                             break
                         typer.echo(json.dumps(record)) # Print as JSON string
                         count += 1
            typer.echo("-------------------------------------------")
    except typer.Exit:
        raise
    except Exception as e:
        typer.secho(f"An unexpected error occurred during query: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

@table_app.command("insert")
def table_insert(
    ctx: typer.Context,
    table_name: str = typer.Argument(..., help="Name of the table to insert into."),
    data_json: str = typer.Argument(..., help="Record data as a JSON string (e.g., '{\"name\": \"Alice\", \"email\": \"a@b.com\"}')."),
    record_type_name: Optional[str] = typer.Option(
        None, "--type", "-t",
        help="Specify record type (e.g., ImageRecord, TextRecord). Default is standard Record."
    ),
):
    """Insert a new record into a table."""
    conn, conn_type = get_connection(ctx)

    try:
        record_data = json.loads(data_json)
        if not isinstance(record_data, dict):
            raise ValueError("Input must be a JSON object (dictionary).")
    except json.JSONDecodeError:
        typer.secho("Error: Invalid JSON data provided.", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    except ValueError as ve:
        typer.secho(f"Error: {ve}", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    typer.echo(f"Attempting to insert into table '{table_name}'" + (f" as type '{record_type_name}'" if record_type_name else "") + "...")

    try:
        if conn_type == 'local':
            db = _ensure_local(ctx)
            if table_name not in db.tables:
                 typer.secho(f"Error: Table '{table_name}' does not exist.", fg=typer.colors.RED)
                 raise typer.Exit(code=1)

            # --- Local Insert Specific Logic ---
            target_table = db.tables[table_name]

            # Determine Record Type for local insertion
            actual_record_type = Record # Default
            if record_type_name:
                # Find the type class within the segadb module
                try:
                    # This assumes types like ImageRecord are directly available via segadb.__all__
                    type_class = getattr(sys.modules['segadb'], record_type_name)
                    if not issubclass(type_class, Record):
                        raise AttributeError # Not a valid record type
                    actual_record_type = type_class
                except (AttributeError, KeyError):
                     typer.secho(f"Error: Invalid record type '{record_type_name}'. Must be a valid Record subclass in segadb.", fg=typer.colors.RED)
                     raise typer.Exit(code=1)

            # Basic Schema Check (only for standard Record type)
            if actual_record_type == Record:
                table_cols = set(target_table.columns)
                data_cols = set(record_data.keys())
                # Allow 'id' to be present in input, Table.insert handles it
                if 'id' in data_cols: data_cols.remove('id')
                if 'id' in table_cols: table_cols.remove('id')

                if data_cols != table_cols:
                     missing = table_cols - data_cols
                     extra = data_cols - table_cols
                     msg = "Error: Data columns mismatch table schema. "
                     if missing: msg += f"Missing: {missing}. "
                     if extra: msg += f"Extra: {extra}."
                     typer.secho(msg, fg=typer.colors.RED)
                     raise typer.Exit(code=1)

            # Perform the insert
            try:
                target_table.insert(record_data, record_type=actual_record_type)
                _save_local_db(db) # Save changes
                typer.secho(f"Record inserted locally into '{table_name}' and file saved.", fg=typer.colors.GREEN)
            except ValueError as insert_error: # Catch constraint errors etc.
                typer.secho(f"Error during local insert: {insert_error}", fg=typer.colors.RED)
                raise typer.Exit(code=1)
            #--- End Local Insert ---

        elif conn_type == 'remote':
            client = _ensure_remote(ctx)
            # Note: Remote insert currently doesn't support specifying record_type via CLI
            # The server-side _handle_command would need modification for this.
            if record_type_name:
                 typer.secho("Warning: Specifying record type (--type) is not currently supported for remote inserts via CLI.", fg=typer.colors.YELLOW)

            params = {"table": table_name, "record": record_data}
            result = _send_authed_remote_command(client, "insert", params) # Handles errors
            typer.secho(f"Record inserted successfully into '{table_name}' on server.", fg=typer.colors.GREEN)
    except typer.Exit:
        raise
    except Exception as e:
        typer.secho(f"An unexpected error occurred during insert: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


# --- User Commands ---
user_app = typer.Typer(help="Manage database users (Remote connection required).")
app.add_typer(user_app, name="user")

@user_app.command("create")
def user_create(
    ctx: typer.Context,
    username: str = typer.Argument(..., help="Username for the new user."),
    password: str = typer.Argument(..., help="Password for the new user. Uses SEGADB_CREATE_PASSWORD env var if set.", envvar="SEGADB_CREATE_PASSWORD"),#), hide_input=True),
    roles: str = typer.Option("read_only", "--roles", "-r", help="Comma-separated list of roles (e.g., 'admin,editor').")
):
    """Register a new user on the remote server."""
    client = _ensure_remote(ctx) # Ensures connection is remote

    role_list = [r.strip() for r in roles.split(',') if r.strip()]
    typer.echo(f"Attempting to register user '{username}' with roles {role_list}...")

    # Use helper - registration might require admin privileges itself
    params = {"username": username, "password": password, "roles": role_list}
    result = _send_authed_remote_command(client, "register_user", params) # Handles errors
    typer.secho(f"User '{username}' registered successfully.", fg=typer.colors.GREEN)


# TODO: Add user list, delete commands following the pattern (remote only)


# --- Backup Commands (Local Only) ---
backup_app = typer.Typer(help="Manage database backups (Only available for local files).")
app.add_typer(backup_app, name="backup")

@backup_app.command("create")
def backup_create(
    ctx: typer.Context,
    backup_dir: Optional[Path] = typer.Option(
        None, "--dir", "-d",
        help="Directory to save backup. Defaults to './backups_<dbname>'.",
        file_okay=False, resolve_path=True
    ),
    compress: bool = typer.Option(False, "--compress", "-c", help="Compress the backup file."),
    encrypt_key: Optional[str] = typer.Option(None, "--key", "-k", help="Base64 encoded key for encryption."),
    add_date: bool = typer.Option(False, "--date", help="Append date/time to backup filename."),
):
    """Create a backup of the local database file."""
    db = _ensure_local(ctx) # Ensures connection is local

    target_dir_str = str(backup_dir) if backup_dir else None # Storage.backup handles default logic using db.name

    typer.echo(f"Creating backup for database '{db.name}'...")
    try:
        # Storage.backup handles directory creation and filename generation
        result_msg = Storage.backup(db, key=encrypt_key, compress=compress, dir=target_dir_str, date=add_date)
        typer.secho(result_msg, fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error creating backup: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@backup_app.command("list")
def backup_list(
    ctx: typer.Context,
    backup_dir: Optional[Path] = typer.Option(
        None, "--dir", "-d",
        help="Directory to list backups from. Defaults to './backups_<dbname>'.",
        exists=True, file_okay=False, resolve_path=True
    ),
):
    """List available backups in a specified or default directory."""
    target_dir_path: Optional[Path] = backup_dir
    db_name_hint = ""

    # Determine target directory if not explicitly provided
    if not target_dir_path:
        try:
            db = _ensure_local(ctx) # Load DB to get name for default path
            db_name_hint = db.name
            target_dir_path = Path(os.getcwd()) / f"backups_{db.name}"
        except typer.Exit:
             typer.secho("Error: Cannot determine default backup directory. Please specify --backup-dir or use --db-file.", fg=typer.colors.RED)
             raise typer.Exit(code=1)
        except Exception as e:
             typer.secho(f"Error trying to determine default backup directory: {e}", fg=typer.colors.RED)
             raise typer.Exit(code=1)


    target_dir_str = str(target_dir_path)
    typer.echo(f"\n--- Backups in '{target_dir_str}'" + (f" (for DB: {db_name_hint})" if db_name_hint else "") + " ---")
    try:
        if not os.path.isdir(target_dir_str):
            typer.echo(f"Directory does not exist or is not accessible: {target_dir_str}")
            # Don't exit, just report no backups found in this case
        else:
            # Use Storage helper to list and sort
            backups = Storage.list_backups(target_dir_str, print_output=False)
            if not backups:
                typer.echo("No backup files (.segadb) found.")
            else:
                for backup_file in backups:
                    typer.echo(f"- {backup_file}")
    except Exception as e:
        typer.secho(f"Error listing backups in '{target_dir_str}': {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)
    typer.echo("---------------------------------" + ("-" * len(db_name_hint)) + "----")


# TODO: Add backup restore command


# --- Server Commands (Remote Only) ---
server_app = typer.Typer(help="Control the remote SegaDB server.")
app.add_typer(server_app, name="server")

@server_app.command("ping")
def server_ping(ctx: typer.Context):
    """Ping the remote server to check connectivity."""
    client = _ensure_remote(ctx) # Ensures connection is remote

    typer.echo(f"Pinging server {state.host}:{state.port}...")
    # Ping doesn't need auth token usually
    try:
        result = client.ping()
    except Exception as e:
         typer.secho(f"Failed to ping server: {e}", fg=typer.colors.RED)
         raise typer.Exit(code=1)

    if result.get("status") == "success":
        typer.secho(f"Pong! Server responded in {result.get('response_time', 0)*1000:.2f} ms.", fg=typer.colors.GREEN)
    else:
        # Handle specific connection refused error during ping attempt
         if "Connection refused" in result.get("message", ""):
              typer.secho(f"Error: Connection refused by server at {state.host}:{state.port}. Is the server running?", fg=typer.colors.RED)
         else:
              typer.secho(f"Ping failed: {result.get('message')}", fg=typer.colors.RED)
         raise typer.Exit(code=1)

@server_app.command("stop")
def server_stop(
    ctx: typer.Context,
    force: bool = typer.Option(False, "--force", "-y", help="Skip confirmation prompt.")
):
    """Stop the remote SegaDB server."""
    client = _ensure_remote(ctx) # Ensures connection is remote

    if not force:
        typer.confirm(f"Are you sure you want to stop the server at {state.host}:{state.port}?", abort=True)

    typer.echo(f"Sending stop command to server {state.host}:{state.port}...")
    # Use helper, stop might require privileges - handles errors
    result = _send_authed_remote_command(client, "stop")

    # Message handled by helper unless it's a success message
    typer.secho(f"Stop command sent (Server message: {result.get('message', 'Success')}).", fg=typer.colors.GREEN if result.get("status") == "success" else typer.colors.YELLOW)


# --- Main Execution ---
if __name__ == "__main__":
    # Consider adding a try-except block around app() for very top-level errors
    # although Typer usually handles internal command errors well.
    app()