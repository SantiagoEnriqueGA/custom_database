# SegaDB Command-Line Interface (segadb-cli)

A command-line tool built with [Typer](https://typer.tiangolo.com/) for interacting with SegaDB databases. This CLI allows you to manage database instances either by interacting directly with local `.segadb` files or by connecting to a running SegaDB server via its socket interface.

## Features

*   Connect to local `.segadb` files or a remote SegaDB server.
*   Get database information (`info`).
*   Manage tables: `list`, `create`, `drop`, `query`, `insert`.
*   Manage users (remote server only): `create`.
*   Handle remote server authentication: `auth login`, `auth logout`.
*   Manage backups (local files only): `backup create`, `backup list`.
*   Control remote server: `server ping`, `server stop`.
*   Uses existing `segadb` package logic (`Storage`, `SocketClient`, etc.).
*   Handles basic error reporting and confirmation prompts.

## Installation

1.  **SegaDB:** Ensure the `segadb` Python package itself is installed or accessible in your `PYTHONPATH`.
2.  **Dependencies:** Install the required libraries for the CLI:
    ```bash
    pip install "typer[all]"
    # rich is optional but recommended by typer for better output formatting
    # pip install rich
    ```
3.  **Script:** Place the `segadb_cli.py` script in your project directory or somewhere accessible.

## Usage

The CLI is invoked using `python segadb_cli.py`. You must specify a connection method:

*   **Local File:** Use `--db-file` (or `-f`) followed by the path to your `.segadb` file.
    ```bash
    python segadb_cli.py --db-file /path/to/my_database.segadb <COMMAND> [ARGS]...
    ```
    *   **Authentication (Local):** If the local database file contains users (is authentication-required), you **must** also provide `--user <username>` and `--password <password>` (or set `SEGADB_PASSWORD`) when running *any* command with `--db-file`. The CLI will attempt to authenticate during the database load process.

*   **Remote Server:** Use `--host` (or `-H`) and `--port` (or `-P`) to specify the running server address.
    ```bash
    python segadb_cli.py --host 127.0.0.1 --port 65432 <COMMAND> [ARGS]...
    ```
    *   **Authentication (Remote):** For commands that require privileges on the remote server (like creating tables, users, etc.), you first need to log in using the `auth login` command. This obtains a session token that the CLI will automatically use for subsequent commands in the same session.

**Getting Help:**

*   General help: `python segadb_cli.py --help`
*   Help for a specific command group (e.g., table): `python segadb_cli.py table --help`
*   Help for a specific command (e.g., table create): `python segadb_cli.py table create --help`

## Command Reference

*   `info`: Display information about the connected database.
*   **`auth`** (Remote Only)
    *   `login USERNAME PASSWORD`: Log in to the remote server.
    *   `logout`: Log out from the remote server.
*   **`table`**
    *   `list`: List tables in the database.
    *   `create TABLE_NAME COLUMNS`: Create a new table (e.g., `table create users "id,name,email"`).
    *   `drop TABLE_NAME [--force]`: Drop an existing table.
    *   `query TABLE_NAME [--filter FILTER] [--limit N]`: Query records from a table. `--filter` is remote only.
    *   `insert TABLE_NAME JSON_DATA [--type TYPE]`: Insert a record (e.g., `table insert users '{"name":"Bob"}'`). `--type` is local only.
*   **`user`** (Remote Only)
    *   `create USERNAME PASSWORD [--roles ROLES]`: Register a new user on the server.
*   **`backup`** (Local Only)
    *   `create [--dir DIR] [--compress] [--key KEY] [--date]`: Create a backup of the local DB file.
    *   `list [--dir DIR]`: List available backups.
*   **`server`** (Remote Only)
    *   `ping`: Check connectivity to the remote server.
    *   `stop [--force]`: Send a stop command to the remote server.

*Refer to `COMMAND --help` for detailed options.*

## Examples

**(Assumes `example_storage/database.segadb` exists and a server can be run from it)**

**Local File Operations:**

```bash
# Get info (assuming no auth needed or providing credentials)
python segadb_cli.py -f ./example_storage/database.segadb info
# OR if auth is required:
# export SEGADB_USER='admin'
# export SEGADB_PASSWORD='your_password'
# python segadb_cli.py -f ./example_storage/database.segadb -u admin info

# List tables
python segadb_cli.py -f ./example_storage/database.segadb table list

# Create a table (saves changes to the file!)
python segadb_cli.py -f ./example_storage/database.segadb table create products "product_id,name,price"

# Query the first 5 orders
python segadb_cli.py -f ./example_storage/database.segadb table query orders --limit 5

# Insert a record (saves changes!)
python segadb_cli.py -f ./example_storage/database.segadb table insert products '{"product_id": 101, "name": "Gadget", "price": 99.99}'

# Insert an ImageRecord locally (saves changes!)
python segadb_cli.py -f ./my_image_db.segadb table insert images '{"image_data":"/path/to/image.jpg"}' --type ImageRecord

# Create a compressed backup with timestamp
python segadb_cli.py -f ./example_storage/database.segadb backup create --compress --date

# List backups in the default directory (./backups_SampleDB)
python segadb_cli.py -f ./example_storage/database.segadb backup list
# Or specify directory
python segadb_cli.py backup list --dir ./my_backups

```

**Remote Server Operations:**

```bash
# --- Terminal 1: Launch the server ---
# python segadb/launch_server.py ./example_storage/database.segadb --user admin --password password123
# (Use appropriate user/pass if server requires it on startup)

# --- Terminal 2: Use the CLI ---

# Ping the server
python segadb_cli.py -H 127.0.0.1 -P 65432 server ping

# Get server info (might require login depending on server config)
# Attempt without login first
python segadb_cli.py -H 127.0.0.1 -P 65432 info

# Login (use correct credentials for the *remote* server)
# export SEGADB_LOGIN_PASSWORD='password123' # Set env var for password
python segadb_cli.py -H 127.0.0.1 -P 65432 auth login admin

# Now run commands requiring authentication
python segadb_cli.py -H 127.0.0.1 -P 65432 info # Should show active user now
python segadb_cli.py -H 127.0.0.1 -P 65432 table list

# Create a table remotely
python segadb_cli.py -H 127.0.0.1 -P 65432 table create remote_log "timestamp,message"

# Insert remotely
python segadb_cli.py -H 127.0.0.1 -P 65432 table insert remote_log '{"timestamp": "2023-10-27T10:00:00Z", "message": "System started"}'

# Query with filter (Use filter syntax expected by your server's _handle_command)
python segadb_cli.py -H 127.0.0.1 -P 65432 table query orders --filter "lambda r: r.data['user_id'] == 5" -l 10

# Create a new user remotely (likely requires admin privileges from login)
# export SEGADB_CREATE_PASSWORD='new_user_pass'
python segadb_cli.py -H 127.0.0.1 -P 65432 user create editor_user --roles "editor"

# Logout
python segadb_cli.py -H 127.0.0.1 -P 65432 auth logout

# Stop the server (likely requires admin privileges from login)
# (Re-login as admin if necessary first)
# python segadb_cli.py -H 127.0.0.1 -P 65432 auth login admin
python segadb_cli.py -H 127.0.0.1 -P 65432 server stop --force
```

## Authentication Notes
- **Local (--db-file):** Authentication (if required by the DB file) happens during the loading of the database file. You must provide --user and --password (or SEGADB_PASSWORD) alongside --db-file for every command if the database has users configured.
- **Remote (--host/--port):** Authentication is session-based. Use auth login once to obtain a session token. The CLI will then automatically use this token for subsequent commands requiring permissions. Use auth logout to clear the token.

## Environment Variables
- **SEGADB_USER:** Can be used instead of providing --user for local file authentication.
- **SEGADB_PASSWORD:** Can be used instead of providing --password for local file authentication.
- **SEGADB_LOGIN_PASSWORD:** Can be used instead of providing the password argument for auth login.
- **SEGADB_CREATE_PASSWORD:** Can be used instead of providing the password argument for user create.

## Important Caveats
- **Local File Saving:** When using --db-file, commands that modify the database (e.g., table create, table drop, table insert, backup create) will overwrite the entire database file on successful execution. This can be slow for large files and potentially risky. Use backups!
- **Remote Filter Security:** The --filter option for table query sends the filter string directly to the server. This is a potential security risk since the server is not configured to handle filters safely. For example, if the server uses eval-() to execute the filter, malicious input could potentially be executed on the server.
- **Remote Record Types:** Specifying --type for table insert is currently only supported for local operations. Remote insertion uses the server's default behavior.
