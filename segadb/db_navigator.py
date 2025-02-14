import curses
import functools
import logging
from typing import Dict, List, Any, Optional
from contextlib import contextmanager

# TODO: Color displayed code for views, mvs, stored procedures, and trigger functions

# Set up logging
logging.basicConfig(
    filename='segadb_error.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Key mapping for cross-platform compatibility
KEY_MAPPING = {
    'UP': [curses.KEY_UP, ord('w'), ord('W')],
    'DOWN': [curses.KEY_DOWN, ord('s'), ord('S')],
    'LEFT': [curses.KEY_LEFT, ord('a'), ord('A')],
    'RIGHT': [curses.KEY_RIGHT, ord('d'), ord('D')],
    'ENTER': [curses.KEY_ENTER, 10, 13],
    'QUIT': [ord('q'), ord('Q')],
    'REFRESH': [ord('r'), ord('R')],
    'HELP': [ord('?')],
    'SEARCH': [ord('/')],
    'PAGE_UP': [curses.KEY_PPAGE],
    'PAGE_DOWN': [curses.KEY_NPAGE]
}

def safe_execution(func):
    """Decorator to handle exceptions and log errors for functions."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}")
            if 'stdscr' in kwargs or (args and hasattr(args[0], 'addstr')):  # Check if curses screen is available
                display_popup(args[0], f"An error occurred: {str(e)}\nCheck logs for details.", 3)
            return None
    return wrapper

def is_key(key, key_type):
    """
    Check if the input key matches any of the mapped keys for the given key type.
    
    Args:
        key: The input key code
        key_type: The type of key to check ('UP', 'DOWN', 'LEFT', 'RIGHT', 'ENTER', 'QUIT')
    
        bool: True if the key matches any of the mapped keys, False otherwise
    Returns:
    """
    return key in KEY_MAPPING[key_type]

def safe_addstr(stdscr, y: int, x: int, text: str, attr=None):
    """Safely add a string to the screen, handling boundary errors."""
    height, width = stdscr.getmaxyx()
    if y < height and x < width:
        try:
            if attr:
                stdscr.addstr(y, x, text[:width-x], attr)
            else:
                stdscr.addstr(y, x, text[:width-x])
        except curses.error:
            pass
        
def remove_leading_spaces(code: str) -> str:
    """Remove leading spaces from each line of the given code."""
    lines = code.split("\n")
    if lines:
        leading_spaces = len(lines[0]) - len(lines[0].lstrip())
        lines = [line[leading_spaces:] for line in lines]
    return "\n".join(lines)

@safe_execution
def display_popup(stdscr, message: str, timeout: int = 0):
    """Display a centered popup message."""
    lines = message.split('\n')
    height = len(lines) + 4
    width = max(len(line) for line in lines) + 4
    
    screen_height, screen_width = stdscr.getmaxyx()
    start_y = (screen_height - height) // 2
    start_x = (screen_width - width) // 2
    
    popup = curses.newwin(height, width, start_y, start_x)
    popup.box()
    
    for i, line in enumerate(lines):
        safe_addstr(popup, i + 2, 2, line)

    popup.refresh()
    
    if timeout > 0:
        curses.napms(timeout * 1000)
    else:
        popup.getch()

def display_help(stdscr):
    """Display help information."""
    help_text = """
    Database Navigator Help
    ----------------------
    Navigation:
    ↑/w: Move up
    ↓/s: Move down
    ←/a: Go back
    →/d/Enter: Select
    
    Commands:
    q: Quit current view
    r: Refresh data
    ?: Show this help
    /: Search
    
    In Tables/Views/MVs:
    Page Up: Go to first page
    Page Down: Go to last page
    
    Press any key to close help
    """
    display_popup(stdscr, help_text)

@safe_execution
def search_prompt(stdscr, items: List[str]) -> Optional[int]:
    """Display search prompt and return index of matched item."""
    screen_height, screen_width = stdscr.getmaxyx()
    search_win = curses.newwin(3, 40, screen_height//2 - 1, screen_width//2 - 20)
    search_win.box()
    safe_addstr(search_win, 1, 2, "Search: ")
    search_win.refresh()
    
    curses.echo()
    curses.curs_set(1)

    search_str = ""
    while True:
        ch = search_win.getch()
        
        # If the user presses Enter, perform the search and return result
        if ch in [ord('\n'), curses.KEY_ENTER]:
            break
        # If the user presses ESC, exit the search
        elif ch == 27:  # ESC
            return None
        # Handle backspace
        elif ch in [curses.KEY_BACKSPACE, 127]:
            search_str = search_str[:-1]
        # Append character to search string
        else:
            search_str += chr(ch)

        # Update the UI
        search_win.clear()
        search_win.box()
        safe_addstr(search_win, 1, 2, f"Search: {search_str}")
        search_win.refresh()

    # Perform search **only after Enter is pressed**
    for i, item in enumerate(items):
        if search_str.lower() in item.lower():
            return i

    return None  # Return None if no match is found


@safe_execution
def db_navigator(stdscr, db):
    """
    Navigates through the database options using a curses-based interface.
    The function initializes the curses environment, sets up color pairs, and defines a menu with various database options.
    It then enters a main loop where it displays the main screen layout, handles user input to navigate through the menu,
    and calls the corresponding display functions based on the selected menu option.
    
    Args:
        stdscr: The curses window object.
        db: The database connection object.
    
    Menu Options:
        - "DB Info": Displays database information.
        - "View Tables": Displays the list of tables.
        - "View Views": Displays the list of views.
        - "View Materialized Views": Displays the list of materialized views.
        - "View Stored Procedures": Displays the list of stored procedures.
        - "View Trigger Functions": Displays the list of trigger functions.
    
    User Input:
        - 'q': Quit the navigator.
        - KEY_UP: Move the selection up.
        - KEY_DOWN: Move the selection down.
        - KEY_ENTER, 10, 13, KEY_RIGHT: Select the current menu option and display the corresponding information.
    """
    # Initialize curses
    curses.curs_set(0)
    curses.start_color()
    curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_WHITE)
    curses.init_pair(3, curses.COLOR_GREEN, curses.COLOR_WHITE)
    stdscr.clear()
    
    info_offset = 7
    
    # Define menu options and corresponding display functions
    menu_options = {
        "DB Info": display_db_info,
        "View Tables": display_tables,
        "View Views": display_views,
        "View Materialized Views": display_mv_views,
        "View Stored Procedures": display_stored_procedures,
        "View Trigger Functions": display_trigger_functions,
    }
    
    menu_list = list(menu_options.keys())
    current_row = 0
    
    while True:
        try:
            stdscr.clear()
            display_info(stdscr, db)
            display_main_screen(stdscr, menu_list, current_row, info_offset)
            
            key = stdscr.getch()
            
            if is_key(key, 'HELP'):
                display_help(stdscr)
            elif is_key(key, 'SEARCH'):
                # Remove "View " from the beginning of the menu list, except for the first item
                menu_list_noView = [item[5:] for item in menu_list]
                menu_list_noView[0] = menu_list[0]
                result = search_prompt(stdscr, menu_list_noView)
                if result is not None:
                    current_row = result
            elif is_key(key, 'REFRESH'):
                try:
                    # Refresh materialized views
                    for mv in db.materialized_views:
                        db.refresh_materialized_view(mv)
                    display_popup(stdscr, "Data refreshed successfully!", 1)
                except Exception as e:
                    logging.error(f"Error refreshing data: {e}")
                    display_popup(stdscr, f"Error refreshing data: {str(e)}", 2)
            elif is_key(key, 'QUIT'):
                break
            elif is_key(key, 'UP') and current_row > 0:
                current_row -= 1
            elif is_key(key, 'DOWN') and current_row < len(menu_list) - 1:
                current_row += 1
            elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
                menu_options[menu_list[current_row]](stdscr, db, info_offset)
                
        except curses.error as e:
            logging.error(f"Curses error: {e}")
            display_popup(stdscr, "Terminal size too small. Please resize.", 2)
                
@safe_execution
def display_info(stdscr, db):
    """
    Displays the database navigator information on the provided screen.

    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the name attribute to be displayed.
    """
    safe_addstr(stdscr, 0, 0, "Database Navigator: " + db.name)
    safe_addstr(stdscr, 1, 0, "-" * 60)
    safe_addstr(stdscr, 2, 0, "Navigation: Arrow keys or W/A/S/D keys")
    safe_addstr(stdscr, 3, 0, "Select: Enter or L key | Back/Quit: Q key or H key")
    safe_addstr(stdscr, 4, 0, "Help: ? | Search: / | Refresh: R")
    safe_addstr(stdscr, 5, 0, "-" * 60)

@safe_execution
def display_main_screen(stdscr, menu_list, selected_row_idx, info_offset):
    """
    Displays the main screen layout with the menu options and highlights the selected row.
    
    Args:
        stdscr: The curses window object where the main screen layout will be displayed.
        menu_list: A list of menu options to display.
        selected_row_idx: The index of the selected row in the menu list.
        info_offset: The vertical offset to display the menu options.
    """
    for idx, row in enumerate(menu_list):
        x = 0
        y = idx + info_offset
        if idx == selected_row_idx:
            stdscr.attron(curses.color_pair(1))
            safe_addstr(stdscr, y, x, row)
            stdscr.attroff(curses.color_pair(1))
        else:
            safe_addstr(stdscr, y, x, row)
    stdscr.refresh()

@safe_execution    
def display_db_info(stdscr, db, info_offset):
    """
    Displays the database information on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        info_offset: The vertical offset to display the database information.
    """
    # While loop to keep the screen open until the user exits
    while True:
        stdscr.clear()
        display_info(stdscr, db)
        safe_addstr(stdscr, info_offset + 1, 0, "Database Info:")
        safe_addstr(stdscr, info_offset + 2, 0, "-" * 80)
        safe_addstr(stdscr, info_offset + 3, 0, "Name:                   " + db.name)
        safe_addstr(stdscr, info_offset + 4, 0, "Database Size (MB):     {:.4f}".format(db.get_db_size() / (1024 * 1024)))
        safe_addstr(stdscr, info_offset + 5, 0, "Authorization Required: " + str(db._is_auth_required()))
        safe_addstr(stdscr, info_offset + 6, 0, "DB Users:               " + str(len(db.tables.get('_users').records)))
        safe_addstr(stdscr, info_offset + 7, 0, "Active User:            " + str(db.get_username_by_session(db.active_session)))
        safe_addstr(stdscr, info_offset + 8, 0, "Session ID:             " + str(db.active_session))
        safe_addstr(stdscr, info_offset + 9, 0, "-" * 80)
        safe_addstr(stdscr, info_offset + 10, 0, "    Tables:             " + str(len(db.tables)))
        safe_addstr(stdscr, info_offset + 11, 0, "    Views:              " + str(len(db.views)))
        safe_addstr(stdscr, info_offset + 12, 0, "    Materialized Views: " + str(len(db.materialized_views)))
        safe_addstr(stdscr, info_offset + 13, 0, "    Stored Procedures:  " + str(len(db.stored_procedures)))
        safe_addstr(stdscr, info_offset + 14, 0, "    Trigger Functions:  " + str(len(db.triggers)))
        stdscr.refresh()
        
        # Get user input, if 'q' is pressed, break
        key = stdscr.getch()        
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break

@safe_execution    
def display_tables(stdscr, db, info_offset):
    """
    Displays the list of tables in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        info_offset: The vertical offset to display the database information.
    """
    # While loop to keep the screen open until the user exits
    current_row = 0
    while True:
        # Display header and table information
        stdscr.clear()
        display_info(stdscr, db)
        safe_addstr(stdscr, info_offset + 1, 0, "Tables:")
        safe_addstr(stdscr, info_offset + 2, 0, "ID | Table Name")
        safe_addstr(stdscr, info_offset + 3, 0, "-" * 40)
        
        # For each table, display the table name with the selected row highlighted
        for i, table_name in enumerate(db.tables.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                safe_addstr(stdscr, y, x, str(i) + "  | " + table_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                safe_addstr(stdscr, y, x, str(i) + "  | " + table_name)
        stdscr.refresh()
        
        # Get user input, if 'q' is pressed, break
        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < len(db.tables) - 1:
            current_row += 1
        # If the user presses Enter or Right arrow key, display the table information
        elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
            table_name = list(db.tables.keys())[current_row]
            offset = info_offset + len(db.tables) + 6
            table = db.get_table(table_name)
            display_table(stdscr, table, table_name, offset)
        
@safe_execution
def display_table(stdscr, table, table_name, tables_offset):
    """
    Displays the table information and records on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        table: The table object containing the records to be displayed.
        table_name: The name of the table.
        tables_offset: The vertical offset to display the table information.
    """
    # Display table information
    safe_addstr(stdscr, tables_offset, 0, "Table: " + table_name)
    safe_addstr(stdscr, tables_offset + 1, 0, "-" * 40)
    safe_addstr(stdscr, tables_offset + 2, 0, "Row Count:    " + str(len(table.records)))
    if table.records: safe_addstr(stdscr, tables_offset + 3, 0, "Record Types: " + str(table.records[0]._type()))
    else: safe_addstr(stdscr, tables_offset + 3, 0, "Record Types: None")
    
    # Get and display the table constraints
    consts = []
    for constraint in table.constraints:
        if len(table.constraints[constraint]) == 1:
            consts.append(f"{constraint}: {table.constraints[constraint][0].__name__}")
    if consts: safe_addstr(stdscr, tables_offset + 4, 0, "Constraints:  " + ", ".join(consts))
    else: safe_addstr(stdscr, tables_offset + 4, 0, "Constraints:  None")
    
    safe_addstr(stdscr, tables_offset + 5, 0, "-" * 40)
    tables_offset += 4
    
    if not table.records:
        safe_addstr(stdscr, tables_offset + 2, 0, "--No records to display.--")
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_LEFT or key == ord('q'):
            return
    
    else:
        # Get max column width
        col_names = [col for col in table.columns]
        # For each row with col_names get the max width of the data
        col_widths = {col: max(len(str(record.data[col])) for record in table.records) for col in col_names}
        
        # Display the table records
        record_count = len(table.records)
        
        # Get max record limit (screen height - offset - header - footer)
        record_limit = 100
        record_limit = min(record_limit, stdscr.getmaxyx()[0] - tables_offset - 8)
        
        current_page = 0
        last_page = (record_count + record_limit - 1) // record_limit - 1
        
        # While loop to keep the screen open until the user exits
        while True:
            safe_addstr(stdscr, tables_offset + 2, 0, str(record_limit) + " records displayed per page. Page: " + str(current_page + 1) + " of " + str(last_page + 1))
            safe_addstr(stdscr, tables_offset + 3, 0, "-" * 80)
            
            # Display the column names
            x = 0
            y = tables_offset + 5
            for col in col_names:
                width = max(col_widths[col], len(col))
                safe_addstr(stdscr, y, x, col.ljust(width) + " | ")
                x += width + 3
                
            # Add a line separator with the column widths
            x = 0
            y = tables_offset + 6
            for col in col_names:
                width = max(col_widths[col], len(col))
                safe_addstr(stdscr, y, x, "-" * width + " | ")
                x += width + 3
            
                
            # Get the records for the current page and display them
            records = _get_record_page(table, current_page, record_limit)
            for i, record in enumerate(records):
                x = 0
                y = i + tables_offset + 7
                for col in col_names:
                    width = max(col_widths[col], len(col))
                    safe_addstr(stdscr, y, x, str(record.data[col]).ljust(width) + " | ")
                    x += width + 3
            
            stdscr.refresh()

            # Get user input, if 'q' is pressed, break
            key = stdscr.getch()
            if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
                break
            # If the user presses Up arrow key, move to the previous page
            elif is_key(key, 'UP') and current_page > 0:
                current_page -= 1
            # If the user presses Down arrow key, move to the next page
            elif is_key(key, 'DOWN') and current_page < (record_count + record_limit - 1) // record_limit - 1:
                current_page += 1
            elif is_key(key, 'PAGE_DOWN') and current_page < (record_count + record_limit - 1) // record_limit - 1:
                current_page = (record_count + record_limit - 1) // record_limit - 1
            elif is_key(key, 'PAGE_UP') and current_page > 0:
                current_page = 0
                
            # Clear only the table display area before refreshing
            stdscr.move(tables_offset + 2, 0)
            stdscr.clrtobot()

@safe_execution
def _get_record_page(table, page_num, page_size):
    """
    Get a page of records based on the page number and page size.
    
    Args:
        table: The table object containing the records.
        page_num: The page number to retrieve.
        page_size: The number of records per page.
    
    Returns:
        A list of records for the specified page.
    """
    # Calculate the start and end index for the page
    start_idx = page_num * page_size
    end_idx = min((page_num + 1) * page_size, len(table.records))
    return table.records[start_idx:end_idx]

@safe_execution
def display_views(stdscr, db, info_offset):
    """
    Displays the list of views in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        info_offset: The vertical offset to display the database information.
    """
    # While loop to keep the screen open until the user exits
    current_row = 0
    while True:
        # Display header and view information
        stdscr.clear()
        display_info(stdscr, db)
        safe_addstr(stdscr, info_offset + 1, 0, "Views:")
        safe_addstr(stdscr, info_offset + 2, 0, "ID | View Name")
        safe_addstr(stdscr, info_offset + 3, 0, "-" * 40)
        
        # For each view, display the view name with the selected row highlighted
        for i, view_name in enumerate(db.views.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                safe_addstr(stdscr, y, x, str(i) + "  | " + view_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                safe_addstr(stdscr, y, x, str(i) + "  | " + view_name)
        stdscr.refresh()
        
        # Get user input, if 'q' is pressed, break
        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < len(db.views) - 1:
            current_row += 1
        # If the user presses Enter or Right arrow key, display the view information
        elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
            # Add loading indicator
            safe_addstr(stdscr, info_offset + len(db.views) + 5, 0, "Loading view, please wait...")
            stdscr.refresh()
            
            view_name = list(db.views.keys())[current_row]
            offset = info_offset + len(db.views) + 6
            table = db.get_view(view_name).get_data()
            query = db.get_view(view_name)._query_to_string()
            
            # Remove loading indicator
            safe_addstr(stdscr, info_offset + len(db.views) + 5, 0, " " * 40)
            
            display_view(stdscr, table, view_name, query, offset)

@safe_execution
def display_view(stdscr, table, view_name, query, view_offset):
    """
    Displays the view information and records on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        table: The table object containing the records to be displayed.
        view_name: The name of the view.
        query: The query used to create the view.
        view_offset: The vertical offset to display the view information.
    """
    # Display view information
    safe_addstr(stdscr, view_offset, 0, "View: " + view_name)
    safe_addstr(stdscr, view_offset + 1, 0, "-" * 40)
    safe_addstr(stdscr, view_offset + 2, 0, "Row Count:    " + str(len(table.records)))
    if table.records: safe_addstr(stdscr, view_offset + 3, 0, "Record Types: " + str(table.records[0]._type()))
    else: safe_addstr(stdscr, view_offset + 3, 0, "Record Types: None")
    safe_addstr(stdscr, view_offset + 4, 0, "-" * 40)
    safe_addstr(stdscr, view_offset + 5, 0, "Query:")
    safe_addstr(stdscr, view_offset + 6, 0, remove_leading_spaces(query))
    
    # Display the query and increment the view offset
    query_lines = query.split("\n")
    view_offset += len(query_lines)
    safe_addstr(stdscr, view_offset + 5, 0, "-" * 40)
    view_offset += 4
    
    if not table.records:
        safe_addstr(stdscr, view_offset + 2, 0, "--No records to display.--")    
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_LEFT or key == ord('q'):
            return
    
    else:
        # Get max column width
        col_names = [col for col in table.columns]
        # For each row with col_names get the max width of the data
        col_widths = {col: max(len(str(record.data[col])) for record in table.records) for col in col_names}
        
        # Display the view records
        record_count = len(table.records)
        
        # Get max record limit (screen height - offset - header - footer)
        record_limit = 100
        record_limit = min(record_limit, stdscr.getmaxyx()[0] - view_offset - 8)
        
        current_page = 0
        last_page = (record_count + record_limit - 1) // record_limit - 1
        
        # While loop to keep the screen open until the user exits
        while True:
            safe_addstr(stdscr, view_offset + 2, 0, str(record_limit) + " records displayed per page. Page: " + str(current_page + 1) + " of " + str((last_page) + 1))
            safe_addstr(stdscr, view_offset + 3, 0, "-" * 80)
            
            # Display the column names
            x = 0
            y = view_offset + 5
            for col in col_names:
                width = max(col_widths[col], len(col))
                safe_addstr(stdscr, y, x, col.ljust(width) + " | ")
                x += width + 3
                
            # Add a line separator with the column widths
            x = 0
            y = view_offset + 6
            for col in col_names:
                width = max(col_widths[col], len(col))
                safe_addstr(stdscr, y, x, "-" * width + " | ")
                x += width + 3
            
                
            # Get the records for the current page and display them
            records = _get_record_page(table, current_page, record_limit)
            for i, record in enumerate(records):
                x = 0
                y = i + view_offset + 7
                for col in col_names:
                    width = max(col_widths[col], len(col))
                    safe_addstr(stdscr, y, x, str(record.data[col]).ljust(width) + " | ")
                    x += width + 3
            
            stdscr.refresh()
            
            # Get user input, if 'q' is pressed, break
            key = stdscr.getch()
            if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
                break
            # If the user presses Up arrow key, move to the previous page
            elif is_key(key, 'UP') and current_page > 0:
                current_page -= 1
            # If the user presses Down arrow key, move to the next page
            elif is_key(key, 'DOWN') and current_page < (record_count + record_limit - 1) // record_limit - 1:
                current_page += 1
            elif is_key(key, 'PAGE_DOWN') and current_page < (record_count + record_limit - 1) // record_limit - 1:
                current_page = (record_count + record_limit - 1) // record_limit - 1
            elif is_key(key, 'PAGE_UP') and current_page > 0:
                current_page = 0
            
            # Clear only the table display area before refreshing
            stdscr.move(view_offset + 2, 0)
            stdscr.clrtobot()

@safe_execution
def display_mv_views(stdscr, db, info_offset):
    """
    Displays the list of materialized views in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        info_offset: The vertical offset to display the database information
    """
    current_row = 0
    while True:
        stdscr.clear()
        display_info(stdscr, db)
        safe_addstr(stdscr, info_offset + 1, 0, "Materialized Views:")
        safe_addstr(stdscr, info_offset + 2, 0, "ID | MV View Name")
        safe_addstr(stdscr, info_offset + 3, 0, "-" * 40)
        for i, view_name in enumerate(db.materialized_views.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                safe_addstr(stdscr, y, x, str(i) + "  | " + view_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                safe_addstr(stdscr, y, x, str(i) + "  | " + view_name)
        stdscr.refresh()
        
        key = stdscr.getch()
        
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < len(db.materialized_views) - 1:
            current_row += 1
        elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
            view_name = list(db.materialized_views.keys())[current_row]
            offset = info_offset + len(db.materialized_views) + 6
            table = db.get_materialized_view(view_name).get_data()
            query = db.get_materialized_view(view_name)._query_to_string()
            display_mv_view(stdscr, table, view_name, query, offset)

@safe_execution
def display_mv_view(stdscr, table, view_name, query, view_offset):
    """
    Displays the materialized view information and records on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        table: The table object containing the records to be displayed.
        view_name: The name of the materialized view.
        query: The query used to create the materialized view.
        view_offset: The vertical offset to display the materialized view information.
    """
    safe_addstr(stdscr, view_offset, 0, "Materialized View: " + view_name)
    safe_addstr(stdscr, view_offset + 1, 0, "-" * 40)
    safe_addstr(stdscr, view_offset + 2, 0, "Row Count:    " + str(len(table.records)))
    if table.records: safe_addstr(stdscr, view_offset + 3, 0, "Record Types: " + str(table.records[0]._type()))
    else: safe_addstr(stdscr, view_offset + 3, 0, "Record Types: None")
    safe_addstr(stdscr, view_offset + 4, 0, "-" * 40)
    safe_addstr(stdscr, view_offset + 5, 0, "Query:")
    safe_addstr(stdscr, view_offset + 6, 0, remove_leading_spaces(query))
    
    query_lines = query.split("\n")
    view_offset += len(query_lines)
    safe_addstr(stdscr, view_offset + 5, 0, "-" * 40)
    view_offset += 4
    
    if not table.records:
        safe_addstr(stdscr, view_offset + 2, 0, "--No records to display.--")    
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_LEFT or key == ord('q'):
            return
    
    else:
        col_names = [col for col in table.columns]
        col_widths = {col: max(len(str(record.data[col])) for record in table.records) for col in col_names}

        record_count = len(table.records)
        
        # Get max record limit (screen height - offset - header - footer)
        record_limit = 100
        record_limit = min(record_limit, stdscr.getmaxyx()[0] - view_offset - 8)
        
        current_page = 0
        last_page = (record_count + record_limit - 1) // record_limit - 1
        
        while True:
            safe_addstr(stdscr, view_offset + 2, 0, str(record_limit) + " records displayed per page. Page: " + str(current_page + 1) + " of " + str(last_page + 1))
            safe_addstr(stdscr, view_offset + 3, 0, "-" * 80)
            
            # Display the column names
            x = 0
            y = view_offset + 5
            for col in col_names:
                width = max(col_widths[col], len(col))
                safe_addstr(stdscr, y, x, col.ljust(width) + " | ")
                x += width + 3
                
            # Add a line separator with the column widths
            x = 0
            y = view_offset + 6
            for col in col_names:
                width = max(col_widths[col], len(col))
                safe_addstr(stdscr, y, x, "-" * width + " | ")
                x += width + 3
            
                
            # Get the records for the current page and display them
            records = _get_record_page(table, current_page, record_limit)
            for i, record in enumerate(records):
                x = 0
                y = i + view_offset + 7
                for col in col_names:
                    width = max(col_widths[col], len(col))
                    safe_addstr(stdscr, y, x, str(record.data[col]).ljust(width) + " | ")
                    x += width + 3
            
            stdscr.refresh()
            key = stdscr.getch()
            
            if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
                break
            elif is_key(key, 'UP') and current_page > 0:
                current_page -= 1
            elif is_key(key, 'DOWN') and current_page < (record_count + record_limit - 1) // record_limit - 1:
                current_page += 1
            elif is_key(key, 'PAGE_DOWN') and current_page < (record_count + record_limit - 1) // record_limit - 1:
                current_page = (record_count + record_limit - 1) // record_limit - 1
            elif is_key(key, 'PAGE_UP') and current_page > 0:
                current_page = 0
            
            # Clear only the table display area before refreshing
            stdscr.move(view_offset + 2, 0)
            stdscr.clrtobot()

@safe_execution
def display_stored_procedures(stdscr, db, info_offset):
    """
    Displays the list of stored procedures in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        info_offset: The vertical offset to display the database information.
    """
    current_row = 0
    while True:
        stdscr.clear()
        display_info(stdscr, db)
        safe_addstr(stdscr, info_offset + 1, 0, "Stored Procedures:")
        safe_addstr(stdscr, info_offset + 2, 0, "ID | Procedure Name")
        safe_addstr(stdscr, info_offset + 3, 0, "-" * 40)
        for i, procedure_name in enumerate(db.stored_procedures.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                safe_addstr(stdscr, y, x, str(i) + "  | " + procedure_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                safe_addstr(stdscr, y, x, str(i) + "  | " + procedure_name)
        stdscr.refresh()
        
        key = stdscr.getch()
        
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < len(db.stored_procedures) - 1:
            current_row += 1
        elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
            procedure_name = list(db.stored_procedures.keys())[current_row]
            offset = info_offset + len(db.stored_procedures) + 6
            procedure = db._stored_procedure_to_string(db.get_stored_procedure(procedure_name))
            display_procedure(stdscr, procedure, procedure_name, offset)
            

@safe_execution
def display_procedure(stdscr, procedure, procedure_name, proc_offset):
    """
    Displays the stored procedure information on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        procedure: The stored procedure code to be displayed.
        procedure_name: The name of the stored procedure.
        proc_offset: The vertical offset to display the stored procedure information
    """
    safe_addstr(stdscr, proc_offset, 0, "Procedure: " + procedure_name)
    safe_addstr(stdscr, proc_offset + 1, 0, "Code:")

    # Remove leading spaces
    procedure = remove_leading_spaces(procedure)
    stdscr.addstr(proc_offset + 2, 0, procedure)
    
    code_lines = procedure.split("\n")
    proc_offset += len(code_lines)
    
    safe_addstr(stdscr, proc_offset + 1, 0, "-" * 40)
    
    stdscr.refresh()
    
    key = stdscr.getch()
    
    if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
        return

@safe_execution
def display_trigger_functions(stdscr, db, info_offset):
    """
    Displays the list of trigger functions in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        info_offset: The vertical offset to display the database information.
    """
    current_row = 0
    while True:
        stdscr.clear()
        display_info(stdscr, db)
        safe_addstr(stdscr, info_offset + 1, 0, "Trigger Functions:")
        safe_addstr(stdscr, info_offset + 2, 0, "ID | Trigger Type | Parent Function Name")
        safe_addstr(stdscr, info_offset + 3, 0, "-" * 40)
        trigger_list = []
        for trigger_type in db.triggers:
            for function_name in db.triggers[trigger_type]:
                trigger_list.append((trigger_type, function_name))
        
        for i, (trigger_type, function_name) in enumerate(trigger_list):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                if trigger_type == "before": safe_addstr(stdscr, y, x, str(i) + " | " + trigger_type + " | " + function_name)
                if trigger_type == "after": safe_addstr(stdscr, y, x, str(i) + " | " + trigger_type + "  | " + function_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                if trigger_type == "before": safe_addstr(stdscr, y, x, str(i) + " | " + trigger_type + " | " + function_name)
                if trigger_type == "after": safe_addstr(stdscr, y, x, str(i) + " | " + trigger_type + "  | " + function_name)
        stdscr.refresh()
        
        key = stdscr.getch()
        
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < len(trigger_list) - 1:
            current_row += 1
        elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
            trigger_type, function_name = trigger_list[current_row]
            offset = info_offset + len(trigger_list) + 6
            trigger = db.triggers[trigger_type][function_name][0]
            function = db._stored_procedure_to_string(trigger)
            display_function(stdscr, function, function_name, offset)

@safe_execution
def display_function(stdscr, function, function_name, func_offset):
    """
    Displays the trigger function information on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        function: The trigger function code to be displayed.
        function_name: The name of the trigger function.
        func_offset: The vertical offset to display the trigger function information.
    """
    safe_addstr(stdscr, func_offset, 0, "Function: " + function_name)
    safe_addstr(stdscr, func_offset + 1, 0, "Code:")
    
    # Remove leading spaces
    function = remove_leading_spaces(function)
    stdscr.addstr(func_offset + 2, 0, function)
    
    code_lines = function.split("\n")
    func_offset += len(code_lines)
    
    safe_addstr(stdscr, func_offset + 1, 0, "-" * 40)
    
    stdscr.refresh()
    
    key = stdscr.getch()
    
    if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
        return
