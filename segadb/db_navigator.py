import curses

# Define key constants for cross-platform compatibility
KEY_MAPPING = {
    'UP': [curses.KEY_UP, ord('w'), ord('W')],
    'DOWN': [curses.KEY_DOWN, ord('s'), ord('S')],
    'LEFT': [curses.KEY_LEFT, ord('a'), ord('A')],
    'RIGHT': [curses.KEY_RIGHT, ord('d'), ord('D')],
    'ENTER': [curses.KEY_ENTER, 10, 13],
    'QUIT': [ord('q'), ord('Q')]
}

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
        # Add more options here
    }
    
    menu_list = list(menu_options.keys())
    
    # Main loop
    current_row = 0
    while True:
        # Display main screen layout
        stdscr.clear()
        display_info(stdscr, db)
        display_main_screen(stdscr, menu_list, current_row, info_offset)
        
        # Get user input
        key = stdscr.getch()
        
        # Handle user input using the key mapping
        if is_key(key, 'QUIT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < len(menu_list) - 1:
            current_row += 1
        elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
            menu_options[menu_list[current_row]](stdscr, db, info_offset)

def display_info(stdscr, db):
    """
    Displays the database navigator information on the provided screen.

    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the name attribute to be displayed.
    """
    stdscr.addstr(0, 0, "Database Navigator: "+ db.name)
    stdscr.addstr(1, 0, "-" * 60)
    stdscr.addstr(2, 0, "Use the arrow keys to navigate, up and down, left to go back")
    stdscr.addstr(3, 0, "Press Enter to select an option")
    stdscr.addstr(4, 0, "Press 'q' for back or to quit")
    stdscr.addstr(5, 0, "-" * 60)

def display_main_screen(stdscr, menu_list, selected_row_idx, info_offset):
    """
    Displays the main screen layout with the menu options and highlights the selected row.
    
    Args:
        stdscr: The curses window object where the main screen layout will be displayed.
        menu_list: A list of menu options to display.
        selected_row_idx: The index of the selected row in the menu list.
        info_offset: The vertical offset to display the menu options.
    """
    # For each menu option, display the option with the selected row highlighted
    for idx, row in enumerate(menu_list):
        x = 0
        y = idx + info_offset
        # If the row is selected, highlight it with a color pair
        if idx == selected_row_idx:
            stdscr.attron(curses.color_pair(1))
            stdscr.addstr(y, x, row)
            stdscr.attroff(curses.color_pair(1))
        # Else, display the row without highlighting
        else:
            stdscr.addstr(y, x, row)
    stdscr.refresh()
    
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
        stdscr.addstr(info_offset + 1, 0, "Database Info:")
        stdscr.addstr(info_offset + 2, 0, "-" * 80)
        stdscr.addstr(info_offset + 3, 0, "Name:                   " + db.name)
        stdscr.addstr(info_offset + 4, 0, "Database Size (MB):     {:.4f}".format(db.get_db_size() / (1024 * 1024)))
        stdscr.addstr(info_offset + 5, 0, "Authorization Required: " + str(db._is_auth_required()))
        stdscr.addstr(info_offset + 6, 0, "DB Users:               " + str(len(db.tables.get('_users').records)))
        stdscr.addstr(info_offset + 7, 0, "Active User:            " + str(db.get_username_by_session(db.active_session)))
        stdscr.addstr(info_offset + 8, 0, "Session ID:             " + str(db.active_session))
        stdscr.addstr(info_offset + 9, 0, "-" * 80)
        
        stdscr.addstr(info_offset + 10, 0, "    Tables:             " + str(len(db.tables)))
        stdscr.addstr(info_offset + 11, 0, "    Views:              " + str(len(db.views)))
        stdscr.addstr(info_offset + 12, 0, "    Materialized Views: " + str(len(db.materialized_views)))
        stdscr.addstr(info_offset + 13, 0, "    Stored Procedures:  " + str(len(db.stored_procedures)))
        stdscr.addstr(info_offset + 14, 0, "    Trigger Functions:  " + str(len(db.triggers)))
        stdscr.refresh()
        
        # Get user input, if 'q' is pressed, break
        key = stdscr.getch()        
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
    
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
        stdscr.addstr(info_offset + 1, 0, "Tables:")
        stdscr.addstr(info_offset + 2, 0, "ID | Table Name")
        stdscr.addstr(info_offset + 3, 0, "-" * 40)
        
        # For each table, display the table name with the selected row highlighted
        for i, table_name in enumerate(db.tables.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                stdscr.addstr(y, x, str(i) + "  | " + table_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                stdscr.addstr(y, x, str(i) + "  | " + table_name)
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
    stdscr.addstr(tables_offset, 0, "Table: " + table_name)
    stdscr.addstr(tables_offset + 1, 0, "-" * 40)
    stdscr.addstr(tables_offset + 2, 0, "Row Count:    " + str(len(table.records)))
    if table.records: stdscr.addstr(tables_offset + 3, 0, "Record Types: " + str(table.records[0]._type()))
    else: stdscr.addstr(tables_offset + 3, 0, "Record Types: None")
    
    # Get and display the table constraints
    consts = []
    for constraint in table.constraints:
        if len(table.constraints[constraint]) == 1:
            consts.append(f"{constraint}: {table.constraints[constraint][0].__name__}")
    if consts: stdscr.addstr(tables_offset + 4, 0, "Constraints:  " + ", ".join(consts))
    else: stdscr.addstr(tables_offset + 4, 0, "Constraints:  None")
    
    stdscr.addstr(tables_offset + 5, 0, "-" * 40)
    tables_offset += 4
    
    if not table.records:
        stdscr.addstr(tables_offset + 2, 0, "--No records to display.--")
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
        record_limit = 10
        current_page = 0
        # While loop to keep the screen open until the user exits
        while True:
            stdscr.addstr(tables_offset + 2, 0, "Page: " + str(current_page + 1) + " of " + str((record_count + record_limit - 1) // record_limit))
            stdscr.addstr(tables_offset + 3, 0, "-" * 80)
            
            # Display the column names
            x = 0
            y = tables_offset + 5
            for col in col_names:
                width = max(col_widths[col], len(col))
                stdscr.addstr(y, x, col.ljust(width) + " | ")
                x += width + 3
                
            # Add a line separator with the column widths
            x = 0
            y = tables_offset + 6
            for col in col_names:
                width = max(col_widths[col], len(col))
                stdscr.addstr(y, x, "-" * width + " | ")
                x += width + 3
            
                
            # Get the records for the current page and display them
            records = _get_record_page(table, current_page, record_limit)
            for i, record in enumerate(records):
                x = 0
                y = i + tables_offset + 7
                for col in col_names:
                    width = max(col_widths[col], len(col))
                    stdscr.addstr(y, x, str(record.data[col]).ljust(width) + " | ")
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
        stdscr.addstr(info_offset + 1, 0, "Views:")
        stdscr.addstr(info_offset + 2, 0, "ID | View Name")
        stdscr.addstr(info_offset + 3, 0, "-" * 40)
        
        # For each view, display the view name with the selected row highlighted
        for i, view_name in enumerate(db.views.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                stdscr.addstr(y, x, str(i) + "  | " + view_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                stdscr.addstr(y, x, str(i) + "  | " + view_name)
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
        elif key == is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
            # TODO: Add loading indicator (visually indicate that the view is being loaded)
            view_name = list(db.views.keys())[current_row]
            offset = info_offset + len(db.views) + 6
            table = db.get_view(view_name).get_data()
            query = db.get_view(view_name)._query_to_string()
            display_view(stdscr, table, view_name, query, offset)

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
    stdscr.addstr(view_offset, 0, "View: " + view_name)
    stdscr.addstr(view_offset + 1, 0, "-" * 40)
    stdscr.addstr(view_offset + 2, 0, "Row Count:    " + str(len(table.records)))
    if table.records: stdscr.addstr(view_offset + 3, 0, "Record Types: " + str(table.records[0]._type()))
    else: stdscr.addstr(view_offset + 3, 0, "Record Types: None")
    stdscr.addstr(view_offset + 4, 0, "-" * 40)
    stdscr.addstr(view_offset + 5, 0, "Query:")
    stdscr.addstr(view_offset + 6, 0, query)
    
    # Display the query and increment the view offset
    query_lines = query.split("\n")
    view_offset += len(query_lines)
    stdscr.addstr(view_offset + 5, 0, "-" * 40)
    view_offset += 4
    
    if not table.records:
        stdscr.addstr(view_offset + 2, 0, "--No records to display.--")    
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
        record_limit = 10
        current_page = 0
        # While loop to keep the screen open until the user exits
        while True:
            stdscr.addstr(view_offset + 2, 0, "Page: " + str(current_page + 1) + " of " + str((record_count + record_limit - 1) // record_limit))
            stdscr.addstr(view_offset + 3, 0, "-" * 80)
            
            # Display the column names
            x = 0
            y = view_offset + 5
            for col in col_names:
                width = max(col_widths[col], len(col))
                stdscr.addstr(y, x, col.ljust(width) + " | ")
                x += width + 3
                
            # Add a line separator with the column widths
            x = 0
            y = view_offset + 6
            for col in col_names:
                width = max(col_widths[col], len(col))
                stdscr.addstr(y, x, "-" * width + " | ")
                x += width + 3
            
                
            # Get the records for the current page and display them
            records = _get_record_page(table, current_page, record_limit)
            for i, record in enumerate(records):
                x = 0
                y = i + view_offset + 7
                for col in col_names:
                    width = max(col_widths[col], len(col))
                    stdscr.addstr(y, x, str(record.data[col]).ljust(width) + " | ")
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
            
            # Clear only the table display area before refreshing
            stdscr.move(view_offset + 2, 0)
            stdscr.clrtobot()

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
        stdscr.addstr(info_offset + 1, 0, "Materialized Views:")
        stdscr.addstr(info_offset + 2, 0, "ID | MV View Name")
        stdscr.addstr(info_offset + 3, 0, "-" * 40)
        for i, view_name in enumerate(db.materialized_views.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                stdscr.addstr(y, x, str(i) + "  | " + view_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                stdscr.addstr(y, x, str(i) + "  | " + view_name)
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
            db.refresh_materialized_view(view_name)
            table = db.get_materialized_view(view_name).get_data()
            query = db.get_materialized_view(view_name)._query_to_string()
            display_mv_view(stdscr, table, view_name, query, offset)

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
    stdscr.addstr(view_offset, 0, "Materialized View: " + view_name)
    stdscr.addstr(view_offset + 1, 0, "-" * 40)
    stdscr.addstr(view_offset + 2, 0, "Row Count:    " + str(len(table.records)))
    if table.records: stdscr.addstr(view_offset + 3, 0, "Record Types: " + str(table.records[0]._type()))
    else: stdscr.addstr(view_offset + 3, 0, "Record Types: None")
    stdscr.addstr(view_offset + 4, 0, "-" * 40)
    stdscr.addstr(view_offset + 5, 0, "Query:")
    stdscr.addstr(view_offset + 6, 0, query)
    
    query_lines = query.split("\n")
    view_offset += len(query_lines)
    stdscr.addstr(view_offset + 5, 0, "-" * 40)
    view_offset += 4
    
    if not table.records:
        stdscr.addstr(view_offset + 2, 0, "--No records to display.--")    
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_LEFT or key == ord('q'):
            return
    
    else:
        col_names = [col for col in table.columns]
        col_widths = {col: max(len(str(record.data[col])) for record in table.records) for col in col_names}

        record_count = len(table.records)
        record_limit = 10
        current_page = 0
        while True:
            stdscr.addstr(view_offset + 2, 0, "Page: " + str(current_page + 1) + " of " + str((record_count + record_limit - 1) // record_limit))
            stdscr.addstr(view_offset + 3, 0, "-" * 80)
            
            # Display the column names
            x = 0
            y = view_offset + 5
            for col in col_names:
                width = max(col_widths[col], len(col))
                stdscr.addstr(y, x, col.ljust(width) + " | ")
                x += width + 3
                
            # Add a line separator with the column widths
            x = 0
            y = view_offset + 6
            for col in col_names:
                width = max(col_widths[col], len(col))
                stdscr.addstr(y, x, "-" * width + " | ")
                x += width + 3
            
                
            # Get the records for the current page and display them
            records = _get_record_page(table, current_page, record_limit)
            for i, record in enumerate(records):
                x = 0
                y = i + view_offset + 7
                for col in col_names:
                    width = max(col_widths[col], len(col))
                    stdscr.addstr(y, x, str(record.data[col]).ljust(width) + " | ")
                    x += width + 3
            
            stdscr.refresh()
            key = stdscr.getch()
            
            if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
                break
            elif is_key(key, 'UP') and current_page > 0:
                current_page -= 1
            elif is_key(key, 'DOWN') and current_page < (record_count + record_limit - 1) // record_limit - 1:
                current_page += 1
            
            # Clear only the table display area before refreshing
            stdscr.move(view_offset + 2, 0)
            stdscr.clrtobot()

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
        stdscr.addstr(info_offset + 1, 0, "Stored Procedures:")
        stdscr.addstr(info_offset + 2, 0, "ID | Procedure Name")
        stdscr.addstr(info_offset + 3, 0, "-" * 40)
        for i, procedure_name in enumerate(db.stored_procedures.keys()):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                stdscr.addstr(y, x, str(i) + "  | " + procedure_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                stdscr.addstr(y, x, str(i) + "  | " + procedure_name)
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
            
def display_procedure(stdscr, procedure, procedure_name, proc_offset):
    """
    Displays the stored procedure information on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        procedure: The stored procedure code to be displayed.
        procedure_name: The name of the stored procedure.
        proc_offset: The vertical offset to display the stored procedure information
    """
    stdscr.addstr(proc_offset, 0, "Procedure: " + procedure_name)
    stdscr.addstr(proc_offset + 1, 0, "Code:")
    stdscr.addstr(proc_offset + 2, 0, procedure)
    
    code_lines = procedure.split("\n")
    proc_offset += len(code_lines)
    
    stdscr.addstr(proc_offset + 1, 0, "-" * 40)
    
    stdscr.refresh()
    
    key = stdscr.getch()
    
    if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
        return
    
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
        stdscr.addstr(info_offset + 1, 0, "Trigger Functions:")
        stdscr.addstr(info_offset + 2, 0, "ID | Trigger Type | Parent Function Name")
        stdscr.addstr(info_offset + 3, 0, "-" * 40)
        trigger_list = []
        for trigger_type in db.triggers:
            for function_name in db.triggers[trigger_type]:
                trigger_list.append((trigger_type, function_name))
        
        for i, (trigger_type, function_name) in enumerate(trigger_list):
            x = 0
            y = i + info_offset + 4
            if i == current_row:
                stdscr.attron(curses.color_pair(1))
                if trigger_type == "before": stdscr.addstr(y, x, str(i) + " | " + trigger_type + " | " + function_name)
                if trigger_type == "after": stdscr.addstr(y, x, str(i) + " | " + trigger_type + "  | " + function_name)
                stdscr.attroff(curses.color_pair(1))
            else:
                if trigger_type == "before": stdscr.addstr(y, x, str(i) + " | " + trigger_type + " | " + function_name)
                if trigger_type == "after": stdscr.addstr(y, x, str(i) + " | " + trigger_type + "  | " + function_name)
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

def display_function(stdscr, function, function_name, func_offset):
    """
    Displays the trigger function information on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        function: The trigger function code to be displayed.
        function_name: The name of the trigger function.
        func_offset: The vertical offset to display the trigger function information.
    """
    stdscr.addstr(func_offset, 0, "Function: " + function_name)
    stdscr.addstr(func_offset + 1, 0, "Code:")
    stdscr.addstr(func_offset + 2, 0, function)
    
    code_lines = function.split("\n")
    func_offset += len(code_lines)
    
    stdscr.addstr(func_offset + 1, 0, "-" * 40)
    
    stdscr.refresh()
    
    key = stdscr.getch()
    
    if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
        return