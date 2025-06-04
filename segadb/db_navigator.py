import curses
import functools
import logging
from typing import Dict, List, Any, Optional
from contextlib import contextmanager
import pygments
from pygments.lexers import PythonLexer
from pygments.token import Token

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
            # Check if curses screen is available; args[0] is typically stdscr
            if args and hasattr(args[0], 'addstr') and hasattr(args[0], 'getmaxyx'): 
                # Ensure it's a window object before calling display_popup
                if isinstance(args[0], type(curses.initscr())):
                    # Check if stdscr is not None and usable
                    try:
                        args[0].getmaxyx() # A simple check to see if stdscr is valid
                        display_popup(args[0], f"An error occurred in {func.__name__}:\n{str(e)}\nCheck logs for details.", 3)
                    except Exception as popup_e:
                        logging.error(f"Error displaying popup in {func.__name__}: {popup_e}")
                else: # Fallback if stdscr is not a window object but in args
                    print(f"Error in {func.__name__}: {e}. (Curses popup not available)")
            else: # Fallback if stdscr is not available at all
                 print(f"Error in {func.__name__}: {e}. (Curses not available for popup)")
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
    if y < 0 or x < 0: # Prevent negative coordinates
        return
    if y < height and x < width:
        try:
            # Truncate text if it exceeds screen width from starting position x
            display_text = text[:width - x -1] if x + len(text) >= width else text
            if attr:
                stdscr.addstr(y, x, display_text, attr)
            else:
                stdscr.addstr(y, x, display_text)
        except curses.error as e:
            logging.warning(f"Curses error in safe_addstr at ({y},{x}) with text '{text[:20]}...': {e}")
            pass # Ignore curses errors, usually due to writing at edge
        
def remove_leading_spaces(code: str) -> str:
    """Remove leading spaces from each line of the given code."""
    lines = code.split("\n")
    if not lines:
        return ""
    
    # Find the minimum leading spaces in non-empty lines
    min_leading_spaces = float('inf')
    for line in lines:
        if line.strip(): # Only consider non-empty lines
            min_leading_spaces = min(min_leading_spaces, len(line) - len(line.lstrip()))
    
    if min_leading_spaces == float('inf'): # All lines are empty or whitespace
        return "\n".join(line.lstrip() for line in lines)

    # Remove the common leading spaces
    stripped_lines = [line[min_leading_spaces:] if len(line) >= min_leading_spaces else line for line in lines]
    return "\n".join(stripped_lines)

@safe_execution
def display_popup(stdscr, message: str, timeout: int = 0):
    """Display a centered popup message."""
    lines = message.split('\n')
    
    # Calculate required height and width for the popup
    popup_height = len(lines) + 4  # 2 for top/bottom padding, 2 for border
    popup_width = max(len(line) for line in lines) + 4  # 2 for left/right padding, 2 for border
    
    screen_height, screen_width = stdscr.getmaxyx()
    
    # Ensure popup dimensions are not larger than the screen
    popup_height = min(popup_height, screen_height)
    popup_width = min(popup_width, screen_width)

    if popup_height <= 4 or popup_width <= 4: # Too small to draw box and content
        logging.warning("Popup too small to render.")
        if timeout > 0:
            curses.napms(timeout * 1000)
        else:
            stdscr.getch() # wait for a key press if screen is too small
        return

    start_y = (screen_height - popup_height) // 2
    start_x = (screen_width - popup_width) // 2
    
    # Ensure start_y and start_x are not negative (can happen if screen is tiny)
    start_y = max(0, start_y)
    start_x = max(0, start_x)

    popup = curses.newwin(popup_height, popup_width, start_y, start_x)
    popup.box()
    
    for i, line in enumerate(lines):
        # Ensure text fits within the popup content area
        # Max line length is popup_width - 4 (2 for border, 2 for padding)
        # Max lines is popup_height - 4
        if i + 2 < popup_height - 2 : # Check if there's space for the line
            safe_addstr(popup, i + 2, 2, line[:popup_width - 4]) # Truncate line if too long

    popup.refresh()
    
    if timeout > 0:
        curses.flushinp() # Clear any pending input
        popup.timeout(timeout * 1000) # Set timeout for getch
        popup.getch() # Wait for timeout or key press
        popup.timeout(-1) # Reset timeout
    else:
        curses.flushinp()
        popup.getch() # Wait for any key
    del popup # Explicitly delete the window

def display_help(stdscr):
    """Display help information."""
    help_text = """
    Database Navigator Help
    ----------------------
    Navigation:
    ↑/w: Move up
    ↓/s: Move down
    ←/a: Go back / Exit current view
    →/d/Enter: Select / View details
    
    Commands:
    q: Quit current view / Quit application
    r: Refresh data (main screen)
    ?: Show this help
    /: Search (in lists)
    
    In Tables/Views/MVs (Record Display):
    Page Up:   Scroll to the first page
    Page Down: Scroll to the last page
    ↑/w: Scroll up (previous page)
    ↓/s: Scroll down (next page)
    
    Press any key to close help
    """
    display_popup(stdscr, help_text.strip())

@safe_execution
def search_prompt(stdscr, items: List[str]) -> Optional[int]:
    """Display search prompt and return index of matched item."""
    screen_height, screen_width = stdscr.getmaxyx()
    
    prompt_text = "Search: "
    win_height = 3
    win_width = max(40, len(prompt_text) + 20) # Ensure enough space for search term
    
    if screen_height < win_height or screen_width < win_width:
        display_popup(stdscr, "Screen too small for search.", 2)
        return None

    start_y = screen_height // 2 - win_height // 2
    start_x = screen_width // 2 - win_width // 2
    
    search_win = curses.newwin(win_height, win_width, start_y, start_x)
    search_win.box()
    safe_addstr(search_win, 1, 2, prompt_text)
    search_win.keypad(True) # Enable special keys like backspace
    search_win.refresh()
    
    curses.echo()
    curses.curs_set(1)
    search_win.move(1, 2 + len(prompt_text)) # Move cursor to after "Search: "

    search_str = ""
    max_input_len = win_width - len(prompt_text) - 4 # Account for borders and padding

    while True:
        try:
            ch = search_win.getch(1, 2 + len(prompt_text) + len(search_str))
        except curses.error: # Cursor out of bounds, shouldn't happen with max_input_len
            ch = -1 # No input

        if ch == 27:  # ESC key
            curses.noecho()
            curses.curs_set(0)
            del search_win
            return None
        elif ch in KEY_MAPPING['ENTER']:
            break
        elif ch in [curses.KEY_BACKSPACE, 127, 8]: # 8 is ASCII backspace
            if search_str:
                search_str = search_str[:-1]
        elif ch != -1 and 32 <= ch <= 126 and len(search_str) < max_input_len: # Printable ASCII
            search_str += chr(ch)
        
        # Update UI
        search_win.clear() # Clear previous content
        search_win.box()
        safe_addstr(search_win, 1, 2, f"{prompt_text}{search_str}")
        search_win.refresh()

    curses.noecho()
    curses.curs_set(0)
    del search_win

    if not search_str:
        return None

    for i, item in enumerate(items):
        if search_str.lower() in item.lower():
            return i
    
    display_popup(stdscr, f"No match found for '{search_str}'.", 2)
    return None


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
    curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE) # Selected item
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_WHITE)   # Error/Warning (not used here but good to have)
    curses.init_pair(3, curses.COLOR_GREEN, curses.COLOR_WHITE) # Success (not used here)
    stdscr.bkgd(' ', curses.color_pair(0)) # Set default background
    stdscr.keypad(True) # Enable special keys

    info_offset = 7  # Height of the display_info box
    navigation_offset = 0 # db_navigator directly uses info_offset for menu placement
    
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
            stdscr.clear() # Clear entire screen
            display_info(stdscr, db) # Display persistent header
            # display_main_screen handles its own clearing and drawing
            display_main_screen(stdscr, menu_list, current_row, info_offset) 
            stdscr.refresh() # Refresh the whole screen once
            
            key = stdscr.getch()
            
            if is_key(key, 'HELP'):
                display_help(stdscr)
            elif is_key(key, 'SEARCH'):
                # Prepare items for search (e.g., remove "View " prefix for better search experience)
                searchable_menu_list = [item.replace("View ", "") if item.startswith("View ") else item for item in menu_list]
                searchable_menu_list[0] = menu_list[0] # Keep "DB Info" as is or specific handling

                result_idx = search_prompt(stdscr, searchable_menu_list)
                if result_idx is not None:
                    current_row = result_idx
            elif is_key(key, 'REFRESH'):
                try:
                    if hasattr(db, 'materialized_views') and hasattr(db, 'refresh_materialized_view'):
                        refreshed_any = False
                        for mv_name in db.materialized_views.keys():
                            db.refresh_materialized_view(mv_name) # Assuming this is the method
                            refreshed_any = True
                        if refreshed_any:
                             display_popup(stdscr, "Materialized views refreshed successfully!", 2)
                        else:
                             display_popup(stdscr, "No materialized views to refresh.", 2)
                    else:
                        display_popup(stdscr, "Refresh not applicable or DB misconfigured.", 2)
                except Exception as e:
                    logging.error(f"Error refreshing data: {e}")
                    display_popup(stdscr, f"Error refreshing data: {str(e)}", 3)
            elif is_key(key, 'QUIT'):
                break
            elif is_key(key, 'UP') and current_row > 0:
                current_row -= 1
            elif is_key(key, 'DOWN') and current_row < len(menu_list) - 1:
                current_row += 1
            elif is_key(key, 'ENTER') or is_key(key, 'RIGHT'):
                if 0 <= current_row < len(menu_list):
                    selected_option_func = menu_options[menu_list[current_row]]
                    # The called function will handle its own screen area below display_info
                    selected_option_func(stdscr, db, info_offset) 
                
        except curses.error as e:
            logging.error(f"Curses error in db_navigator: {e}")
            # Check if screen size is the issue
            h, w = stdscr.getmaxyx()
            if h < 15 or w < 60 : # Minimum reasonable size
                 display_popup(stdscr, "Terminal size too small. Please resize.", 3)
            else:
                 display_popup(stdscr, f"A display error occurred: {e}", 3)
        except Exception as e: # Catch any other unexpected error
            logging.error(f"Unexpected error in db_navigator: {e}")
            display_popup(stdscr, f"An unexpected error occurred: {e}", 3)
                
@safe_execution
def display_info(stdscr, db):
    """
    Displays the database navigator information on the provided screen with a pretty box style.
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the name attribute to be displayed.
    """
    # Box-drawing border
    width = 60
    title = f" Database Navigator: {db.name} "
    border_top = "╭" + "─" * (width - 2) + "╮"
    border_bottom = "╰" + "─" * (width - 2) + "╯"
    stdscr.addstr(0, 0, border_top)
    stdscr.addstr(1, 0, "│" + title.center(width - 2) + "│")
    stdscr.addstr(2, 0, "├" + "─" * (width - 2) + "┤")
    stdscr.addstr(3, 0, "│ Navigation: ↑/↓/←/→ or W/A/S/D keys".ljust(width - 1) + "│")
    stdscr.addstr(4, 0, "│ Select: Enter or L | Back/Quit: Q or H".ljust(width - 1) + "│")
    stdscr.addstr(5, 0, "│ Help: ? | Search: / | Refresh: R".ljust(width - 1) + "│")
    stdscr.addstr(6, 0, border_bottom)

@safe_execution
def display_main_screen(stdscr, menu_list, selected_row_idx, start_y_offset):
    """
    Displays the main screen layout with the menu options and highlights the selected row, using a pretty box style.
    Args:
        stdscr: The curses window object where the main screen layout will be displayed.
        menu_list: A list of menu options to display.
        selected_row_idx: The index of the selected row in the menu list.
        start_y_offset: The vertical offset to display the menu options.
    """
    # Calculate box width based on the longest menu item
    if not menu_list:
        safe_addstr(stdscr, start_y_offset, 0, "No menu items to display.")
        return

    # Max length of menu items + padding for "  ITEM  " and borders "│ │"
    content_width = max(len(option) for option in menu_list)
    box_width = content_width + 6 # "│  " + "  │" = 6
    box_width = max(box_width, 20) # Minimum width for aesthetics

    box_height = len(menu_list) + 2 # +2 for top/bottom borders
    
    # stdscr.clrtobot() might not be needed if db_navigator clears screen.
    # Clear the area for the main screen menu first
    screen_height, screen_width = stdscr.getmaxyx()
    for y_line in range(start_y_offset, screen_height):
        safe_addstr(stdscr, y_line, 0, " " * (screen_width -1))


    # Draw top border
    safe_addstr(stdscr, start_y_offset, 0, '╭' + '─' * (box_width - 2) + '╮')
    
    # Draw menu options
    for idx, row_text in enumerate(menu_list):
        y_pos = start_y_offset + 1 + idx
        # Ensure text fits, pad to content_width
        display_text = f"  {row_text.ljust(content_width)}  " 
        full_line = f"│{display_text}│"

        if idx == selected_row_idx:
            safe_addstr(stdscr, y_pos, 0, full_line, curses.color_pair(1))
        else:
            safe_addstr(stdscr, y_pos, 0, full_line)
            
    # Draw bottom border
    safe_addstr(stdscr, start_y_offset + 1 + len(menu_list), 0, '╰' + '─' * (box_width - 2) + '╯')
    # No stdscr.refresh() here, db_navigator will do it.

@safe_execution    
def display_db_info(stdscr, db, base_offset):
    """
    Displays the database information on the provided screen in a visually appealing box layout.
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        base_offset: The vertical offset to display the database information.
    """
    width = 60
    box_left = 0
    current_y = base_offset

    # Clear area for this display component
    screen_height, screen_width = stdscr.getmaxyx()
    for y_line in range(base_offset, screen_height):
        safe_addstr(stdscr, y_line, 0, " " * (screen_width-1))

    # Main info box borders
    border_top = "╭" + "─" * (width - 2) + "╮"
    border_sep = "├" + "─" * (width - 2) + "┤"
    border_bottom = "╰" + "─" * (width - 2) + "╯"

    while True:
        # display_info(stdscr, db) is already called by db_navigator
        # This function should only draw its specific content below the main info header
        
        current_y = base_offset
        safe_addstr(stdscr, current_y, box_left, border_top); current_y += 1
        safe_addstr(stdscr, current_y, box_left, "│" + " DATABASE INFO ".center(width - 2) + "│"); current_y += 1
        safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
        
        # Info rows
        safe_addstr(stdscr, current_y, box_left, f"│ Name:          │ {db.name[:width-21].ljust(width-20)}│"); current_y += 1
        db_size_mb = db.get_db_size() / (1024 * 1024) if hasattr(db, 'get_db_size') else 0.0
        safe_addstr(stdscr, current_y, box_left, f"│ Size (MB):     │ {str('{:.4f}'.format(db_size_mb))[:width-21].ljust(width-20)}│"); current_y += 1
        
        is_auth_req = db._is_auth_required() if hasattr(db, '_is_auth_required') else 'N/A'
        safe_addstr(stdscr, current_y, box_left, f"│ Auth Required: │ {str(is_auth_req)[:width-21].ljust(width-20)}│"); current_y += 1
        
        num_users = len(db.tables.get('_users').records) if hasattr(db, 'tables') and db.tables.get('_users') else 'N/A'
        safe_addstr(stdscr, current_y, box_left, f"│ DB Users:      │ {str(num_users)[:width-21].ljust(width-20)}│"); current_y += 1
        
        active_user = db.get_username_by_session(db.active_session) if hasattr(db, 'get_username_by_session') else 'N/A'
        safe_addstr(stdscr, current_y, box_left, f"│ Active User:   │ {str(active_user)[:width-21].ljust(width-20)}│"); current_y += 1
        
        session_id = db.active_session if hasattr(db, 'active_session') else 'N/A'
        safe_addstr(stdscr, current_y, box_left, f"│ Session ID:    │ {str(session_id)[:width-21].ljust(width-20)}│"); current_y += 1
        
        safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
        
        # Section: Object counts
        safe_addstr(stdscr, current_y, box_left,  "│ Objects: ".ljust(width - 2) + " │"); current_y += 1 # Corrected ljust and added end pipe
        
        len_tables = len(db.tables) if hasattr(db, 'tables') else 0
        len_views = len(db.views) if hasattr(db, 'views') else 0
        len_mvs = len(db.materialized_views) if hasattr(db, 'materialized_views') else 0
        
        # Ensure object count line fits
        obj_line1 = f"│   Tables: {str(len_tables).ljust(5)} Views: {str(len_views).ljust(5)} MVs: {str(len_mvs).ljust(5)}"
        safe_addstr(stdscr, current_y, box_left, obj_line1.ljust(width - 2)[:width-2] + " │"); current_y += 1

        len_sp = len(db.stored_procedures) if hasattr(db, 'stored_procedures') else 0
        len_trig = len(db.triggers) if hasattr(db, 'triggers') else 0 # Assuming triggers is a dict like others
        
        obj_line2 = f"│   Stored Procs: {str(len_sp).ljust(5)} Triggers: {str(len_trig).ljust(5)}"
        safe_addstr(stdscr, current_y, box_left, obj_line2.ljust(width - 2)[:width-2] + " │"); current_y += 1
        
        safe_addstr(stdscr, current_y, box_left, border_bottom); current_y += 1
        
        # Footer
        safe_addstr(stdscr, current_y, box_left, "Press ← or Q to return...".ljust(width)); current_y += 2 # +1 for line, +1 for cursor move
        
        stdscr.refresh()
        
        key = stdscr.getch()        
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            # Clear this component's area before returning
            for y_line in range(base_offset, current_y +1): # +1 to clear the footer line too
                 safe_addstr(stdscr, y_line, 0, " " * (screen_width-1))
            break

@safe_execution    
def display_tables(stdscr, db, base_offset):
    """
    Displays the list of tables in the database on the provided screen with a pretty box style.
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        base_offset: The vertical offset to display the database information.
    """
    current_row = 0
    width = 48 
    box_left = 0
    
    while True:
        # Clear area for this display component (below the main info header)
        screen_height, screen_width = stdscr.getmaxyx()
        for y_line in range(base_offset, screen_height):
            safe_addstr(stdscr, y_line, 0, " " * (screen_width-1))

        table_names = list(db.tables.keys()) if hasattr(db, 'tables') else []
        count = len(table_names)
        
        current_list_y = base_offset

        # Draw box borders
        border_top = "╭" + "─" * (width - 2) + "╮"
        border_sep = "├" + "─" * (width - 2) + "┤"
        border_bottom = "╰" + "─" * (width - 2) + "╯"
        
        safe_addstr(stdscr, current_list_y, box_left, border_top); current_list_y +=1
        safe_addstr(stdscr, current_list_y, box_left, "│" + f" Tables ({count}) ".center(width - 2) + "│"); current_list_y +=1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y +=1
        safe_addstr(stdscr, current_list_y, box_left, "│ ID  │ Table Name".ljust(width - 2) + " │"); current_list_y +=1 # Adjusted ljust
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y +=1
        
        # Table rows
        # Calculate how many items can be displayed
        displayable_items_area_height = screen_height - current_list_y - 2 # -1 for bottom border, -1 for footer
        items_per_page = max(1, displayable_items_area_height) # Avoid 0 or negative
        
        start_index = 0 # For future pagination if needed, for now, show all or up to screen limit
        
        for i_display, i_actual in enumerate(range(start_index, count)):
            if i_display >= items_per_page: # Stop if we run out of screen space
                break
            
            table_name = table_names[i_actual]
            y_pos = current_list_y + i_display
            
            # Pad name to fit: width - (len("│ ") + len("ID") + len("  │ ") + len(" │"))
            # width - (2 + 2 + 3 + 2) = width - 9
            name_padding = width - 9 
            row_str = f"│ {str(i_actual).rjust(2)}  │ {table_name[:name_padding].ljust(name_padding)}│"

            if i_actual == current_row:
                safe_addstr(stdscr, y_pos, box_left, row_str, curses.color_pair(1))
            else:
                safe_addstr(stdscr, y_pos, box_left, row_str)
        
        current_list_y += min(count, items_per_page) # Move Y to after the last displayed item
        safe_addstr(stdscr, current_list_y, box_left, border_bottom); current_list_y +=1
        
        # Footer
        safe_addstr(stdscr, current_list_y, box_left, "Enter/→: View | Q/←: Back | ↑/↓: Nav".ljust(width)); current_list_y +=1
        stdscr.refresh()
        
        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break 
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < count - 1:
            current_row += 1
        elif (is_key(key, 'ENTER') or is_key(key, 'RIGHT')) and count > 0:
            if 0 <= current_row < count:
                table_name_selected = table_names[current_row]
                detail_offset = current_list_y + 1 # Start detail display below the list footer
                
                table_obj = db.get_table(table_name_selected)
                if table_obj:
                    display_table(stdscr, table_obj, table_name_selected, detail_offset)
                    # After display_table returns, the loop continues, redrawing the list
                else:
                    display_popup(stdscr, f"Could not load table: {table_name_selected}", 2)
        
@safe_execution
def display_table(stdscr, table, table_name, tables_offset):
    """
    Displays the table information and records on the provided screen in a pretty box style.
    Args:
        stdscr: The curses window object where the information will be displayed.
        table: The table object containing the records to be displayed.
        table_name: The name of the table.
        tables_offset: The vertical offset to display the table information.
    """
    width = 60
    box_left = 0
    border_top = "╭" + "─" * (width - 2) + "╮"
    border_sep = "├" + "─" * (width - 2) + "┤"
    border_bottom = "╰" + "─" * (width - 2) + "╯"
    # Info box
    stdscr.addstr(tables_offset, box_left, border_top)
    stdscr.addstr(tables_offset + 1, box_left, "│" + f" Table: {table_name} ".center(width - 2) + "│")
    stdscr.addstr(tables_offset + 2, box_left, border_sep)
    stdscr.addstr(tables_offset + 3, box_left, f"│  Row Count:    │ {str(len(table.records)).ljust(width-20)}│")
    if table.records:
        stdscr.addstr(tables_offset + 4, box_left, f"│  Record Types: │ {str(table.records[0]._type()).ljust(width-20)}│")
    else:
        stdscr.addstr(tables_offset + 4, box_left, f"│  Record Types: │ {'None'.ljust(width-21)}│")
    # Constraints
    consts = []
    for constraint in table.constraints:
        if len(table.constraints[constraint]) == 1:
            consts.append(f"{constraint}: {table.constraints[constraint][0].__name__}")
    stdscr.addstr(tables_offset + 5, box_left, border_sep)
    if consts:
        stdscr.addstr(tables_offset + 6, box_left, f"│  Constraints:  {', '.join(consts)[:width-17]}".ljust(width - 1) + "│")
    else:
        stdscr.addstr(tables_offset + 6, box_left, f"│  Constraints:  None".ljust(width - 1) + "│")
    stdscr.addstr(tables_offset + 7, box_left, border_bottom)
    # Footer
    stdscr.addstr(tables_offset + 8, box_left, "Press Q/← to return, ↑/↓ to scroll records".ljust(width))
    tables_offset += 8
    # Records section
    if not table.records:
        safe_addstr(stdscr, tables_offset, 0, "--No records to display.--")
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_LEFT or key == ord('q'):
            return
    else:
        col_names = [col for col in table.columns]
        col_widths = {col: max(len(str(record.data[col])) for record in table.records) for col in col_names}
        record_limit = 100
        record_limit = min(record_limit, stdscr.getmaxyx()[0] - tables_offset - 8)
        display_table_records(stdscr, table, col_names, col_widths, tables_offset, record_limit)

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
    start_idx = page_num * page_size
    end_idx = min((page_num + 1) * page_size, len(table.records))
    return table.records[start_idx:end_idx]

def display_table_records(stdscr, table, col_names, col_widths, offset, record_limit):
    """
    Helper to display paginated table records with navigation.
    Args:
        stdscr: The curses window object.
        table: The table-like object with .records and .columns.
        col_names: List of column names.
        col_widths: Dict of column widths.
        offset: Vertical offset to start rendering.
        record_limit: Max records per page.
    """
    record_count = len(table.records)
    current_page = 0
    last_page = (record_count + record_limit - 1) // record_limit - 1
    # Precompute column widths (add padding for aesthetics)
    col_pads = {col: max(col_widths[col], len(col)) for col in col_names}
    total_width = sum(col_pads[col] + 2 for col in col_names) + len(col_names) + 1
    while True:
        safe_addstr(stdscr, offset + 2, 0, f"{record_limit} records displayed per page. Page: {current_page + 1} of {last_page + 1}")
        # Draw top border
        x = 0
        y = offset + 3
        border = '╭'
        for idx, col in enumerate(col_names):
            border += '─' * (col_pads[col] + 2)
            if idx < len(col_names) - 1:
                border += '┬'
        border += '╮'
        safe_addstr(stdscr, y, x, border)
        # Draw header row
        y += 1
        row = '│'
        for col in col_names:
            row += ' ' + col.ljust(col_pads[col]) + ' │'
        safe_addstr(stdscr, y, x, row)
        # Draw header separator
        y += 1
        sep = '├'
        for idx, col in enumerate(col_names):
            sep += '─' * (col_pads[col] + 2)
            if idx < len(col_names) - 1:
                sep += '┼'
        sep += '┤'
        safe_addstr(stdscr, y, x, sep)
        # Draw records
        records = _get_record_page(table, current_page, record_limit)
        for i, record in enumerate(records):
            y += 1
            row = '│'
            for col in col_names:
                val = str(record.data[col])
                row += ' ' + val.ljust(col_pads[col]) + ' │'
            safe_addstr(stdscr, y, x, row)
        # Draw bottom border
        y += 1
        border = '╰'
        for idx, col in enumerate(col_names):
            border += '─' * (col_pads[col] + 2)
            if idx < len(col_names) - 1:
                border += '┴'
        border += '╯'
        safe_addstr(stdscr, y, x, border)
        stdscr.refresh()
        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_page > 0:
            current_page -= 1
        elif is_key(key, 'DOWN') and current_page < last_page:
            current_page += 1
        elif is_key(key, 'PAGE_DOWN') and current_page < last_page:
            current_page = last_page
        elif is_key(key, 'PAGE_UP') and current_page > 0:
            current_page = 0
        # Clear only the table display area before refreshing
        stdscr.move(offset + 2, 0)
        stdscr.clrtobot()

@safe_execution
def display_views(stdscr, db, base_offset):
    """
    Displays the list of views in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        base_offset: The vertical offset to display the database information.
    """
    current_row = 0
    width = 48  # Consistent width with display_tables
    box_left = 0

    while True:
        # Clear area for this display component
        screen_height, screen_width = stdscr.getmaxyx()
        for y_line in range(base_offset, screen_height):
            safe_addstr(stdscr, y_line, 0, " " * (screen_width -1))

        view_names = list(db.views.keys()) if hasattr(db, 'views') else []
        count = len(view_names)
        
        current_list_y = base_offset

        border_top = "╭" + "─" * (width - 2) + "╮"
        border_sep = "├" + "─" * (width - 2) + "┤"
        border_bottom = "╰" + "─" * (width - 2) + "╯"

        safe_addstr(stdscr, current_list_y, box_left, border_top); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, "│" + f" Views ({count}) ".center(width - 2) + "│"); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, "│ ID  │ View Name".ljust(width - 2) + " │"); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1

        displayable_items_area_height = screen_height - current_list_y - 2
        items_per_page = max(1, displayable_items_area_height)
        
        for i_display, i_actual in enumerate(range(count)): # Assuming no pagination for now for simplicity
            if i_display >= items_per_page:
                break
            view_name = view_names[i_actual]
            y_pos = current_list_y + i_display
            name_padding = width - 9
            row_str = f"│ {str(i_actual).rjust(2)}  │ {view_name[:name_padding].ljust(name_padding)}│"

            if i_actual == current_row:
                safe_addstr(stdscr, y_pos, box_left, row_str, curses.color_pair(1))
            else:
                safe_addstr(stdscr, y_pos, box_left, row_str)
        
        current_list_y += min(count, items_per_page)
        safe_addstr(stdscr, current_list_y, box_left, border_bottom); current_list_y += 1
        
        safe_addstr(stdscr, current_list_y, box_left, "Enter/→: View | Q/←: Back | ↑/↓: Nav".ljust(width)); current_list_y +=1
        stdscr.refresh()

        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < count - 1:
            current_row += 1
        elif (is_key(key, 'ENTER') or is_key(key, 'RIGHT')) and count > 0:
            if 0 <= current_row < count:
                selected_view_name = view_names[current_row]
                detail_offset = current_list_y + 1 # Start detail display below list footer
                
                # Optional: Loading message
                loading_msg_y = detail_offset -1 # Place it right before where detail view starts
                safe_addstr(stdscr, loading_msg_y, 0, "Loading view, please wait...".ljust(width))
                stdscr.refresh()

                try:
                    view_object = db.get_view(selected_view_name)
                    table_data = view_object.get_data() 
                    query_string = view_object._query_to_string()
                    
                    # Clear loading message before displaying view
                    safe_addstr(stdscr, loading_msg_y, 0, " " * width) 
                    stdscr.refresh() # Refresh to clear message

                    display_view(stdscr, table_data, selected_view_name, query_string, detail_offset)
                except Exception as e:
                    safe_addstr(stdscr, loading_msg_y, 0, " " * width) # Clear loading message on error too
                    logging.error(f"Error displaying view {selected_view_name}: {e}")
                    display_popup(stdscr, f"Error loading view '{selected_view_name}':\n{str(e)}", 3)
                    # Loop will continue, redrawing the list

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
    box_left = 0
    current_y = view_offset
    screen_height, screen_width = stdscr.getmaxyx()
    
    # Calculate width based on query lines
    query_lines = remove_leading_spaces(query).split("\n")
    max_query_width = max(len(line) for line in query_lines) if query_lines else 0
    
    # Minimum width for view info, max of screen width or query width
    min_width = max(len(f"View: {view_name}") + 4, len(f"Record Types: {table.records[0]._type() if table.records else 'None'}") + 4)
    width = min(max(min_width, max_query_width + 4), screen_width - 2)
    
    # Box drawing characters
    border_top = "╭" + "─" * (width - 2) + "╮"
    border_sep = "├" + "─" * (width - 2) + "┤"
    border_bottom = "╰" + "─" * (width - 2) + "╯"
    
    # View information box
    safe_addstr(stdscr, current_y, box_left, border_top); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│" + f" View: {view_name} ".ljust(width - 2) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    
    # Row count
    row_count_text = f"Row Count: {len(table.records)}"
    safe_addstr(stdscr, current_y, box_left, "│ " + row_count_text.ljust(width - 4) + " │"); current_y += 1
    
    # Record types
    if table.records:
        record_types_text = f"Record Types: {table.records[0]._type()}"
    else:
        record_types_text = "Record Types: None"
    safe_addstr(stdscr, current_y, box_left, "│ " + record_types_text.ljust(width - 4) + " │"); current_y += 1
    
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│ Query:".ljust(width - 1) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    
    # Use helper to display code lines
    current_y = display_code_lines_in_box(stdscr, query_lines, width, current_y, box_left)
    
    safe_addstr(stdscr, current_y, box_left, border_bottom); current_y += 1
    current_y += 1  # Add some spacing
    
    if not table.records:
        # No records box
        safe_addstr(stdscr, current_y, box_left, border_top); current_y += 1
        safe_addstr(stdscr, current_y, box_left, "│" + " No records to display ".center(width - 2) + "│"); current_y += 1
        safe_addstr(stdscr, current_y, box_left, border_bottom); current_y += 1
        current_y += 1
        
        # Instructions
        safe_addstr(stdscr, current_y, box_left, "Q/←: Back".ljust(width))
        
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_LEFT or key == ord('q'):
            return
    
    else:
        col_names = [col for col in table.columns]
        col_widths = {col: max(len(str(record.data[col])) for record in table.records) for col in col_names}
        record_limit = 100
        record_limit = min(record_limit, stdscr.getmaxyx()[0] - current_y - 8)
        display_table_records(stdscr, table, col_names, col_widths, current_y, record_limit)

@safe_execution
def display_mv_views(stdscr, db, base_offset):
    """
    Displays the list of materialized views in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        base_offset: The vertical offset to display the database information
    """
    current_row = 0
    width = 48  # Consistent width
    box_left = 0

    while True:
        screen_height, screen_width = stdscr.getmaxyx()
        for y_line in range(base_offset, screen_height):
            safe_addstr(stdscr, y_line, 0, " " * (screen_width-1))

        mv_view_names = list(db.materialized_views.keys()) if hasattr(db, 'materialized_views') else []
        count = len(mv_view_names)
        
        current_list_y = base_offset

        border_top = "╭" + "─" * (width - 2) + "╮"
        border_sep = "├" + "─" * (width - 2) + "┤"
        border_bottom = "╰" + "─" * (width - 2) + "╯"

        safe_addstr(stdscr, current_list_y, box_left, border_top); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, "│" + f" Materialized Views ({count}) ".center(width - 2) + "│"); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, "│ ID  │ MV Name".ljust(width - 2) + " │"); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1

        displayable_items_area_height = screen_height - current_list_y - 2
        items_per_page = max(1, displayable_items_area_height)

        for i_display, i_actual in enumerate(range(count)):
            if i_display >= items_per_page:
                break
            mv_view_name = mv_view_names[i_actual]
            y_pos = current_list_y + i_display
            name_padding = width - 9
            row_str = f"│ {str(i_actual).rjust(2)}  │ {mv_view_name[:name_padding].ljust(name_padding)}│"

            if i_actual == current_row:
                safe_addstr(stdscr, y_pos, box_left, row_str, curses.color_pair(1))
            else:
                safe_addstr(stdscr, y_pos, box_left, row_str)
        
        current_list_y += min(count, items_per_page)
        safe_addstr(stdscr, current_list_y, box_left, border_bottom); current_list_y += 1
        
        safe_addstr(stdscr, current_list_y, box_left, "Enter/→: View | Q/←: Back | ↑/↓: Nav".ljust(width)); current_list_y +=1
        stdscr.refresh()

        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < count - 1:
            current_row += 1
        elif (is_key(key, 'ENTER') or is_key(key, 'RIGHT')) and count > 0:
            if 0 <= current_row < count:
                selected_mv_name = mv_view_names[current_row]
                detail_offset = current_list_y + 1

                loading_msg_y = detail_offset -1
                safe_addstr(stdscr, loading_msg_y, 0, "Loading MV, please wait...".ljust(width))
                stdscr.refresh()

                try:
                    mv_object = db.get_materialized_view(selected_mv_name)
                    table_data = mv_object.get_data()
                    query_string = mv_object._query_to_string()

                    safe_addstr(stdscr, loading_msg_y, 0, " " * width)
                    stdscr.refresh()
                    
                    display_mv_view(stdscr, table_data, selected_mv_name, query_string, detail_offset)
                except Exception as e:
                    safe_addstr(stdscr, loading_msg_y, 0, " " * width)
                    logging.error(f"Error displaying MV {selected_mv_name}: {e}")
                    display_popup(stdscr, f"Error loading MV '{selected_mv_name}':\n{str(e)}", 3)

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
    box_left = 0
    current_y = view_offset
    screen_height, screen_width = stdscr.getmaxyx()
    
    # Calculate width based on query lines
    query_lines = remove_leading_spaces(query).split("\n")
    max_query_width = max(len(line) for line in query_lines) if query_lines else 0
    
    # Minimum width for view info, max of screen width or query width
    min_width = max(len(f"Materialized View: {view_name}") + 4, len(f"Record Types: {table.records[0]._type() if table.records else 'None'}") + 4)
    width = min(max(min_width, max_query_width + 4), screen_width - 2)
    
    # Box drawing characters
    border_top = "╭" + "─" * (width - 2) + "╮"
    border_sep = "├" + "─" * (width - 2) + "┤"
    border_bottom = "╰" + "─" * (width - 2) + "╯"
    
    # Materialized view information box
    safe_addstr(stdscr, current_y, box_left, border_top); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│" + f" Materialized View: {view_name} ".ljust(width - 2) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    
    # Row count
    row_count_text = f"Row Count: {len(table.records)}"
    safe_addstr(stdscr, current_y, box_left, "│ " + row_count_text.ljust(width - 4) + " │"); current_y += 1
    
    # Record types
    if table.records:
        record_types_text = f"Record Types: {table.records[0]._type()}"
    else:
        record_types_text = "Record Types: None"
    safe_addstr(stdscr, current_y, box_left, "│ " + record_types_text.ljust(width - 4) + " │"); current_y += 1
    
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│ Query:".ljust(width - 1) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    
    # Use helper to display code lines
    current_y = display_code_lines_in_box(stdscr, query_lines, width, current_y, box_left)
    
    safe_addstr(stdscr, current_y, box_left, border_bottom); current_y += 1
    current_y += 1  # Add some spacing
    
    if not table.records:
        # No records box
        safe_addstr(stdscr, current_y, box_left, border_top); current_y += 1
        safe_addstr(stdscr, current_y, box_left, "│" + " No records to display ".center(width - 2) + "│"); current_y += 1
        safe_addstr(stdscr, current_y, box_left, border_bottom); current_y += 1
        current_y += 1
        
        # Instructions
        safe_addstr(stdscr, current_y, box_left, "Q/←: Back".ljust(width))
        
        stdscr.refresh()
        key = stdscr.getch()
        if key == curses.KEY_LEFT or key == ord('q'):
            return
    
    else:
        col_names = [col for col in table.columns]
        col_widths = {col: max(len(str(record.data[col])) for record in table.records) for col in col_names}
        record_limit = 100
        record_limit = min(record_limit, stdscr.getmaxyx()[0] - current_y - 8)
        display_table_records(stdscr, table, col_names, col_widths, current_y, record_limit)

@safe_execution
def display_stored_procedures(stdscr, db, base_offset):
    """
    Displays the list of stored procedures in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        base_offset: The vertical offset to display the database information.
    """
    current_row = 0
    width = 60 # Wider for procedure names
    box_left = 0

    while True:
        screen_height, screen_width = stdscr.getmaxyx()
        for y_line in range(base_offset, screen_height):
            safe_addstr(stdscr, y_line, 0, " " * (screen_width-1))

        proc_names = list(db.stored_procedures.keys()) if hasattr(db, 'stored_procedures') else []
        count = len(proc_names)
        
        current_list_y = base_offset

        border_top = "╭" + "─" * (width - 2) + "╮"
        border_sep = "├" + "─" * (width - 2) + "┤"
        border_bottom = "╰" + "─" * (width - 2) + "╯"

        safe_addstr(stdscr, current_list_y, box_left, border_top); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, "│" + f" Stored Procedures ({count}) ".center(width - 2) + "│"); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, "│ ID  │ Procedure Name".ljust(width - 2) + " │"); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1

        displayable_items_area_height = screen_height - current_list_y - 2
        items_per_page = max(1, displayable_items_area_height)

        for i_display, i_actual in enumerate(range(count)):
            if i_display >= items_per_page: break
            proc_name = proc_names[i_actual]
            y_pos = current_list_y + i_display
            name_padding = width - 9 
            row_str = f"│ {str(i_actual).rjust(2)}  │ {proc_name[:name_padding].ljust(name_padding)}│"

            if i_actual == current_row:
                safe_addstr(stdscr, y_pos, box_left, row_str, curses.color_pair(1))
            else:
                safe_addstr(stdscr, y_pos, box_left, row_str)
        
        current_list_y += min(count, items_per_page)
        safe_addstr(stdscr, current_list_y, box_left, border_bottom); current_list_y += 1
        
        safe_addstr(stdscr, current_list_y, box_left, "Enter/→: View Code | Q/←: Back | ↑/↓: Nav".ljust(width)); current_list_y +=1
        stdscr.refresh()

        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < count - 1:
            current_row += 1
        elif (is_key(key, 'ENTER') or is_key(key, 'RIGHT')) and count > 0:
            if 0 <= current_row < count:
                selected_proc_name = proc_names[current_row]
                detail_offset = current_list_y + 1
                
                safe_addstr(stdscr, detail_offset -1 , 0, "Loading procedure...".ljust(width))
                stdscr.refresh()
                try:
                    # Assuming get_stored_procedure returns the function object
                    # and _stored_procedure_to_string gets its source code
                    proc_object = db.get_stored_procedure(selected_proc_name)
                    proc_code = db._stored_procedure_to_string(proc_object)
                    
                    safe_addstr(stdscr, detail_offset -1, 0, " " * width) # Clear loading
                    display_procedure(stdscr, proc_code, selected_proc_name, detail_offset)
                except Exception as e:
                    safe_addstr(stdscr, detail_offset -1, 0, " " * width) # Clear loading
                    logging.error(f"Error displaying procedure {selected_proc_name}: {e}")
                    display_popup(stdscr, f"Error loading procedure code:\n{str(e)}", 3)
            

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
    box_left = 0
    current_y = proc_offset
    screen_height, screen_width = stdscr.getmaxyx()
    
    # Calculate width based on procedure code lines
    procedure_clean = remove_leading_spaces(procedure)
    code_lines = procedure_clean.split("\n")
    max_code_width = max(len(line) for line in code_lines) if code_lines else 0
    
    # Minimum width for procedure info, max of screen width or code width
    min_width = max(len(f"Procedure: {procedure_name}") + 4, len("Code:") + 4)
    width = min(max(min_width, max_code_width + 4), screen_width - 2)
    
    # Box drawing characters
    border_top = "╭" + "─" * (width - 2) + "╮"
    border_sep = "├" + "─" * (width - 2) + "┤"
    border_bottom = "╰" + "─" * (width - 2) + "╯"
    
    # Procedure information box
    safe_addstr(stdscr, current_y, box_left, border_top); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│" + f" Procedure: {procedure_name} ".ljust(width - 2) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│ Code:".ljust(width - 1) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    
    # Use helper to display code lines
    current_y = display_code_lines_in_box(stdscr, code_lines, width, current_y, box_left)
    
    safe_addstr(stdscr, current_y, box_left, border_bottom); current_y += 1
    current_y += 1  # Add some spacing
    
    # Instructions
    safe_addstr(stdscr, current_y, box_left, "Q/←: Back".ljust(width))
    
    stdscr.refresh()
    
    key = stdscr.getch()
    
    if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
        return

@safe_execution
def display_trigger_functions(stdscr, db, base_offset):
    """
    Displays the list of trigger functions in the database on the provided screen.
    
    Args:
        stdscr: The curses window object where the information will be displayed.
        db: The database object containing the information to be displayed.
        base_offset: The vertical offset to display the database information.
    """
    current_row = 0
    width = 70 # Wider for "Type | Parent Function"
    box_left = 0

    while True:
        screen_height, screen_width = stdscr.getmaxyx()
        for y_line in range(base_offset, screen_height):
            safe_addstr(stdscr, y_line, 0, " " * (screen_width-1))

        trigger_list = []
        if hasattr(db, 'triggers'):
            for trigger_type, functions in db.triggers.items():
                for function_name in functions.keys(): # functions is a dict of {func_name: [func_obj, ...]}
                    trigger_list.append({'id': len(trigger_list) , 'type': trigger_type, 'name': function_name})
        count = len(trigger_list)
        
        current_list_y = base_offset

        border_top = "╭" + "─" * (width - 2) + "╮"
        border_sep = "├" + "─" * (width - 2) + "┤"
        border_bottom = "╰" + "─" * (width - 2) + "╯"

        safe_addstr(stdscr, current_list_y, box_left, border_top); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, "│" + f" Trigger Functions ({count}) ".center(width - 2) + "│"); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1
        # Header: "│ ID  │ Type    │ Function Name                  │"
        # Widths: ID(2) Type(8) Name(remaining)
        # Fixed: "│ "(2) "  │ "(3) " │ "(3) "│"(1) = 9
        # ID_W=2, TYPE_W=8. NAME_W = width - 9 - ID_W - TYPE_W
        id_col_w, type_col_w = 3, 8
        name_col_w = width - 9 - id_col_w - type_col_w
        header_str = f"│ {'ID'.ljust(id_col_w)} │ {'Type'.ljust(type_col_w)} │ {'Function Name'.ljust(name_col_w)}│"
        safe_addstr(stdscr, current_list_y, box_left, header_str); current_list_y += 1
        safe_addstr(stdscr, current_list_y, box_left, border_sep); current_list_y += 1

        displayable_items_area_height = screen_height - current_list_y - 2
        items_per_page = max(1, displayable_items_area_height)

        for i_display, i_actual in enumerate(range(count)):
            if i_display >= items_per_page: break
            
            trigger_item = trigger_list[i_actual]
            y_pos = current_list_y + i_display
            
            row_str = f"│ {str(trigger_item['id']).ljust(id_col_w)} │ {trigger_item['type'][:type_col_w].ljust(type_col_w)} │ {trigger_item['name'][:name_col_w].ljust(name_col_w)}│"

            if i_actual == current_row:
                safe_addstr(stdscr, y_pos, box_left, row_str, curses.color_pair(1))
            else:
                safe_addstr(stdscr, y_pos, box_left, row_str)
        
        current_list_y += min(count, items_per_page)
        safe_addstr(stdscr, current_list_y, box_left, border_bottom); current_list_y += 1
        
        safe_addstr(stdscr, current_list_y, box_left, "Enter/→: View Code | Q/←: Back | ↑/↓: Nav".ljust(width)); current_list_y +=1
        stdscr.refresh()

        key = stdscr.getch()
        if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
            break
        elif is_key(key, 'UP') and current_row > 0:
            current_row -= 1
        elif is_key(key, 'DOWN') and current_row < count - 1:
            current_row += 1
        elif (is_key(key, 'ENTER') or is_key(key, 'RIGHT')) and count > 0:
            if 0 <= current_row < count:
                selected_trigger = trigger_list[current_row]
                detail_offset = current_list_y + 1
                
                safe_addstr(stdscr, detail_offset -1, 0, "Loading function code...".ljust(width))
                stdscr.refresh()
                try:
                    # db.triggers[type][func_name] is a list, first element is the function object
                    trigger_func_obj = db.triggers[selected_trigger['type']][selected_trigger['name']][0]
                    function_code = db._stored_procedure_to_string(trigger_func_obj) # Use same util
                    
                    safe_addstr(stdscr, detail_offset -1, 0, " " * width) # Clear loading
                    display_function(stdscr, function_code, selected_trigger['name'], detail_offset)
                except Exception as e:
                    safe_addstr(stdscr, detail_offset-1, 0, " " * width) # Clear loading
                    logging.error(f"Error displaying trigger function {selected_trigger['name']}: {e}")
                    display_popup(stdscr, f"Error loading function code:\n{str(e)}", 3)

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
    box_left = 0
    current_y = func_offset
    screen_height, screen_width = stdscr.getmaxyx()
    
    # Calculate width based on function code lines
    function_clean = remove_leading_spaces(function)
    code_lines = function_clean.split("\n")
    max_code_width = max(len(line) for line in code_lines) if code_lines else 0
    
    # Minimum width for function info, max of screen width or code width
    min_width = max(len(f"Function: {function_name}") + 4, len("Code:") + 4)
    width = min(max(min_width, max_code_width + 4), screen_width - 2)
    
    # Box drawing characters
    border_top = "╭" + "─" * (width - 2) + "╮"
    border_sep = "├" + "─" * (width - 2) + "┤"
    border_bottom = "╰" + "─" * (width - 2) + "╯"
    
    # Function information box
    safe_addstr(stdscr, current_y, box_left, border_top); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│" + f" Function: {function_name} ".ljust(width - 2) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    safe_addstr(stdscr, current_y, box_left, "│ Code:".ljust(width - 1) + "│"); current_y += 1
    safe_addstr(stdscr, current_y, box_left, border_sep); current_y += 1
    
    # Use helper to display code lines
    current_y = display_code_lines_in_box(stdscr, code_lines, width, current_y, box_left)
    
    safe_addstr(stdscr, current_y, box_left, border_bottom); current_y += 1
    current_y += 1  # Add some spacing
    
    # Instructions
    safe_addstr(stdscr, current_y, box_left, "Q/←: Back".ljust(width))
    
    stdscr.refresh()
    
    key = stdscr.getch()
    
    if is_key(key, 'QUIT') or is_key(key, 'LEFT'):
        return

# Mapping of Pygments token types to curses color pairs
PYGMENTS_TOKEN_TO_COLOR = {
    Token.Keyword: 4,
    Token.Name.Function: 5,
    Token.Name.Class: 5,
    Token.Name: 9,
    Token.Literal.String: 10,
    Token.Literal.Number: 5,
    Token.Literal: 6,
    Token.Operator: 7,
    Token.Comment: 0,
    Token.Text: 0,
}

PYGMENTS_COLOR_INIT = False

def init_pygments_curses_colors():
    global PYGMENTS_COLOR_INIT
    if PYGMENTS_COLOR_INIT:
        return
    # Only initialize pairs 4-9 for syntax highlighting if supported
    max_needed = 9
    if hasattr(curses, 'COLOR_PAIRS') and curses.COLOR_PAIRS > max_needed:
        for pair, color in [
            (4, curses.COLOR_BLUE),
            (5, curses.COLOR_YELLOW),
            (6, curses.COLOR_CYAN),
            (7, curses.COLOR_MAGENTA),
            (8, curses.COLOR_WHITE),
            (9, curses.COLOR_GREEN),
            (10, curses.COLOR_RED),
        ]:
            try:
                curses.init_pair(pair, color, curses.COLOR_BLACK)
            except curses.error:
                pass  # Skip if not supported
    PYGMENTS_COLOR_INIT = True

def display_code_lines_in_box(stdscr, code_lines, width, start_y, box_left):
    """
    Helper to display code lines inside a box, handling truncation and alignment, with syntax highlighting.
    Args:
        stdscr: The curses window object.
        code_lines: List of code lines to display.
        width: Width of the box.
        start_y: Starting y position.
        box_left: Starting x position (usually 0).
    Returns:
        The next y position after the last code line.
    """
    init_pygments_curses_colors()
    current_y = start_y
    lexer = PythonLexer()
    max_code_width = width - 4
    
    code_na = True if code_lines[0] == 'Source code not available' else False
    
    in_tripple_quote = False
    for line in code_lines:
        tokens = list(pygments.lex(line, lexer))
        x = box_left + 2  # Start after left border and space
        safe_addstr(stdscr, current_y, box_left, "│ ")
        chars_written = 0
        
        for ttype, value in tokens:
            # print(f"Token: {ttype}, Value: '{value}'")  # Debugging output

            # If we are in a tripple quote we want to treat as a string untill we see the next tripple
            if '"""' in value or "'''" in value:
                in_tripple_quote = not in_tripple_quote

            # Truncate if line is too long
            if chars_written >= max_code_width:
                break
            value = value[:max_code_width - chars_written]
            color = 0
            
            if in_tripple_quote:
                color = PYGMENTS_TOKEN_TO_COLOR.get(Token.Literal.String, 5)  # Default to string color
            else:   
                # Find the most specific color mapping
                for token_type, color_pair in PYGMENTS_TOKEN_TO_COLOR.items():
                    if ttype in token_type:
                        color = color_pair
                        break
            try:
                if code_na:
                    color = PYGMENTS_TOKEN_TO_COLOR.get(Token.Literal.String, 10)
                    stdscr.addstr(current_y, x, value, curses.color_pair(color))
                if color > 0:
                    stdscr.addstr(current_y, x, value, curses.color_pair(color))
                else:
                    stdscr.addstr(current_y, x, value)
            except curses.error:
                pass
            x += len(value)
            chars_written += len(value)
        # Fill the rest of the line with spaces if needed
        if chars_written < max_code_width:
            safe_addstr(stdscr, current_y, box_left + 2 + chars_written, " " * (max_code_width - chars_written))
        safe_addstr(stdscr, current_y, box_left + width - 2, " │")
        current_y += 1
    return current_y