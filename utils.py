import re
from pathlib import Path
from logger import log_message, LOG_LEVEL_WARNING, LOG_LEVEL_ERROR

def sanitize_column_name(name_in, index_for_fallback=None):
    """Converts a CSV header name to a SQL-friendly column name."""
    original_name_for_error_msg = name_in
    name = str(name_in).strip().lower()
    
    name = name.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss')
    name = re.sub(r'[\s\-\./\(\)]+', '_', name)
    name = re.sub(r'[^a-z0-9_]', '', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    
    if not name:
        if index_for_fallback is not None:
            log_message(LOG_LEVEL_WARNING, f"Original column name '{original_name_for_error_msg}' (at index {index_for_fallback}) sanitized to empty or was originally empty. Using fallback 'col_unnamed_{index_for_fallback}'.")
            return f"col_unnamed_{index_for_fallback}"
        else:
            log_message(LOG_LEVEL_WARNING, f"Original column name '{original_name_for_error_msg}' sanitized to empty. Using fallback 'col_unnamed'.")
            return "col_unnamed"
    
    if name and name[0].isdigit():
        name = f"col_{name}"
    return name

def count_data_lines_in_file(filepath: Path) -> int:
    """Counts the number of data lines in a file (total lines - 1 for header)."""
    try:
        with open(filepath, 'r', encoding='windows-1252') as f:
            lines = sum(1 for _ in f)
        return max(0, lines - 1)
    except Exception as e:
        log_message(LOG_LEVEL_WARNING, f"Could not count lines in {filepath.name} for progress bar: {e}")
        return 0

def find_tg_code_column_index(header: list, known_tg_code_names: list) -> tuple[int, str] | tuple[None, None]:
    """Finds the index and name of the TG-Code column in the header."""
    for i, col_name in enumerate(header):
        if str(col_name) in known_tg_code_names: # Ensure col_name is string
            return i, str(col_name)
    return None, None

def setup_directories(data_dir: Path, database_dir: Path):
    """Creates data and database directories if they don't exist."""
    from logger import log_message, LOG_LEVEL_INFO # Local import to avoid circular dependency if logger uses utils
    log_message(LOG_LEVEL_INFO, "Setting up directories...")
    data_dir.mkdir(parents=True, exist_ok=True)
    database_dir.mkdir(parents=True, exist_ok=True)
    log_message(LOG_LEVEL_INFO, f"Data directory will be: {data_dir.resolve()}")
    log_message(LOG_LEVEL_INFO, f"Database directory will be: {database_dir.resolve()}")