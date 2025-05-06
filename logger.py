# --- Logging Configuration ---
LOG_LEVEL_NONE = 0
LOG_LEVEL_ERROR = 1
LOG_LEVEL_WARNING = 2
LOG_LEVEL_INFO = 3

# Set the desired log level here
CURRENT_LOG_LEVEL = LOG_LEVEL_ERROR # Default to INFO, can be changed by other modules if needed

def set_log_level(level: int):
    global CURRENT_LOG_LEVEL
    CURRENT_LOG_LEVEL = level

def log_message(level: int, message: str):
    """Prints a message if the level is appropriate for the current log setting."""
    if level <= CURRENT_LOG_LEVEL:
        level_prefix = {LOG_LEVEL_ERROR: "ERROR: ", LOG_LEVEL_WARNING: "WARNING: "}.get(level, "")
        print(f"{level_prefix}{message}")