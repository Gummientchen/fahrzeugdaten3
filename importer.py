print("Attempting to parse importer.py...") # For Docker log debugging

import sqlite3
import logging # Using standard logging
import os # For os.path.dirname

# Imports from your project that are safe to import at module level
from config import DATABASE_PATH, FILES_TO_PROCESS, COLUMNS_TO_NORMALIZE_CONFIG, TG_CODE_COLUMN_NAMES, STANDARDIZED_TG_CODE_COL
# Defer other custom module imports to within the function if they cause load-time issues

# Use standard logging for this module
logger = logging.getLogger(__name__)
# Basic configuration for the logger if not configured elsewhere (e.g., by Gunicorn/Flask)
if not logger.handlers: # Avoid adding multiple handlers if already configured
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO) # Set a default level

def main_importer_logic():
    logger.info("Importer: main_importer_logic function started for data import/update.")
    
    db_path_str = str(DATABASE_PATH) # Ensure DATABASE_PATH is string for os.path
    db_dir = os.path.dirname(db_path_str)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.info(f"Importer: Created database directory: {db_dir}")

    conn = None
    try:
        conn = sqlite3.connect(DATABASE_PATH) 
        if conn is None:
            logger.error("Importer: Failed to create database connection.")
            return
        
        # Import db_manager here, only when the function is called
        from db_manager import execute_sql 

        logger.info("Importer: Dropping existing tables for fresh import...")
        
        all_lookup_tables_to_drop = set()
        if COLUMNS_TO_NORMALIZE_CONFIG: # Check if it's not None or empty
            for table_key in COLUMNS_TO_NORMALIZE_CONFIG:
                for col_to_norm, lkp_table_name in COLUMNS_TO_NORMALIZE_CONFIG[table_key].items():
                    all_lookup_tables_to_drop.add(lkp_table_name)
        
        for lkp_table_name in sorted(list(all_lookup_tables_to_drop), reverse=True):
            drop_table_sql = f"DROP TABLE IF EXISTS \"{lkp_table_name}\";"
            execute_sql(conn, drop_table_sql)
            logger.info(f"Importer: Dropped lookup table {lkp_table_name} if it existed.")

        for file_info in reversed(FILES_TO_PROCESS): 
            table_name = file_info['table_name']
            drop_table_sql = f"DROP TABLE IF EXISTS \"{table_name}\";"
            execute_sql(conn, drop_table_sql)
            logger.info(f"Importer: Dropped table {table_name} if it existed.")
        
        conn.commit()

        # This assumes 'main_importer_script.py' exists and has 'main_import_process'
        # Ensure 'main_importer_script.py' is also copied into your Docker image.
        try:
            # Import main_importer_script here, only when the function is called
            from main_importer_script import main_import_process as run_the_actual_importer
            logger.info("Importer: Calling the main import process from main_importer_script.py...")
            run_the_actual_importer() 
            logger.info("Importer: Main import process finished.")
        except ImportError:
            logger.error("Importer: Failed to import 'main_import_process' from 'main_importer_script.py'. Make sure the file and function exist and it has no import errors itself.")
            raise # Re-raise the error to stop execution if critical
        except Exception as e_main_import:
            logger.error(f"Importer: Error during execution of main_import_process from main_importer_script.py: {e_main_import}", exc_info=True)
            raise

    except Exception as e:
        logger.error(f"Importer: Error during main_importer_logic: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Importer: Database connection closed by main_importer_logic.")

print("importer.py has been parsed, main_importer_logic should be defined.") # For Docker log debugging

if __name__ == '__main__':
    # This block is for running importer.py directly (e.g., for testing the import)
    # The logger configuration here will apply if this script is the entry point.
    # If imported, the logger config at the top of the file (or from webapp.py) applies.
    if not logging.getLogger(__name__).handlers: # Check if handlers are already set (e.g. by webapp)
        logging.basicConfig(level=logging.INFO, 
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            handlers=[logging.StreamHandler()])
    
    logger.info("importer.py is being run directly.")
    main_importer_logic()
