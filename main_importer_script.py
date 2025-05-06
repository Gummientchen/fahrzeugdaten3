import sqlite3
from collections import defaultdict
import traceback

import config
import logger
import utils
import downloader
import db_manager
import importer

def main_import_process():
    logger.log_message(logger.LOG_LEVEL_INFO, "Starting the data import process...")
    
    utils.setup_directories(config.DATA_DIR, config.DATABASE_DIR)
    downloader.download_all_files(config.FILES_TO_PROCESS, config.DATA_DIR)

    logger.log_message(logger.LOG_LEVEL_INFO, f"\n--- Stage 2: Importing data into SQLite database: {config.DATABASE_PATH.resolve()} ---")
    db_conn = None

    primary_table_details = next((f for f in config.FILES_TO_PROCESS if f.get("is_primary_key_table")), None)
    if not primary_table_details:
        logger.log_message(logger.LOG_LEVEL_ERROR, "No primary key table defined. Cannot establish FK relationships.")
        return
    primary_ref_table_name = primary_table_details["table_name"]

    if config.DATABASE_PATH.exists():
        logger.log_message(logger.LOG_LEVEL_INFO, f"Deleting existing database: {config.DATABASE_PATH.resolve()}")
        config.DATABASE_PATH.unlink()

    try:
        db_conn = sqlite3.connect(config.DATABASE_PATH)
        db_conn.execute("PRAGMA foreign_keys = ON;")

        cursor = db_conn.cursor()
        db_manager.create_all_lookup_tables(cursor, config.COLUMNS_TO_NORMALIZE_CONFIG)
        db_conn.commit()

        sorted_files = sorted(config.FILES_TO_PROCESS, key=lambda x: not x['is_primary_key_table'])
        lookup_caches = defaultdict(dict)
        
        for file_conf in sorted_files:
            importer.import_single_file_data(db_conn, file_conf, config.DATA_DIR, primary_ref_table_name, config.COLUMNS_TO_NORMALIZE_CONFIG, lookup_caches)
        
        logger.log_message(logger.LOG_LEVEL_INFO, "\nData import process finished successfully!")

    except Exception as e:
        logger.log_message(logger.LOG_LEVEL_ERROR, f"An unexpected error occurred: {e}")
        if logger.CURRENT_LOG_LEVEL >= logger.LOG_LEVEL_ERROR:
            traceback.print_exc()
    finally:
        if db_conn:
            db_conn.close()
            logger.log_message(logger.LOG_LEVEL_INFO, "\nDatabase connection closed.")

if __name__ == "__main__":
    # You can set a different log level for the import process if needed
    # logger.set_log_level(logger.LOG_LEVEL_INFO) 
    main_import_process()