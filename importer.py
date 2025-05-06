import csv
import sqlite3
from pathlib import Path
from collections import defaultdict

from logger import log_message, LOG_LEVEL_INFO, LOG_LEVEL_WARNING, LOG_LEVEL_ERROR
from utils import find_tg_code_column_index
from config import TG_CODE_COLUMN_NAMES, STANDARDIZED_TG_CODE_COL
from data_parser import process_header_and_column_setup, prepare_and_transform_rows_from_csv
from db_manager import create_main_db_table, insert_data_generic, insert_data_emissions_special

def import_single_file_data(db_conn: sqlite3.Connection, file_config: dict, data_dir: Path,
                            primary_reference_table_name: str | None,
                            normalization_rules: dict, lookup_caches: defaultdict[str, dict]):
    local_filepath = data_dir / file_config["local_name"]
    table_name = file_config["table_name"]
    is_main_fk_table = not file_config["is_primary_key_table"]

    log_message(LOG_LEVEL_INFO, f"\nProcessing file: {local_filepath.resolve()} for table: {table_name}")

    if not local_filepath.exists():
        log_message(LOG_LEVEL_WARNING, f"File not found: {local_filepath.resolve()}. Skipping import.")
        return

    try:
        with open(local_filepath, 'r', encoding='windows-1252') as f_csv:
            reader = csv.reader(f_csv, delimiter='\t')
            header = next(reader)
            original_header_len = len(header)
            
            tg_code_col_idx, _ = find_tg_code_column_index(header, TG_CODE_COLUMN_NAMES)
            if tg_code_col_idx == -1:
                log_message(LOG_LEVEL_ERROR, f"Key column not found in {local_filepath.name}. Expected: {TG_CODE_COLUMN_NAMES}. Found: {header}")
                return            

            table_norm_rules = normalization_rules.get(table_name, {})
            db_col_defs, db_col_names_insert, row_transform_plan, main_tbl_fks_lkp = \
                process_header_and_column_setup(header, tg_code_col_idx, table_norm_rules)

            if not db_col_names_insert:
                log_message(LOG_LEVEL_WARNING, f"No processable columns for {local_filepath.name}. Skipping.")
                return

            cursor = db_conn.cursor()
            create_main_db_table(cursor, table_name, db_col_defs, is_main_fk_table, primary_reference_table_name, main_tbl_fks_lkp)
            
            rows_to_insert = prepare_and_transform_rows_from_csv(cursor, reader, local_filepath, row_transform_plan, original_header_len, lookup_caches)

            if table_name == "emissions":
                insert_data_emissions_special(cursor, table_name, db_col_names_insert, rows_to_insert, primary_reference_table_name)
            else:
                insert_data_generic(cursor, table_name, db_col_names_insert, rows_to_insert)
            db_conn.commit()
    except Exception as e:
        log_message(LOG_LEVEL_ERROR, f"Error processing {local_filepath.name}: {e}")
        if LOG_LEVEL_ERROR <= log_message(0, ""): # Check if error logging is enabled
            import traceback; traceback.print_exc()