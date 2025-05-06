import csv
from pathlib import Path
from tqdm.auto import tqdm
from collections import defaultdict

from logger import log_message, LOG_LEVEL_INFO, LOG_LEVEL_WARNING, LOG_LEVEL_ERROR
from utils import sanitize_column_name, count_data_lines_in_file
from config import STANDARDIZED_TG_CODE_COL
from db_manager import get_or_create_lookup_id # Import the specific function

def process_header_and_column_setup(original_header: list, tg_code_col_idx: int,
                                     table_normalization_rules: dict) -> tuple[list[str], list[str], list[tuple], list[str]]:
    db_column_definitions_for_create = []
    db_column_names_for_insert = []
    row_transformation_plan = [] # (csv_idx, db_col_name_for_insert, is_normalized, lookup_table_name, is_pk)
    fk_constraints_to_lookup_tables = []
    temp_db_col_name_counts = {}

    for csv_idx, original_col_name_csv in enumerate(original_header):
        sanitized_original_col_name = sanitize_column_name(original_col_name_csv, csv_idx)

        if "unnamed" in sanitized_original_col_name:
            log_message(LOG_LEVEL_INFO, f"Skipping unnamed column: original '{original_col_name_csv}' (sanitized: {sanitized_original_col_name}) at index {csv_idx}.")
            continue

        is_pk = (csv_idx == tg_code_col_idx)
        db_col_name_final = STANDARDIZED_TG_CODE_COL if is_pk else sanitized_original_col_name
        
        if not is_pk and db_col_name_final == STANDARDIZED_TG_CODE_COL:
            db_col_name_final = f"{STANDARDIZED_TG_CODE_COL}_orig_idx_{csv_idx}"

        if db_col_name_final in temp_db_col_name_counts:
            temp_db_col_name_counts[db_col_name_final] += 1
            db_col_name_final = f"{db_col_name_final}_{temp_db_col_name_counts[db_col_name_final]}"
        else:
            temp_db_col_name_counts[db_col_name_final] = 0

        is_normalized = sanitized_original_col_name in table_normalization_rules
        lookup_table_name = table_normalization_rules.get(sanitized_original_col_name) if is_normalized else None
        col_def_type = "TEXT"
        current_db_col_name_for_insert = db_col_name_final

        if is_pk:
            col_def_type = "TEXT PRIMARY KEY"
        elif is_normalized:
            current_db_col_name_for_insert = f"{db_col_name_final}_id"
            col_def_type = "INTEGER"
            if lookup_table_name: # Ensure lookup_table_name is not None
                 fk_constraints_to_lookup_tables.append(f"FOREIGN KEY (\"{current_db_col_name_for_insert}\") REFERENCES \"{lookup_table_name}\"(id)")

        db_column_definitions_for_create.append(f'"{current_db_col_name_for_insert}" {col_def_type}')
        db_column_names_for_insert.append(current_db_col_name_for_insert)
        row_transformation_plan.append((csv_idx, current_db_col_name_for_insert, is_normalized, lookup_table_name, is_pk))

    return db_column_definitions_for_create, db_column_names_for_insert, row_transformation_plan, fk_constraints_to_lookup_tables

def prepare_and_transform_rows_from_csv(cursor, csv_reader: csv.reader, csv_filepath: Path,
                                         row_transformation_plan: list[tuple], original_header_len: int,
                                         lookup_caches: defaultdict[str, dict]) -> list[tuple]:
    transformed_rows_for_db = []
    csv_filename = csv_filepath.name
    total_data_rows = count_data_lines_in_file(csv_filepath)
    
    progress_bar = tqdm(csv_reader, total=total_data_rows, desc=f"Parsing {csv_filename}", mininterval=0.25, unit="rows", leave=False)
    
    for i, raw_csv_row_list in enumerate(progress_bar):
        current_csv_col_count = len(raw_csv_row_list)
        
        if current_csv_col_count < original_header_len:
            raw_csv_row_list.extend([None] * (original_header_len - current_csv_col_count))
            log_message(LOG_LEVEL_INFO, f"Row {i+2} in {csv_filename} (raw) padded. Data: {raw_csv_row_list[:5]}...")
        elif current_csv_col_count > original_header_len:
            log_message(LOG_LEVEL_WARNING, f"Skipping row {i+2} in {csv_filename} (raw). Expected {original_header_len} cols, got {current_csv_col_count}. Data: {raw_csv_row_list[:5]}...")
            continue

        single_transformed_row = []
        valid_row = True
        for csv_idx, _, is_normalized, lookup_table_name, _ in row_transformation_plan:
            if csv_idx >= len(raw_csv_row_list):
                log_message(LOG_LEVEL_ERROR, f"Row {i+2} in {csv_filename}: CSV index {csv_idx} out of bounds. Skipping row.")
                valid_row = False; break
            
            original_value = raw_csv_row_list[csv_idx]

            if is_normalized:
                if not lookup_table_name:
                     log_message(LOG_LEVEL_ERROR, f"Row {i+2} in {csv_filename}: Col at CSV idx {csv_idx} normalized but no lookup table. Skipping value.")
                     single_transformed_row.append(None); continue
                
                lookup_id = get_or_create_lookup_id(cursor, lookup_table_name, original_value, lookup_caches[lookup_table_name])
                single_transformed_row.append(lookup_id)
            else:
                single_transformed_row.append(original_value)
        
        if valid_row:
            transformed_rows_for_db.append(tuple(single_transformed_row))
            
    return transformed_rows_for_db