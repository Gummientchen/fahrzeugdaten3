import sqlite3
from pathlib import Path

from logger import log_message, LOG_LEVEL_INFO, LOG_LEVEL_WARNING, LOG_LEVEL_ERROR
from config import COLUMNS_TO_NORMALIZE_CONFIG, FILES_TO_PROCESS, STANDARDIZED_TG_CODE_COL

def search_car_data(db_path: Path, 
                    tg_code: str = None, 
                    marke_str: str = None, # New: search by marke string
                    typ_str: str = None,   # New: search by typ string
                    year_str: str = None,  # New: search by year string (YYYY)
                    # Keep ID based search for internal/advanced use if needed, or remove if only string search is desired
                    col_04_marke_id: int = None,
                    col_04_typ_id: int = None,
                    col_06_vorziffer_id: int = None, 
                    col_09_eu_gesamtgenehmigung_id: int = None) -> list[dict]:
    if not db_path.exists():
        log_message(LOG_LEVEL_ERROR, f"Database file not found at {db_path}")
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # --- Convert string search terms to IDs ---
    # These will override direct ID inputs if both are somehow provided
    final_col_04_marke_ids = [col_04_marke_id] if col_04_marke_id is not None else []
    final_col_04_typ_ids = [col_04_typ_id] if col_04_typ_id is not None else []

    if marke_str:
        # Find the lookup table name for 'col_04_marke' in 'cars'
        marke_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_marke")
        if marke_lookup_table:
            cursor.execute(f"SELECT id, value FROM \"{marke_lookup_table}\" WHERE value LIKE ?", (f"%{marke_str}%",))
            matches = cursor.fetchall()
            if not matches:
                log_message(LOG_LEVEL_WARNING, f"Marke string '{marke_str}' not found in lookup table '{marke_lookup_table}'.")
                final_col_04_marke_ids = [-1] # Use -1 to ensure no SQL match if string not found
            else:
                final_col_04_marke_ids = [m['id'] for m in matches]
                if len(matches) > 1:
                    log_message(LOG_LEVEL_INFO, f"Found {len(matches)} matching marken for '{marke_str}'. Using all in search.")
        else: log_message(LOG_LEVEL_ERROR, "Lookup table for 'col_04_marke' not defined in config.")

    if typ_str:
        typ_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_typ")
        if typ_lookup_table:
            cursor.execute(f"SELECT id, value FROM \"{typ_lookup_table}\" WHERE value LIKE ?", (f"%{typ_str}%",))
            matches = cursor.fetchall()
            if not matches:
                log_message(LOG_LEVEL_WARNING, f"Typ string '{typ_str}' not found in lookup table '{typ_lookup_table}'.")
                final_col_04_typ_ids = [-1] # Use -1 to ensure no SQL match
            else:
                final_col_04_typ_ids = [m['id'] for m in matches]
                if len(matches) > 1:
                    log_message(LOG_LEVEL_INFO, f"Found {len(matches)} matching typen for '{typ_str}'. Using all in search.")
        else: log_message(LOG_LEVEL_ERROR, "Lookup table for 'col_04_typ' not defined in config.")

    # --- Stage 1: Fetch main data with IDs ---
    main_select_clauses, main_join_clauses_str, where_conditions, query_params = [], [], [], {}

    def add_main_table_fields(table_db_name: str, select_list: list):
        """Adds all columns from a main table to the select list."""
        cursor.execute(f"PRAGMA table_info(\"{table_db_name}\")")
        columns_info = cursor.fetchall()
        for col_info in columns_info:
            col_db_name = col_info['name']
            # Prefix with table name to avoid ambiguity if same column name exists in multiple joined tables (e.g. tg_code)
            select_list.append(f"\"{table_db_name}\".\"{col_db_name}\" AS \"{table_db_name}_{col_db_name}\"")

    cars_table_conf = next(f for f in FILES_TO_PROCESS if f.get("is_primary_key_table"))
    cars_table_name = cars_table_conf["table_name"]
    
    main_from_sql = f"FROM \"{cars_table_name}\" "
    add_main_table_fields(cars_table_name, main_select_clauses)

    for table_conf in FILES_TO_PROCESS:
        if not table_conf["is_primary_key_table"]:
            current_table_name = table_conf["table_name"]
            main_join_clauses_str.append(f"LEFT JOIN \"{current_table_name}\" ON \"{cars_table_name}\".\"{STANDARDIZED_TG_CODE_COL}\" = \"{current_table_name}\".\"{STANDARDIZED_TG_CODE_COL}\"")
            add_main_table_fields(current_table_name, main_select_clauses)

    # Build WHERE clause
    search_criteria_map = {
        "tg_code": (f"\"{cars_table_name}\".\"{STANDARDIZED_TG_CODE_COL}\" = :tg_code_val", tg_code),
        # For marke_ids and typ_ids, we'll handle IN clause separately if they are lists
        "col_06_vorziffer_id": (f"\"{cars_table_name}\".\"col_06_vorziffer_id\" = :col_06_vorziffer_id_val", col_06_vorziffer_id),
        "col_09_eu_gesamtgenehmigung_id": (f"\"{cars_table_name}\".\"col_09_eu_gesamtgenehmigung_id\" = :col_09_eu_gesamtgenehmigung_id_val", col_09_eu_gesamtgenehmigung_id),
    }
    for param_key, (condition_str, value) in search_criteria_map.items():
        if value is not None:
            where_conditions.append(condition_str)
            query_params[f"{param_key}_val"] = value

    if final_col_04_marke_ids:
        marke_placeholders = ','.join([f':marke_id_{i}' for i in range(len(final_col_04_marke_ids))])
        where_conditions.append(f"\"{cars_table_name}\".\"col_04_marke_id\" IN ({marke_placeholders})")
        for i, mid in enumerate(final_col_04_marke_ids):
            query_params[f'marke_id_{i}'] = mid
            
    if final_col_04_typ_ids:
        typ_placeholders = ','.join([f':typ_id_{i}' for i in range(len(final_col_04_typ_ids))])
        where_conditions.append(f"\"{cars_table_name}\".\"col_04_typ_id\" IN ({typ_placeholders})")
        for i, tid in enumerate(final_col_04_typ_ids):
            query_params[f'typ_id_{i}'] = tid

    if year_str:
        # Assuming 'typengenehmigung_erteilt' is the column in the 'cars' table (cars_table_name)
        # and it stores the date as YYYYMMDD. SUBSTR will extract the YYYY part.
        where_conditions.append(f"SUBSTR(\"{cars_table_name}\".\"typengenehmigung_erteilt\", 1, 4) = :year_val")
        query_params['year_val'] = year_str

    main_sql = f"SELECT {', '.join(main_select_clauses)} {main_from_sql} {' '.join(main_join_clauses_str)}"
    if where_conditions:
        main_sql += " WHERE " + " AND ".join(where_conditions)
    else:
        log_message(LOG_LEVEL_WARNING, "Search called with no criteria; this might return a very large dataset.")
    
    log_message(LOG_LEVEL_INFO, f"Executing main data query: {main_sql}")
    log_message(LOG_LEVEL_INFO, f"With params: {query_params}")

    initial_results_with_ids = []
    try:
        cursor.execute(main_sql, query_params)
        initial_results_with_ids = [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        log_message(LOG_LEVEL_ERROR, f"Error during main data query execution: {e}")
        conn.close()
        return []

    if not initial_results_with_ids:
        conn.close()
        return []

    # --- Stage 2: De-normalize _id fields ---
    final_results = []
    for row_dict in initial_results_with_ids:
        denormalized_row = dict(row_dict) # Start with a copy
        for table_conf in FILES_TO_PROCESS: # Iterate through cars, emissions, consumption
            table_db_name = table_conf["table_name"] # This is 'cars', 'emissions', or 'consumption'
            table_specific_norm_rules = COLUMNS_TO_NORMALIZE_CONFIG.get(table_db_name, {})
            
            # Iterate through the normalization rules for the current table_db_name (e.g., 'cars')
            for original_sanitized_name_in_config, lookup_table_name in table_specific_norm_rules.items():
                # Construct the key for the _id column as it would appear in row_dict
                # e.g., if table_db_name is 'cars' and original_sanitized_name_in_config is 'col_04_marke',
                # then id_column_key becomes 'cars_col_04_marke_id'
                id_column_key = f"{table_db_name}_{original_sanitized_name_in_config}_id"
                
                if id_column_key in denormalized_row and denormalized_row[id_column_key] is not None:
                    lookup_id = denormalized_row[id_column_key]
                    try:
                        cursor.execute(f"SELECT value FROM \"{lookup_table_name}\" WHERE id = ?", (lookup_id,))
                        lookup_result = cursor.fetchone()
                        if lookup_result:
                            # Construct the key for the de-normalized value
                            # e.g., cars_col_04_marke_value
                            value_column_key = f"{table_db_name}_{original_sanitized_name_in_config}_value"
                            denormalized_row[value_column_key] = lookup_result['value']
                    except sqlite3.Error as e_lookup:
                        log_message(LOG_LEVEL_ERROR, f"Error looking up ID {lookup_id} in {lookup_table_name}: {e_lookup}")
        final_results.append(denormalized_row)
    
    conn.close()
    return final_results
