import sqlite3
from tqdm.auto import tqdm
from logger import log_message, LOG_LEVEL_INFO, LOG_LEVEL_WARNING, LOG_LEVEL_ERROR
from config import STANDARDIZED_TG_CODE_COL # For pk_col_name_for_main_fk

def create_all_lookup_tables(cursor: sqlite3.Cursor, columns_to_normalize_config: dict):
    """Creates all unique lookup tables based on the normalization configuration."""
    all_lookup_table_names = set()
    for config_per_table in columns_to_normalize_config.values():
        for lookup_name in config_per_table.values():
            all_lookup_table_names.add(lookup_name)
    
    for lookup_name in sorted(list(all_lookup_table_names)):
        log_message(LOG_LEVEL_INFO, f"Ensuring lookup table '{lookup_name}' exists...")
        cursor.execute(f"CREATE TABLE IF NOT EXISTS \"{lookup_name}\" (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT UNIQUE NOT NULL)")

def create_main_db_table(cursor: sqlite3.Cursor, table_name: str,
                         column_definitions: list[str],
                         is_main_fk_table: bool,
                         primary_reference_table_name: str | None,
                         fk_constraints_to_lookup_tables: list[str]):
    """Creates a main data table with appropriate primary and foreign keys."""
    all_definitions = list(column_definitions)

    if is_main_fk_table and primary_reference_table_name:
        all_definitions.append(f"FOREIGN KEY(\"{STANDARDIZED_TG_CODE_COL}\") REFERENCES \"{primary_reference_table_name}\"(\"{STANDARDIZED_TG_CODE_COL}\")")
    
    all_definitions.extend(fk_constraints_to_lookup_tables)
    
    create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(all_definitions)})"
    cursor.execute(create_table_sql)

def get_or_create_lookup_id(cursor: sqlite3.Cursor, lookup_table_name: str, value: str | None, cache: dict) -> int | None:
    """Gets or creates an ID for a value in a lookup table, using a cache."""
    if value is None or value == '':
        return None

    if value in cache:
        return cache[value]

    cursor.execute(f"SELECT id FROM \"{lookup_table_name}\" WHERE value = ?", (value,))
    row = cursor.fetchone()
    if row:
        cache[value] = row[0]
        return row[0]
    else:
        try:
            cursor.execute(f"INSERT INTO \"{lookup_table_name}\" (value) VALUES (?)", (value,))
            new_id = cursor.lastrowid
            cache[value] = new_id
            return new_id
        except sqlite3.IntegrityError:
            log_message(LOG_LEVEL_WARNING, f"IntegrityError on inserting '{value}' into '{lookup_table_name}'. Re-querying.")
            cursor.execute(f"SELECT id FROM \"{lookup_table_name}\" WHERE value = ?", (value,))
            row = cursor.fetchone() # Re-fetch after potential concurrent insert
            if row:
                cache[value] = row[0]
                return row[0]
            log_message(LOG_LEVEL_ERROR, f"Could not get or create ID for '{value}' in '{lookup_table_name}' after IntegrityError.")
            return None

def insert_data_generic(cursor: sqlite3.Cursor, table_name: str, db_column_names_for_insert: list[str], rows_to_insert: list[tuple]):
    """Inserts data into a table using batch INSERT OR IGNORE."""
    if not rows_to_insert:
        log_message(LOG_LEVEL_INFO, f"No valid rows found to insert into \"{table_name}\".")
        return

    quoted_final_cols = [f'"{c}"' for c in db_column_names_for_insert]
    placeholders = ', '.join(['?'] * len(quoted_final_cols))
    insert_sql = f"INSERT OR IGNORE INTO \"{table_name}\" ({', '.join(quoted_final_cols)}) VALUES ({placeholders})"
    
    cursor.executemany(insert_sql, rows_to_insert)
    log_message(LOG_LEVEL_INFO, f"Inserted/updated data for {cursor.rowcount} rows into \"{table_name}\" (out of {len(rows_to_insert)} valid rows from CSV).")

def insert_data_emissions_special(cursor: sqlite3.Cursor, table_name: str, db_column_names_for_insert: list[str],
                                  rows_to_insert: list[tuple], primary_reference_table_name: str):
    """Inserts data into the 'emissions' table with special FK pre-checks."""
    if not rows_to_insert:
        log_message(LOG_LEVEL_INFO, f"No valid rows found to insert into \"{table_name}\".")
        return

    log_message(LOG_LEVEL_INFO, f"Using row-by-row insert with pre-FK check for table '{table_name}'...")
    inserted_count, skipped_fk, ignored_pk, fatal_errors = 0, 0, 0, []
    tg_code_idx = db_column_names_for_insert.index(STANDARDIZED_TG_CODE_COL)
    
    quoted_cols = [f'"{c}"' for c in db_column_names_for_insert]
    placeholders = ', '.join(['?'] * len(quoted_cols))
    insert_sql = f"INSERT OR IGNORE INTO \"{table_name}\" ({', '.join(quoted_cols)}) VALUES ({placeholders})"

    for row_tuple in tqdm(rows_to_insert, desc=f"Inserting into {table_name}", mininterval=0.25, unit="rows", leave=False):
        tg_code_value = row_tuple[tg_code_idx]
        cursor.execute(f"SELECT 1 FROM \"{primary_reference_table_name}\" WHERE \"{STANDARDIZED_TG_CODE_COL}\" = ?", (tg_code_value,))
        if not cursor.fetchone():
            skipped_fk += 1
            continue
        try:
            cursor.execute(insert_sql, row_tuple)
            if cursor.rowcount > 0: inserted_count += 1
            else: ignored_pk += 1
        except sqlite3.IntegrityError as e:
            fatal_errors.append(tg_code_value)
            if len(fatal_errors) <= 5: log_message(LOG_LEVEL_ERROR, f"FATAL UNEXPECTED IntegrityError for TG-Code {tg_code_value} in {table_name}: {e}")
    
    log_message(LOG_LEVEL_INFO, f"For '{table_name}': Inserted={inserted_count}, Skipped (FK)={skipped_fk}, Ignored (PK)={ignored_pk} out of {len(rows_to_insert)} rows.")
    if fatal_errors: log_message(LOG_LEVEL_ERROR, f"Encountered {len(fatal_errors)} TG-Codes with FATAL IntegrityErrors. First 5: {fatal_errors[:5]}")