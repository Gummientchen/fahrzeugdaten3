import os
import csv
import sqlite3
import requests
import re
from pathlib import Path
from tqdm.auto import tqdm # Use tqdm.auto for better notebook/console compatibility
from collections import Counter, defaultdict

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATABASE_DIR = BASE_DIR / "database"
DATABASE_PATH = DATABASE_DIR / "data.db"

# TG-Code column names to look for in CSV headers
TG_CODE_COLUMN_NAMES = ["TG-Code", "Typengenehmigungsnummer"]
# Standardized name for this key column in the database
STANDARDIZED_TG_CODE_COL = "tg_code"

# --- Normalization Configuration ---
# Maps original (sanitized) column names to their lookup table names.
COLUMNS_TO_NORMALIZE_CONFIG = {
    "cars": {
        "col_01_fahrzeugart": "lkp_fahrzeugart",
        "col_02_fahrzeugsubart_code": "lkp_fahrzeugsubart_code",
        "col_02_fahrzeugsubart": "lkp_fahrzeugsubart",
        "col_03_fahrzeugklasse": "lkp_fahrzeugklasse",
        "col_04_marke": "lkp_marken",
        "col_04_typ": "lkp_typen",
        "col_06_vorziffer": "lkp_vorziffer",
        "col_07_karosserieform_code": "lkp_karosserieform_code",
        "col_07_karosserieform": "lkp_karosserieform",
        "col_09_eu_gesamtgenehmigung": "lkp_eu_gesamtgenehmigung",
        "col_10_efko_code": "lkp_efko_code",
        "col_10_hersteller": "lkp_hersteller",
        "col_11_herstellerplakette": "lkp_herstellerplakette",
        "col_12_fahrgestellnummer": "lkp_fahrgestellnummer_struktur",
        "col_14_achsen_raeder": "lkp_achsen_raeder",
        "col_15_federung": "lkp_federung",
        "col_16_lenkung": "lkp_lenkung",
        "col_17_achsantrieb": "lkp_achsantrieb",
        "col_18_getriebe_1": "lkp_getriebe_art",
        "col_18_zuordnung_1": "lkp_getriebe_zuordnung",
        "col_18_getriebe_2": "lkp_getriebe_art",
        "col_18_zuordnung_2": "lkp_getriebe_zuordnung",
        "col_18_getriebe_3": "lkp_getriebe_art",
        "col_18_zuordnung_3": "lkp_getriebe_zuordnung",
        "col_18_getriebe_4": "lkp_getriebe_art",
        "col_18_zuordnung_4": "lkp_getriebe_zuordnung",
        "col_20_betriebsbremse_z1": "lkp_bremssystem_komponente",
        "col_20_betriebsbremse_z2": "lkp_bremssystem_komponente",
        "col_20_betriebsbremse_z3": "lkp_bremssystem_komponente",
        "col_21_hilfsbremse_z1": "lkp_bremssystem_komponente",
        "col_21_hilfsbremse_z2": "lkp_bremssystem_komponente",
        "col_22_feststellbremse": "lkp_bremssystem_komponente",
        "col_25_motor_marke": "lkp_motor_marken",
        "col_25_motor_typ": "lkp_motor_typen",
        "col_26_bauart": "lkp_motor_bauart",
        "col_26_bauart_treibstoff": "lkp_treibstoffe",
        "col_30_abgasreinigung": "lkp_abgasreinigung_systeme",
        "col_31_schalldaempfer_1_art": "lkp_schalldaempfer_art",
        "col_31_schalldaempfer_1_bez": "lkp_schalldaempfer_bezeichnung",
        "col_32_schalldaempfer_2_art": "lkp_schalldaempfer_art",
        "col_32_schalldaempfer_2_bez": "lkp_schalldaempfer_bezeichnung",
        "col_33_schalldaempfer_3_art": "lkp_schalldaempfer_art",
        "col_33_schalldaempfer_3_bez": "lkp_schalldaempfer_bezeichnung",
        "col_34_motorkennzeichen": "lkp_motorkennzeichen_art",
        "col_34_motorkennzeichen_anbringungsort": "lkp_anbringungsorte",
        "col_35_geraeuschdaempfung_z1": "lkp_daempfungsmassnahme",
        "col_35_geraeuschdaempfung_z2": "lkp_daempfungsmassnahme",
        "col_35_geraeuschdaempfung_z3": "lkp_daempfungsmassnahme",
        "col_35_geraeuschdaempfung_z4": "lkp_daempfungsmassnahme",
        "col_38_anzahl_tueren": "lkp_anzahl_tueren",
        "col_39_rueckspiegel": "lkp_rueckspiegel_art",
        "col_55_keine_dachlast": "lkp_dachlast_status",
        "col_56_keine_anhaengelast": "lkp_anhaengelast_status",
        "col_69_reifen_felgen": "lkp_reifen_felgen_kombination",
        "col_70_reifen_felgen": "lkp_reifen_felgen_kombination",
        "col_71_reifen_felgen": "lkp_reifen_felgen_kombination",
        "bemerkungen_z1": "lkp_bemerkungen", "bemerkungen_z2": "lkp_bemerkungen", "bemerkungen_z3": "lkp_bemerkungen", "bemerkungen_z4": "lkp_bemerkungen", "bemerkungen_z5": "lkp_bemerkungen", "bemerkungen_z6": "lkp_bemerkungen", "bemerkungen_z7": "lkp_bemerkungen", "bemerkungen_z8": "lkp_bemerkungen", "bemerkungen_z9": "lkp_bemerkungen", "bemerkungen_z10": "lkp_bemerkungen", "bemerkungen_z11": "lkp_bemerkungen", "bemerkungen_z12": "lkp_bemerkungen", "bemerkungen_z13": "lkp_bemerkungen", "bemerkungen_z14": "lkp_bemerkungen", "bemerkungen_z15": "lkp_bemerkungen", "bemerkungen_z16": "lkp_bemerkungen", "bemerkungen_z17": "lkp_bemerkungen", "bemerkungen_z18": "lkp_bemerkungen", "bemerkungen_z19": "lkp_bemerkungen", "bemerkungen_z20": "lkp_bemerkungen", "bemerkungen_z21": "lkp_bemerkungen", "bemerkungen_z22": "lkp_bemerkungen", "bemerkungen_z23": "lkp_bemerkungen", "bemerkungen_z24": "lkp_bemerkungen",
    },
    "consumption": {
        "marke": "lkp_marken", "typ": "lkp_typen", "treibstoff": "lkp_treibstoffe", "getriebe": "lkp_getriebe_art", "hinweis": "lkp_hinweise", "energieeffizienzkategorie": "lkp_energieeffizienz_kategorie",
    },
    "emissions": {
        "marke": "lkp_marken", "typ": "lkp_typen", "getriebe": "lkp_getriebe_art", "motormarke": "lkp_motor_marken", "motortyp": "lkp_motor_typen", "bauart": "lkp_motor_bauart", "treibstoff": "lkp_treibstoffe", "abgasreinigung": "lkp_abgasreinigung_systeme", "anzahl_achsen_raeder": "lkp_achsen_raeder", "antrieb": "lkp_achsantrieb", "abgascode": "lkp_abgascodes", "emissionscode": "lkp_emissionscodes", "geraeuschcode": "lkp_geraeuschcodes", "bemerkung": "lkp_bemerkungen",
    }
}

# --- Logging Configuration ---
LOG_LEVEL_NONE = 0
LOG_LEVEL_ERROR = 1
LOG_LEVEL_WARNING = 2
LOG_LEVEL_INFO = 3

# Set the desired log level here
CURRENT_LOG_LEVEL = LOG_LEVEL_ERROR

# --- File Processing Configuration ---

FILES_TO_PROCESS = [
    {
        "url": "https://opendata.astra.admin.ch/ivzod/2000-Typengenehmigungen_TG_TARGA/2200-Basisdaten_TG_ab_1995/TG-Automobil.txt",
        "local_name": "TG-Automobil.txt",
        "table_name": "cars",
        "is_primary_key_table": True,  # This table's tg_code is the reference
    },
    {
        "url": "https://opendata.astra.admin.ch/ivzod/2000-Typengenehmigungen_TG_TARGA/2200-Basisdaten_TG_ab_1995/emissionen.txt",
        "local_name": "emissionen.txt",
        "table_name": "emissions",
        "is_primary_key_table": False,
    },
    {
        "url": "https://opendata.astra.admin.ch/ivzod/2000-Typengenehmigungen_TG_TARGA/2200-Basisdaten_TG_ab_1995/verbrauch.txt",
        "local_name": "verbrauch.txt",
        "table_name": "consumption",
        "is_primary_key_table": False,
    },
]

# --- Helper Functions ---

def _log(level: int, message: str):
    """Prints a message if the level is appropriate for the current log setting."""
    if level <= CURRENT_LOG_LEVEL:
        level_prefix = {LOG_LEVEL_ERROR: "ERROR: ", LOG_LEVEL_WARNING: "WARNING: "}.get(level, "")
        print(f"{level_prefix}{message}")

def sanitize_column_name(name_in, index_for_fallback=None):
    """Converts a CSV header name to a SQL-friendly column name."""
    original_name_for_error_msg = name_in
    name = str(name_in).strip().lower()
    
    # Handle German umlauts and ß
    name = name.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue').replace('ß', 'ss')
    
    # Replace common separators (space, dash, dot, slash, parentheses) with underscores
    name = re.sub(r'[\s\-\./\(\)]+', '_', name)
    
    # Remove any characters that are not alphanumeric or underscore
    name = re.sub(r'[^a-z0-9_]', '', name)
    
    # Consolidate multiple underscores into one
    name = re.sub(r'_+', '_', name)
    
    # Remove leading/trailing underscores
    name = name.strip('_')
    
    if not name:
        # If sanitization results in an empty string, use a fallback
        if index_for_fallback is not None:
            _log(LOG_LEVEL_WARNING, f"Original column name '{original_name_for_error_msg}' (at index {index_for_fallback}) sanitized to empty or was originally empty. Using fallback 'col_unnamed_{index_for_fallback}'.")
            return f"col_unnamed_{index_for_fallback}"
        else:
            # This case should ideally not be hit if index is always provided for context
            _log(LOG_LEVEL_WARNING, f"Original column name '{original_name_for_error_msg}' sanitized to empty. Using fallback 'col_unnamed'. This might lead to duplicates if not further processed.")
            return "col_unnamed" # Fallback if no index given
    
    if name and name[0].isdigit(): # Check 'name' is not empty before indexing
        name = f"col_{name}"
    return name

def setup_directories():
    """Creates data and database directories if they don't exist."""
    _log(LOG_LEVEL_INFO, "Setting up directories...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DATABASE_DIR.mkdir(parents=True, exist_ok=True)
    _log(LOG_LEVEL_INFO, f"Data directory will be: {DATA_DIR.resolve()}")
    _log(LOG_LEVEL_INFO, f"Database directory will be: {DATABASE_DIR.resolve()}")

def download_file(url: str, local_path: Path):
    """Downloads a file if it doesn't already exist locally."""
    if local_path.exists():
        _log(LOG_LEVEL_INFO, f"File already exists: {local_path.resolve()}. Skipping download.")
        return
    
    _log(LOG_LEVEL_INFO, f"Downloading {url} to {local_path.resolve()}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
        with open(local_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        _log(LOG_LEVEL_INFO, f"Download complete for {local_path.name}.")
    except requests.exceptions.RequestException as e:
        _log(LOG_LEVEL_ERROR, f"Downloading {url}: {e}")
        # Depending on desired behavior, you might want to exit or raise the error
 
def _count_data_lines_in_file(filepath: Path) -> int:
    """Counts the number of data lines in a file (total lines - 1 for header)."""
    try:
        with open(filepath, 'r', encoding='windows-1252') as f:
            lines = sum(1 for _ in f)
        return max(0, lines - 1) # Subtract 1 for header, ensure non-negative
    except Exception as e:
        _log(LOG_LEVEL_WARNING, f"Could not count lines in {filepath.name} for progress bar: {e}")
        return 0 # Fallback for tqdm if count fails

def _find_tg_code_column_index(header: list, known_tg_code_names: list) -> tuple[int, str] | tuple[None, None]:
    """Finds the index and name of the TG-Code column in the header."""
    for i, col_name in enumerate(header):
        # Ensure col_name is a string before checking
        if col_name in known_tg_code_names:
            return i, col_name
    return None, None

def _generate_final_sql_column_names(header: list, tg_code_col_idx: int, standardized_tg_code_col_name: str) -> list[str] | None:
    """Sanitizes header names for SQL, standardizes the TG-Code column, and handles duplicates."""
    sanitized_column_names = [sanitize_column_name(col, idx) for idx, col in enumerate(header)]
    
    final_sql_column_names = []
    temp_counts = {}  # For de-duplicating names
    for i, san_name in enumerate(sanitized_column_names):
        current_col_name = san_name
        if i == tg_code_col_idx:
            current_col_name = standardized_tg_code_col_name
        elif san_name == standardized_tg_code_col_name:  # Another column sanitized to the key column's standard name
            current_col_name = f"{standardized_tg_code_col_name}_orig_idx_{i}" # Make it unique

        if current_col_name in temp_counts:
            temp_counts[current_col_name] += 1
            final_sql_column_names.append(f"{current_col_name}_{temp_counts[current_col_name]}")
        else:
            temp_counts[current_col_name] = 0
            final_sql_column_names.append(current_col_name)

    if len(final_sql_column_names) != len(set(final_sql_column_names)):
        _log(LOG_LEVEL_ERROR, f"Could not resolve duplicate column names. Original: {header}, Processed: {final_sql_column_names}")
        return None
    return final_sql_column_names

def _process_header_and_column_setup(original_header: list, tg_code_col_idx: int,
                                     table_normalization_rules: dict) -> tuple[list[str], list[str], list[tuple], list[str]]:
    """
    Processes the CSV header to determine final DB column names, types, and transformation plan.
    Returns:
        db_column_definitions_for_create: For CREATE TABLE (e.g., "col_name TEXT", "col_name_id INTEGER").
        db_column_names_for_insert: For INSERT INTO (e.g., "col_name", "col_name_id").
        row_transformation_plan: List of (csv_idx, db_col_name_for_insert, is_normalized, lookup_table_name, is_pk).
        fk_constraints_to_lookup_tables: List of FOREIGN KEY constraint strings for lookup tables.
    """
    db_column_definitions_for_create = []
    db_column_names_for_insert = []
    row_transformation_plan = []
    fk_constraints_to_lookup_tables = []

    temp_db_col_name_counts = {} # For de-duplicating final DB column names if needed

    for csv_idx, original_col_name_csv in enumerate(original_header):
        sanitized_original_col_name = sanitize_column_name(original_col_name_csv, csv_idx)

        if "unnamed" in sanitized_original_col_name:
            _log(LOG_LEVEL_INFO, f"Skipping unnamed column: original '{original_col_name_csv}' (sanitized: {sanitized_original_col_name}) at index {csv_idx}.")
            continue

        is_pk = (csv_idx == tg_code_col_idx)
        db_col_name_final = STANDARDIZED_TG_CODE_COL if is_pk else sanitized_original_col_name
        
        # Handle potential duplicates if a sanitized non-PK column becomes STANDARDIZED_TG_CODE_COL
        if not is_pk and db_col_name_final == STANDARDIZED_TG_CODE_COL:
            db_col_name_final = f"{STANDARDIZED_TG_CODE_COL}_orig_idx_{csv_idx}"

        # Further de-duplicate db_col_name_final if it's already been used
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
            current_db_col_name_for_insert = f"{db_col_name_final}_id" # Append _id for FK columns
            col_def_type = "INTEGER"
            fk_constraints_to_lookup_tables.append(f"FOREIGN KEY (\"{current_db_col_name_for_insert}\") REFERENCES \"{lookup_table_name}\"(id)")

        db_column_definitions_for_create.append(f'"{current_db_col_name_for_insert}" {col_def_type}')
        db_column_names_for_insert.append(current_db_col_name_for_insert)
        row_transformation_plan.append((csv_idx, current_db_col_name_for_insert, is_normalized, lookup_table_name, is_pk))

    return db_column_definitions_for_create, db_column_names_for_insert, row_transformation_plan, fk_constraints_to_lookup_tables


def _create_db_table(cursor: sqlite3.Cursor, table_name: str,
                     column_definitions: list[str],
                     is_main_fk_table: bool, # If this table itself is an FK to the primary_ref_table
                     pk_col_name_for_main_fk: str | None, # The PK col name (e.g. tg_code) for main FK
                     primary_reference_table_name: str | None, # The table it's an FK to (e.g. cars)
                     fk_constraints_to_lookup_tables: list[str]):
    """Creates the database table with appropriate primary and foreign keys."""
    all_definitions = list(column_definitions) # Start with column type definitions

    if is_main_fk_table and primary_reference_table_name and pk_col_name_for_main_fk:
        all_definitions.append(f"FOREIGN KEY(\"{pk_col_name_for_main_fk}\") REFERENCES \"{primary_reference_table_name}\"(\"{pk_col_name_for_main_fk}\")")
    
    all_definitions.extend(fk_constraints_to_lookup_tables) # Add FKs to lookup tables
    
    create_table_sql = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(all_definitions)})"
    
    cursor.execute(create_table_sql)

def _get_or_create_lookup_id(cursor: sqlite3.Cursor, lookup_table_name: str, value: str | None, cache: dict) -> int | None:
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
        except sqlite3.IntegrityError: # Should not happen if value is checked first, but as a safeguard for UNIQUE constraint
            _log(LOG_LEVEL_WARNING, f"IntegrityError on inserting '{value}' into '{lookup_table_name}'. Re-querying.")
            cursor.execute(f"SELECT id FROM \"{lookup_table_name}\" WHERE value = ?", (value,))
            row = cursor.fetchone()
            if row:
                cache[value] = row[0]
                return row[0]
            _log(LOG_LEVEL_ERROR, f"Could not get or create ID for '{value}' in '{lookup_table_name}' after IntegrityError.")
            return None # Should not happen

def _prepare_and_transform_rows_from_csv(cursor: sqlite3.Cursor, csv_reader: csv.reader, csv_filepath: Path,
                                         row_transformation_plan: list[tuple], original_header_len: int,
                                         lookup_caches: defaultdict[str, dict]) -> list[tuple]:
    """Reads, transforms (normalizes), and prepares rows from CSV for DB insertion."""
    transformed_rows_for_db = []
    csv_filename_for_logging = csv_filepath.name
    total_data_rows = _count_data_lines_in_file(csv_filepath)

    # Using enumerate with tqdm to get index 'i' for logging row numbers accurately
    # The iterator for tqdm is the csv_reader itself.
    progress_bar = tqdm(csv_reader, total=total_data_rows, desc=f"Preparing/Transforming {csv_filename_for_logging}", mininterval=0.25, unit="rows", leave=False)
    
    for i, raw_csv_row_list in enumerate(progress_bar):
        current_csv_col_count = len(raw_csv_row_list)
        
        # Pad raw CSV row if it has fewer columns than the original header
        if current_csv_col_count < original_header_len:
            missing_cols_count = original_header_len - current_csv_col_count
            raw_csv_row_list.extend([None] * missing_cols_count)
            _log(LOG_LEVEL_INFO, f"Row {i+2} in {csv_filename_for_logging} (raw) has {current_csv_col_count} cols, expected {original_header_len}. Padding. Data: {raw_csv_row_list[:5]}...")
        elif current_csv_col_count > original_header_len:
            _log(LOG_LEVEL_WARNING, f"Skipping row {i+2} in {csv_filename_for_logging} (raw). Expected {original_header_len} cols, got {current_csv_col_count}. Data: {raw_csv_row_list[:5]}...")
            continue

        single_transformed_row = []
        valid_row = True
        for csv_idx, _, is_normalized, lookup_table_name, _ in row_transformation_plan:
            if csv_idx >= len(raw_csv_row_list): # Should not happen if padding is correct
                _log(LOG_LEVEL_ERROR, f"Row {i+2} in {csv_filename_for_logging}: CSV index {csv_idx} out of bounds for row length {len(raw_csv_row_list)}. Skipping row.")
                valid_row = False
                break
            
            original_value = raw_csv_row_list[csv_idx]

            if is_normalized:
                if lookup_table_name is None: # Should not happen if plan is correct
                     _log(LOG_LEVEL_ERROR, f"Row {i+2} in {csv_filename_for_logging}: Column at CSV index {csv_idx} marked for normalization but no lookup table name. Skipping value.")
                     single_transformed_row.append(None)
                     continue
                
                lookup_id = _get_or_create_lookup_id(cursor, lookup_table_name, original_value, lookup_caches[lookup_table_name])
                single_transformed_row.append(lookup_id)
            else:
                single_transformed_row.append(original_value)
        
        if valid_row:
            transformed_rows_for_db.append(tuple(single_transformed_row))
            
    return transformed_rows_for_db

def _insert_data_generic(cursor: sqlite3.Cursor, table_name: str, final_sql_column_names: list[str], rows_to_insert: list[tuple]):
    """Inserts data into a table using batch INSERT OR IGNORE."""
    if not rows_to_insert:
        _log(LOG_LEVEL_INFO, f"No valid rows found to insert into \"{table_name}\".")
        return

    quoted_final_cols = [f'"{c}"' for c in final_sql_column_names]
    placeholders = ', '.join(['?'] * len(quoted_final_cols)) # Use length of quoted_final_cols
    insert_sql = f"INSERT OR IGNORE INTO \"{table_name}\" ({', '.join(quoted_final_cols)}) VALUES ({placeholders})"
    
    cursor.executemany(insert_sql, rows_to_insert)
    _log(LOG_LEVEL_INFO, f"Inserted/updated data for {cursor.rowcount} rows into \"{table_name}\" (out of {len(rows_to_insert)} valid rows from CSV).")
 
def _insert_data_emissions_special(cursor: sqlite3.Cursor, table_name: str, final_sql_column_names: list[str],
                                   rows_to_insert: list[tuple], pk_col_name: str, primary_reference_table_name: str):
    """Inserts data into the 'emissions' table with special FK pre-checks."""
    if not rows_to_insert:
        _log(LOG_LEVEL_INFO, f"No valid rows found to insert into \"{table_name}\".")
        return

    _log(LOG_LEVEL_INFO, f"Using row-by-row insert with pre-FK check for table '{table_name}'...")
    inserted_count = 0
    skipped_due_to_fk_count = 0
    ignored_due_to_pk_on_emissions_count = 0
    fatal_error_tg_codes = []

    try:
        tg_code_idx_in_row_tuple = final_sql_column_names.index(pk_col_name)
    except ValueError:
        _log(LOG_LEVEL_ERROR, f"Could not find '{pk_col_name}' in final_sql_column_names for table {table_name}. Aborting import for this table.")
        return

    quoted_final_cols = [f'"{c}"' for c in final_sql_column_names]
    placeholders = ', '.join(['?'] * len(quoted_final_cols)) # Use length of quoted_final_cols
    insert_sql = f"INSERT OR IGNORE INTO \"{table_name}\" ({', '.join(quoted_final_cols)}) VALUES ({placeholders})"

    # Using enumerate with tqdm for progress bar
    progress_bar = tqdm(rows_to_insert, total=len(rows_to_insert), desc=f"Inserting into {table_name}", mininterval=0.25, unit="rows", leave=False)
    for i, row_tuple in enumerate(progress_bar):
        tg_code_value = row_tuple[tg_code_idx_in_row_tuple]

        cursor.execute(f"SELECT 1 FROM \"{primary_reference_table_name}\" WHERE \"{pk_col_name}\" = ?", (tg_code_value,))
        if not cursor.fetchone():
            skipped_due_to_fk_count += 1
            if skipped_due_to_fk_count <= 5:
                _log(LOG_LEVEL_INFO, f"Pre-check: Skipping data for TG-Code {tg_code_value} ({table_name}) due to missing FK in '{primary_reference_table_name}'.")
            continue

        try:
            cursor.execute(insert_sql, row_tuple)
            if cursor.rowcount > 0:
                inserted_count += 1
            else:
                ignored_due_to_pk_on_emissions_count += 1
        except sqlite3.IntegrityError as e_fatal:
            fatal_error_tg_codes.append(tg_code_value)
            if len(fatal_error_tg_codes) <= 5:
                _log(LOG_LEVEL_ERROR, f"FATAL UNEXPECTED IntegrityError for TG-Code {tg_code_value} in {table_name} (row {i}) AFTER FK pre-check: {e_fatal}")
    
    _log(LOG_LEVEL_INFO, f"For '{table_name}': Inserted={inserted_count}, Skipped (FK pre-check)={skipped_due_to_fk_count}, Ignored (PK on {table_name})={ignored_due_to_pk_on_emissions_count} out of {len(rows_to_insert)} rows.")
    if fatal_error_tg_codes:
        _log(LOG_LEVEL_ERROR, f"Encountered {len(fatal_error_tg_codes)} TG-Codes with FATAL IntegrityErrors AFTER FK pre-check. First 5: {fatal_error_tg_codes[:5]}")
 
def import_data_to_db(db_conn: sqlite3.Connection, file_config: dict, data_dir_path: Path,
                        primary_reference_table_name: str | None,
                        normalization_rules: dict, lookup_caches: defaultdict[str, dict]):
    """Imports data from a single CSV file into the specified database table."""
    local_filepath = data_dir_path / file_config["local_name"]
    table_name = file_config["table_name"]
    is_main_fk_table = not file_config["is_primary_key_table"] # True if this table has FK to primary_reference_table_name

    _log(LOG_LEVEL_INFO, f"\nProcessing file: {local_filepath.resolve()} for table: {table_name}")

    if not local_filepath.exists():
        _log(LOG_LEVEL_WARNING, f"File not found: {local_filepath.resolve()}. Skipping import for this file.")
        return

    try:
        with open(local_filepath, 'r', encoding='windows-1252') as f_csv:
            reader = csv.reader(f_csv, delimiter='\t')
            header = next(reader) # First row is the header
            original_header_len = len(header)
            
            tg_code_col_idx, _ = _find_tg_code_column_index(header, TG_CODE_COLUMN_NAMES)
            if tg_code_col_idx == -1:
                _log(LOG_LEVEL_ERROR, f"Key column (e.g. 'TG-Code') not found in {local_filepath.name}.")
                _log(LOG_LEVEL_ERROR, f"  Expected one of: {TG_CODE_COLUMN_NAMES}")
                _log(LOG_LEVEL_ERROR, f"  Found headers: {header}")
                return            

            table_normalization_rules = normalization_rules.get(table_name, {})
            db_col_defs_create, db_col_names_insert, row_transform_plan, main_tbl_fks_to_lookup = \
                _process_header_and_column_setup(header, tg_code_col_idx, table_normalization_rules)

            if not db_col_names_insert: # No columns to insert (e.g., all were unnamed or TG-Code missing)
                _log(LOG_LEVEL_WARNING, f"No processable columns found for {local_filepath.name} after header processing. Skipping import.")
                return

            cursor = db_conn.cursor()
            _create_db_table(cursor, table_name, db_col_defs_create,
                             is_main_fk_table, STANDARDIZED_TG_CODE_COL, primary_reference_table_name,
                             main_tbl_fks_to_lookup)

            rows_to_insert = _prepare_and_transform_rows_from_csv(cursor, reader, local_filepath,
                                                                  row_transform_plan, original_header_len, lookup_caches)

            if table_name == "emissions": # Special handling for emissions table
                _insert_data_emissions_special(cursor, table_name, db_col_names_insert, rows_to_insert,
                                               STANDARDIZED_TG_CODE_COL, primary_reference_table_name)
            else:
                _insert_data_generic(cursor, table_name, db_col_names_insert, rows_to_insert)
            
            db_conn.commit()

    except FileNotFoundError: # Should be caught by earlier check, but as a safeguard
        _log(LOG_LEVEL_WARNING, f"File not found: {local_filepath.resolve()}. Make sure it's downloaded.")
    except ValueError as e: # From sanitize_column_name if a header becomes empty
        _log(LOG_LEVEL_ERROR, f"Processing column names for {local_filepath.name}: {e}")
    except Exception as e:
        _log(LOG_LEVEL_ERROR, f"An error occurred while processing {local_filepath.name}: {e}")
        if CURRENT_LOG_LEVEL >= LOG_LEVEL_ERROR:
            import traceback
            traceback.print_exc()

# --- Main Logic ---
def main():
    _log(LOG_LEVEL_INFO, "Starting the data import process...")
    
    # Create necessary directories
    setup_directories()

    # Download files if they don't exist
    _log(LOG_LEVEL_INFO, "\n--- Stage 1: Downloading files ---")
    for file_info in FILES_TO_PROCESS:
        local_file_path = DATA_DIR / file_info["local_name"]
        download_file(file_info["url"], local_file_path)

    # Connect to SQLite database (it will be created if it doesn't exist)
    _log(LOG_LEVEL_INFO, f"\n--- Stage 2: Importing data into SQLite database: {DATABASE_PATH.resolve()} ---")
    db_conn = None # Initialize to ensure it's defined for finally block

    primary_table_details = next((f for f in FILES_TO_PROCESS if f.get("is_primary_key_table")), None)
    if not primary_table_details:
        _log(LOG_LEVEL_ERROR, "No primary key table defined in FILES_TO_PROCESS. Cannot establish foreign key relationships.")
        return
    primary_ref_table_name = primary_table_details["table_name"]

    if DATABASE_PATH.exists():
        _log(LOG_LEVEL_INFO, f"Deleting existing database file to ensure a clean run: {DATABASE_PATH.resolve()}")
        DATABASE_PATH.unlink()

    try:
        db_conn = sqlite3.connect(DATABASE_PATH)
        # Enable foreign key support (good practice, off by default in older SQLite versions for CLI)
        db_conn.execute("PRAGMA foreign_keys = ON;")

        # Create all unique lookup tables first
        cursor = db_conn.cursor()
        all_lookup_table_names = set()
        for config_per_table in COLUMNS_TO_NORMALIZE_CONFIG.values():
            for lookup_name in config_per_table.values():
                all_lookup_table_names.add(lookup_name)
        
        for lookup_name in sorted(list(all_lookup_table_names)): # Sort for deterministic creation order
            _log(LOG_LEVEL_INFO, f"Ensuring lookup table '{lookup_name}' exists...")
            cursor.execute(f"CREATE TABLE IF NOT EXISTS \"{lookup_name}\" (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT UNIQUE NOT NULL)")
        db_conn.commit()

        # Process files: primary table first to satisfy foreign key constraints
        # Sort files so that 'is_primary_key_table = True' comes first.
        sorted_files_to_process = sorted(FILES_TO_PROCESS, key=lambda x: not x['is_primary_key_table'])
        lookup_caches = defaultdict(dict) # Initialize caches for lookup IDs
        for file_config in sorted_files_to_process:
            import_data_to_db(db_conn, file_config, DATA_DIR, primary_ref_table_name, COLUMNS_TO_NORMALIZE_CONFIG, lookup_caches)
        
        _log(LOG_LEVEL_INFO, "\nData import process finished successfully!")

    except sqlite3.Error as e:
        _log(LOG_LEVEL_ERROR, f"A database error occurred: {e}")
    except Exception as e:
        _log(LOG_LEVEL_ERROR, f"An unexpected error occurred during the main process: {e}")
        if CURRENT_LOG_LEVEL >= LOG_LEVEL_ERROR:
            import traceback
            traceback.print_exc()
    finally:
        if db_conn:
            db_conn.close()
            _log(LOG_LEVEL_INFO, "\nDatabase connection closed.")

if __name__ == "__main__":
    main()