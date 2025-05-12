from flask import Flask, render_template, request, jsonify
import sqlite3
import os
import logging # Using standard logging
import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from pathlib import Path # For DATABASE_PATH if it's a Path object

from config import DATABASE_PATH, STANDARDIZED_TG_CODE_COL, COLUMNS_TO_NORMALIZE_CONFIG
from search import search_car_data 
# from logger import set_log_level, LOG_LEVEL_INFO, LOG_LEVEL_ERROR # Your custom logger
from display_config import (
    DISPLAY_LABELS,
    DATA_GROUPS_ORDER,
    DATA_GROUPS_MAPPING,
    LABELS_TO_EXCLUDE,
    FIELD_UNITS
)
# Import the main logic from your importer script
try:
    # Assuming main_importer_logic is the function that does the full import
    from importer import main_importer_logic as run_full_import 
except ImportError as e_initial_import: # Catch the specific error
    # Log the actual ImportError to get more details about why the import failed
    logging.getLogger(__name__).error(
        f"CRITICAL: Failed to import 'main_importer_logic' from importer.py during initial app load. Specific error: {e_initial_import}",
        exc_info=True # This will include the full traceback of the ImportError
    )
    def run_full_import_placeholder(): # Placeholder if import fails
        logging.getLogger(__name__).error("Placeholder Importer: Importer script function 'main_importer_logic' is not available due to earlier import failure.")
    run_full_import = run_full_import_placeholder # Assign the placeholder


app = Flask(__name__)
# Configure Flask's built-in logger to be more verbose for Docker
gunicorn_logger = logging.getLogger('gunicorn.error')
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level if gunicorn_logger.level != 0 else logging.INFO) # Ensure a default if gunicorn level is 0 (NOTSET)


# --- Initial Data Import ---
def perform_initial_data_import():
    # This function will be called once when the app starts (due to Gunicorn --preload)
    # Convert DATABASE_PATH to string if it's a Path object for os.path.exists
    db_path_str = str(DATABASE_PATH)
    if not os.path.exists(db_path_str):
        app.logger.info(f"Database not found at {db_path_str}. Starting initial data import...")
        try:
            run_full_import()
            app.logger.info("Initial data import completed successfully.")
        except Exception as e:
            app.logger.error(f"Error during initial data import: {e}", exc_info=True)
    else:
        app.logger.info(f"Database found at {db_path_str}. Skipping initial import.")

# --- Scheduled Data Update ---
def scheduled_update_job():
    with app.app_context(): # Ensure Flask app context is available if needed by importer
        app.logger.info("Starting scheduled weekly data update...")
        try:
            run_full_import() # This should drop tables and re-import
            app.logger.info("Scheduled weekly data update completed successfully.")
        except Exception as e:
            app.logger.error(f"Error during scheduled weekly data update: {e}", exc_info=True)

# --- APScheduler Setup ---
scheduler = BackgroundScheduler(daemon=True, timezone="Europe/Berlin") # Set your timezone
# Schedule job to run every Monday at 3 AM
scheduler.add_job(scheduled_update_job, 'cron', day_of_week='mon', hour=3, minute=0)
# For testing:
# scheduler.add_job(scheduled_update_job, 'interval', minutes=10) # Test every 10 mins
# scheduler.add_job(scheduled_update_job, 'date', run_date='2023-10-27 15:00:00') # Test at specific time


def _is_value_valid_for_display(value_to_check):
    if value_to_check is None:
        return False, None
    processed_value = value_to_check 
    if isinstance(value_to_check, str):
        stripped_value = value_to_check.strip()
        if not stripped_value: return False, None
        try:
            if float(stripped_value) == 0: return False, None
            processed_value = stripped_value 
        except ValueError:
            processed_value = stripped_value 
    elif isinstance(value_to_check, (int, float)):
        if value_to_check == 0: return False, None
    return True, processed_value

def _process_single_car_result(row_dict, display_labels_config, data_groups_order_config, 
                               data_groups_mapping_config, field_units_config, labels_to_exclude_config, 
                               standardized_tg_code_col_config):
    grouped_data = {group_name: {} for group_name in data_groups_order_config}
    processed_db_keys = set()

    for db_key, value in row_dict.items():
        if db_key in processed_db_keys:
            continue

        key_for_display_lookup = db_key
        value_to_display_final = None 

        if db_key.endswith("_von"):
            base_name = db_key[:-4]
            bis_key = base_name + "_bis"
            val_von = value
            val_bis = row_dict.get(bis_key)
            if bis_key in row_dict: processed_db_keys.add(bis_key)
            is_von_valid, processed_von = _is_value_valid_for_display(val_von)
            is_bis_valid, processed_bis = _is_value_valid_for_display(val_bis)
            if is_von_valid and is_bis_valid:
                str_von, str_bis = str(processed_von), str(processed_bis)
                value_to_display_final = str_von if str_von == str_bis else f"{str_von} - {str_bis}"
            elif is_von_valid: value_to_display_final = str(processed_von)
            elif is_bis_valid: value_to_display_final = str(processed_bis)
            key_for_display_lookup = base_name
        else:
            # For _id fields that were NOT denormalized by search.py, their db_key is like 'cars_col_01_fahrzeugart_id'
            # For fields that WERE denormalized by search.py, db_key is like 'cars_col_04_marke_value'
            # For direct fields, db_key is like 'cars_col_37_anzahl_plaetze_vorne'
            
            is_val_valid, processed_val = _is_value_valid_for_display(value)
            if is_val_valid: value_to_display_final = str(processed_val)
        
        if value_to_display_final is not None:
            # Determine the correct label for display
            # If db_key ends with _id, try to find label for corresponding _value field
            if db_key.endswith("_id"):
                value_field_key = db_key[:-3] + "_value" # e.g. cars_col_01_fahrzeugart_value
                final_display_label = display_labels_config.get(value_field_key, value_field_key.replace("_", " ").title())
            else: # For _value fields or direct fields or _von/_bis base names
                final_display_label = display_labels_config.get(key_for_display_lookup, key_for_display_lookup.replace("_", " ").title())

            if final_display_label in labels_to_exclude_config:
                continue

            value_for_group = value_to_display_final 
            if final_display_label == "Elektrischer Verbrauch WLTP":
                parts_original_str = value_to_display_final.split(' - ')
                formatted_wh_km_parts, formatted_kwh_100km_parts, formatted_km_kwh_parts = [], [], []
                conversion_possible_for_all = True
                for part_str in parts_original_str:
                    try:
                        val_numeric_wh_km = float(part_str)
                        formatted_wh_km_parts.append(str(int(val_numeric_wh_km)) if val_numeric_wh_km == int(val_numeric_wh_km) else f"{val_numeric_wh_km:.1f}")
                        val_kwh_100km = val_numeric_wh_km / 10.0
                        formatted_kwh_100km_parts.append(str(int(val_kwh_100km)) if val_kwh_100km == int(val_kwh_100km) else f"{val_kwh_100km:.1f}")
                        val_km_kwh = 1000.0 / val_numeric_wh_km if val_numeric_wh_km != 0 else 0 
                        formatted_km_kwh_parts.append(f"{val_km_kwh:.1f}")
                    except ValueError: conversion_possible_for_all = False; break 
                if conversion_possible_for_all:
                    value_for_group = f"{' - '.join(formatted_wh_km_parts)} Wh/km | {' - '.join(formatted_kwh_100km_parts)} kWh/100km | {' - '.join(formatted_km_kwh_parts)} km/kWh"
                else: 
                    primary_unit = field_units_config.get(final_display_label) 
                    if primary_unit: value_for_group = f"{value_to_display_final} {primary_unit}"
            else: 
                unit = field_units_config.get(final_display_label)
                if unit: value_for_group = f"{value_to_display_final} {unit}"

            group_name = data_groups_mapping_config.get(final_display_label)
            if not group_name: 
                key_lower = final_display_label.lower() # Use final_display_label for grouping fallback
                if "bemerkung" in key_lower: group_name = "Bemerkungen"
                else: group_name = "Sonstige Daten"
            
            if group_name not in grouped_data: grouped_data[group_name] = {}
            grouped_data[group_name][final_display_label] = value_for_group
    
    final_ordered_groups = {}
    for group_name_ordered in data_groups_order_config:
        if group_name_ordered in grouped_data and grouped_data[group_name_ordered]:
            final_ordered_groups[group_name_ordered] = grouped_data[group_name_ordered]
    return final_ordered_groups

@app.route('/', methods=['GET', 'POST'])
def index():
    results_from_search = []
    search_performed = False
    error_message = None

    if request.method == 'POST':
        search_performed = True
        tg_code_input = request.form.get('tg_code')
        marke_str_input = request.form.get('marke')
        typ_str_input = request.form.get('typ')
        year_input = request.form.get('year') # Get the year input
        # Removed marke_id and typ_id from form, so these will be None
        col_04_marke_id = None
        col_04_typ_id = None
        tg_code = tg_code_input.strip().upper() if tg_code_input and tg_code_input.strip() else None
        marke_str = marke_str_input if marke_str_input and marke_str_input.strip() else None
        typ_str = typ_str_input if typ_str_input and typ_str_input.strip() else None
        year_str = year_input.strip() if year_input and year_input.strip() else None

        if not any([tg_code, marke_str, typ_str, year_str]): # Updated check
            error_message = "Please enter at least one search criterion (TG-Code, Marke, Typ, or Year)."
        else:
            try:
                # Convert DATABASE_PATH to Path object if it's a string
                db_path_obj = Path(DATABASE_PATH) if isinstance(DATABASE_PATH, str) else DATABASE_PATH
                results_from_search = search_car_data(
                    db_path=db_path_obj,
                    tg_code=tg_code,
                    marke_str=marke_str,
                    typ_str=typ_str,
                    year_str=year_str # Pass the year string
                    # col_04_marke_id and col_04_typ_id are not passed as they are None
                )
            except Exception as e:
                error_message = f"An error occurred during the search: {e}"
                app.logger.error(f"Search error: {e}", exc_info=True)

    data_for_template = [] 
    if results_from_search:
        if len(results_from_search) == 1:
            processed_single_result = _process_single_car_result(
                results_from_search[0],
                DISPLAY_LABELS, 
                DATA_GROUPS_ORDER, 
                DATA_GROUPS_MAPPING, 
                FIELD_UNITS, 
                LABELS_TO_EXCLUDE,
                STANDARDIZED_TG_CODE_COL 
            )
            if processed_single_result: 
                data_for_template.append(processed_single_result)
        else: 
            data_for_template = results_from_search

    template_context = {
        'results': data_for_template,
        'search_performed': search_performed,
        'error_message': error_message,
        'data_groups_order': DATA_GROUPS_ORDER
    }
    if results_from_search and len(results_from_search) == 1:
        template_context['actual_tg_code_label'] = DISPLAY_LABELS.get("cars_tg_code", "TG-Code (Typen&shy;genehmigungs&shy;nummer)")
        template_context['actual_marke_label'] = DISPLAY_LABELS.get("cars_col_04_marke_value", "Marke")
        template_context['actual_typ_label'] = DISPLAY_LABELS.get("cars_col_04_typ_value", "Typ")
    return render_template('search_results.html', **template_context)

@app.route('/autocomplete/marken')
def autocomplete_marken():
    term = request.args.get('term', '')
    if not term or len(term) < 1: return jsonify([])
    marke_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_marke")
    if not marke_lookup_table:
        app.logger.error("Lookup table for 'col_04_marke' not defined in config.")
        return jsonify({"error": "Server configuration error"}), 500
    suggestions = []
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query = f"SELECT DISTINCT value FROM \"{marke_lookup_table}\" WHERE value LIKE ? ORDER BY value LIMIT 10"
        cursor.execute(query, (f'{term}%',)) 
        suggestions = [row['value'] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        app.logger.error(f"Error in /autocomplete/marken: {e}")
        return jsonify({"error": "Database error"}), 500
    return jsonify(suggestions)

@app.route('/autocomplete/typen')
def autocomplete_typen():
    term = request.args.get('term', '')
    marke_value_filter = request.args.get('marke', None) 
    if not term or len(term) < 1: return jsonify([])
    typ_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_typ")
    marke_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_marke")
    cars_table_name = "cars" 
    if not typ_lookup_table or (marke_value_filter and not marke_lookup_table):
        app.logger.error("Lookup table for typ or marke not defined in config.")
        return jsonify({"error": "Server configuration error"}), 500
    suggestions = []
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        query_params = [f'{term}%']
        if marke_value_filter and marke_value_filter.strip():
            query = f"""
                SELECT DISTINCT typ_lkp.value FROM "{typ_lookup_table}" typ_lkp
                JOIN "{cars_table_name}" c ON c.col_04_typ_id = typ_lkp.id
                JOIN "{marke_lookup_table}" marke_lkp ON c.col_04_marke_id = marke_lkp.id
                WHERE typ_lkp.value LIKE ? AND marke_lkp.value = ? ORDER BY typ_lkp.value LIMIT 10"""
            query_params.append(marke_value_filter)
        else: 
            query = f"SELECT DISTINCT value FROM \"{typ_lookup_table}\" WHERE value LIKE ? ORDER BY value LIMIT 10"
        cursor.execute(query, tuple(query_params))
        suggestions = [row['value'] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        app.logger.error(f"Error in /autocomplete/typen: {e}")
        return jsonify({"error": "Database error"}), 500
    return jsonify(suggestions)

# --- Application Initialization for Gunicorn ---
if os.environ.get("WERKZEUG_RUN_MAIN") != "true": 
    perform_initial_data_import()
    if not scheduler.running:
        try:
            scheduler.start()
            app.logger.info("APScheduler started for periodic updates.")
            atexit.register(lambda: scheduler.shutdown())
        except Exception as e:
            app.logger.error(f"Failed to start APScheduler: {e}", exc_info=True)

if __name__ == '__main__':
    app.logger.info("Starting Flask development server...")
    db_path_str_main = str(DATABASE_PATH)
    if not os.path.exists(db_path_str_main):
        app.logger.info(f"Database not found at {db_path_str_main} for local dev. Running initial import.")
        perform_initial_data_import() # Call it directly for local dev if DB doesn't exist
    
    if not scheduler.running and not os.environ.get("WERKZEUG_RUN_MAIN"): # Avoid double start with reloader
        try:
            scheduler.start()
            app.logger.info("APScheduler started for local development.")
            atexit.register(lambda: scheduler.shutdown())
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
        except Exception as e:
            app.logger.error(f"Failed to start APScheduler for local dev: {e}", exc_info=True)
            
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=True)
