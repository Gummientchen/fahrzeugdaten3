from flask import Flask, render_template, request, jsonify # Added jsonify
import sqlite3 # For autocomplete database access

from config import DATABASE_PATH, STANDARDIZED_TG_CODE_COL, COLUMNS_TO_NORMALIZE_CONFIG
from search import search_car_data 
from logger import set_log_level, LOG_LEVEL_INFO, LOG_LEVEL_ERROR
from display_config import (
    DISPLAY_LABELS,
    DATA_GROUPS_ORDER,
    DATA_GROUPS_MAPPING,
    LABELS_TO_EXCLUDE,
    FIELD_UNITS
)

app = Flask(__name__)

# Optional: Set a log level for webapp operations if different from default
# set_log_level(LOG_LEVEL_INFO) 

def _is_value_valid_for_display(value_to_check):
    """
    Checks if a value is considered valid for display (not None, not empty string, not 0 or "0").
    Returns a tuple: (bool_is_valid, processed_value_or_None).
    """
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
            # Not a float-convertible string (e.g., "abc"), so it's not "0".
            processed_value = stripped_value 
    elif isinstance(value_to_check, (int, float)):
        if value_to_check == 0: return False, None
        # For non-zero numbers, processed_value is already value_to_check
    # For any other data types (booleans, etc.), they are considered valid if not None.

    return True, processed_value

def _process_single_car_result(row_dict, display_labels_config, data_groups_order_config, 
                               data_groups_mapping_config, field_units_config, labels_to_exclude_config, 
                               standardized_tg_code_col_config):
    """
    Processes a single car data row (dictionary) into a grouped and formatted structure for display.
    """
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

            if bis_key in row_dict:
                processed_db_keys.add(bis_key)

            is_von_valid, processed_von = _is_value_valid_for_display(val_von)
            is_bis_valid, processed_bis = _is_value_valid_for_display(val_bis)

            if is_von_valid and is_bis_valid:
                str_von, str_bis = str(processed_von), str(processed_bis)
                value_to_display_final = str_von if str_von == str_bis else f"{str_von} - {str_bis}"
            elif is_von_valid:
                value_to_display_final = str(processed_von)
            elif is_bis_valid:
                value_to_display_final = str(processed_bis)
            key_for_display_lookup = base_name
        else:
            is_a_tg_code_field = standardized_tg_code_col_config in db_key 
            if db_key.endswith("_id") and not is_a_tg_code_field and db_key not in display_labels_config:
                continue 

            is_val_valid, processed_val = _is_value_valid_for_display(value)
            if is_val_valid:
                value_to_display_final = str(processed_val)
        
        if value_to_display_final is not None:
            final_display_label = display_labels_config.get(key_for_display_lookup, 
                                                             key_for_display_lookup.replace("_", " ").title())
            
            if final_display_label in labels_to_exclude_config:
                continue

            value_for_group = value_to_display_final 
            if final_display_label == "Elektrischer Verbrauch WLTP":
                parts_original_str = value_to_display_final.split(' - ')
                formatted_wh_km_parts = []
                formatted_kwh_100km_parts = []
                formatted_km_kwh_parts = [] 
                conversion_possible_for_all = True

                for part_str in parts_original_str:
                    try:
                        val_numeric_wh_km = float(part_str)
                        if val_numeric_wh_km == int(val_numeric_wh_km): formatted_wh_km_parts.append(str(int(val_numeric_wh_km)))
                        else: formatted_wh_km_parts.append(f"{val_numeric_wh_km:.1f}")
                        
                        val_kwh_100km = val_numeric_wh_km / 10.0
                        if val_kwh_100km == int(val_kwh_100km): formatted_kwh_100km_parts.append(str(int(val_kwh_100km)))
                        else: formatted_kwh_100km_parts.append(f"{val_kwh_100km:.1f}")
                        
                        val_km_kwh = 1000.0 / val_numeric_wh_km if val_numeric_wh_km != 0 else 0 
                        formatted_km_kwh_parts.append(f"{val_km_kwh:.1f}")
                    except ValueError: 
                        conversion_possible_for_all = False
                        break 
                
                if conversion_possible_for_all:
                    display_wh_km_str = " - ".join(formatted_wh_km_parts)
                    display_kwh_100km_str = " - ".join(formatted_kwh_100km_parts)
                    display_km_kwh_str = " - ".join(formatted_km_kwh_parts) 
                    value_for_group = f"{display_wh_km_str} Wh/km | {display_kwh_100km_str} kWh/100km | {display_km_kwh_str} km/kWh"
                else: 
                    primary_unit = field_units_config.get(final_display_label) 
                    if primary_unit: value_for_group = f"{value_to_display_final} {primary_unit}"
            else: 
                unit = field_units_config.get(final_display_label)
                if unit:
                    value_for_group = f"{value_to_display_final} {unit}"

            group_name = data_groups_mapping_config.get(final_display_label)
            if not group_name: 
                key_lower = key_for_display_lookup.lower()
                if "bemerkung" in key_lower: 
                    group_name = "Bemerkungen"
                else:
                    group_name = "Sonstige Daten"
            
            if group_name not in grouped_data: 
                grouped_data[group_name] = {}
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
        marke_id_input_str = request.form.get('marke_id')
        typ_id_input_str = request.form.get('typ_id')

        col_04_marke_id = int(marke_id_input_str) if marke_id_input_str and marke_id_input_str.strip() else None
        col_04_typ_id = int(typ_id_input_str) if typ_id_input_str and typ_id_input_str.strip() else None
        tg_code = tg_code_input if tg_code_input and tg_code_input.strip() else None
        marke_str = marke_str_input if marke_str_input and marke_str_input.strip() else None
        typ_str = typ_str_input if typ_str_input and typ_str_input.strip() else None

        if not any([tg_code, marke_str, typ_str, col_04_marke_id is not None, col_04_typ_id is not None]):
            error_message = "Please enter at least one search criterion."
        else:
            try:
                results_from_search = search_car_data(
                    db_path=DATABASE_PATH,
                    tg_code=tg_code,
                    marke_str=marke_str,
                    typ_str=typ_str,
                    col_04_marke_id=col_04_marke_id,
                    col_04_typ_id=col_04_typ_id
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

    # If it's a single result, pass the specific labels needed for history saving
    if results_from_search and len(results_from_search) == 1:
        # These keys must match the keys used in DISPLAY_LABELS for these core fields
        template_context['actual_tg_code_label'] = DISPLAY_LABELS.get("cars_tg_code", "TG-Code (Typen&shy;genehmigungs&shy;nummer)")
        template_context['actual_marke_label'] = DISPLAY_LABELS.get("cars_col_04_marke_value", "Marke")
        template_context['actual_typ_label'] = DISPLAY_LABELS.get("cars_col_04_typ_value", "Typ")

    return render_template('search_results.html', **template_context)

@app.route('/autocomplete/marken')
def autocomplete_marken():
    term = request.args.get('term', '')
    if not term or len(term) < 1: 
        return jsonify([])

    marke_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_marke")
    if not marke_lookup_table:
        app.logger.error("Lookup table for 'col_04_marke' not defined in config.")
        return jsonify({"error": "Server configuration error for marken autocomplete"}), 500

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
        return jsonify({"error": "Database error during marken autocomplete"}), 500
        
    return jsonify(suggestions)

@app.route('/autocomplete/typen')
def autocomplete_typen():
    term = request.args.get('term', '')
    marke_value_filter = request.args.get('marke', None) 

    if not term or len(term) < 1: 
        return jsonify([])

    typ_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_typ")
    marke_lookup_table = COLUMNS_TO_NORMALIZE_CONFIG.get("cars", {}).get("col_04_marke")
    cars_table_name = "cars" 

    if not typ_lookup_table or (marke_value_filter and not marke_lookup_table):
        app.logger.error("Lookup table for typ or marke (if filtering) not defined in config.")
        return jsonify({"error": "Server configuration error for typen autocomplete"}), 500

    suggestions = []
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query_params = [f'{term}%']
        
        if marke_value_filter and marke_value_filter.strip():
            query = f"""
                SELECT DISTINCT typ_lkp.value
                FROM "{typ_lookup_table}" typ_lkp
                JOIN "{cars_table_name}" c ON c.col_04_typ_id = typ_lkp.id
                JOIN "{marke_lookup_table}" marke_lkp ON c.col_04_marke_id = marke_lkp.id
                WHERE typ_lkp.value LIKE ? AND marke_lkp.value = ?
                ORDER BY typ_lkp.value
                LIMIT 10
            """
            query_params.append(marke_value_filter)
        else: 
            query = f"SELECT DISTINCT value FROM \"{typ_lookup_table}\" WHERE value LIKE ? ORDER BY value LIMIT 10"

        cursor.execute(query, tuple(query_params))
        suggestions = [row['value'] for row in cursor.fetchall()]
        conn.close()
    except Exception as e:
        app.logger.error(f"Error in /autocomplete/typen: {e}")
        return jsonify({"error": "Database error during typen autocomplete"}), 500
    return jsonify(suggestions)

if __name__ == '__main__':
    app.run(debug=True)
