import sqlite3
import argparse
from config import DATABASE_PATH, STANDARDIZED_TG_CODE_COL
from search import search_car_data
from logger import log_message, LOG_LEVEL_INFO, LOG_LEVEL_WARNING, LOG_LEVEL_ERROR, LOG_LEVEL_NONE, set_log_level

def run_example_searches():
    log_message(LOG_LEVEL_INFO, "\n--- Example Search ---")
    if not DATABASE_PATH.exists():
        log_message(LOG_LEVEL_WARNING, "Database file does not exist. Cannot run search examples.")
        return

    sample_tg_code, sample_marke_id = None, None
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute(f"SELECT \"{STANDARDIZED_TG_CODE_COL}\", \"col_04_marke_id\" FROM cars WHERE \"col_04_marke_id\" IS NOT NULL LIMIT 1")
        row = cursor.fetchone()
        if row: sample_tg_code, sample_marke_id = row[0], row[1]
        conn.close()
    except Exception as e:
        log_message(LOG_LEVEL_ERROR, f"Error fetching sample data: {e}")

    if sample_tg_code:
        log_message(LOG_LEVEL_INFO, f"\nSearching for TG-Code: {sample_tg_code}")
        results = search_car_data(DATABASE_PATH, tg_code=sample_tg_code)
        if results:
            log_message(LOG_LEVEL_INFO, f"Found {len(results)} match(es). Details of the first result (non-None values):")
            first_result_tg = results[0]
            for key, value in first_result_tg.items():
                if value is not None:
                    log_message(LOG_LEVEL_INFO, f"  {key}: {value}")
        else:
            log_message(LOG_LEVEL_INFO, "No results found.")

    if sample_marke_id is not None:
        log_message(LOG_LEVEL_INFO, f"\nSearching for Marke ID: {sample_marke_id}")
        results = search_car_data(DATABASE_PATH, col_04_marke_id=sample_marke_id)
        if results:
            log_message(LOG_LEVEL_INFO, f"Found {len(results)} match(es) for Marke ID {sample_marke_id} (showing key details for first 5):")
            for i, car_data in enumerate(results[:5]):
                log_message(LOG_LEVEL_INFO, f"  Result {i+1}: TG-Code: {car_data.get('cars_tg_code')}, Marke: {car_data.get('cars_col_04_marke_value')}, Typ: {car_data.get('cars_col_04_typ_value')}")
        else:
            log_message(LOG_LEVEL_INFO, "No results found.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Search for car data in the database.")
    parser.add_argument("--tg_code", help="TG-Code to search for.")
    parser.add_argument("--marke", help="Marke (brand name) to search for (e.g., HYUNDAI). Case-insensitive, fuzzy.")
    parser.add_argument("--typ", help="Typ (model name) to search for. Case-insensitive, fuzzy.")
    parser.add_argument("--marke_id", type=int, help="Exact Marke ID to search for.")
    parser.add_argument("--typ_id", type=int, help="Exact Typ ID to search for.")
    # Add more arguments as needed for other search fields
    parser.add_argument("--loglevel", type=str, choices=['info', 'warning', 'error', 'none'], default='info', help="Set log level.")

    args = parser.parse_args()
    log_levels = {'info': LOG_LEVEL_INFO, 'warning': LOG_LEVEL_WARNING, 'error': LOG_LEVEL_ERROR, 'none': LOG_LEVEL_NONE}
    set_log_level(log_levels[args.loglevel])

    if any([args.tg_code, args.marke, args.typ, args.marke_id, args.typ_id]): # If any specific search criteria given
        results = search_car_data(DATABASE_PATH, 
                                  tg_code=args.tg_code, 
                                  marke_str=args.marke, 
                                  typ_str=args.typ,
                                  col_04_marke_id=args.marke_id,
                                  col_04_typ_id=args.typ_id)
        
        if results and isinstance(results, list) and len(results) > 0 and "clarification_needed_for" in results[0]:
            clarification_info = results[0]
            field_name = clarification_info["clarification_needed_for"]
            log_message(LOG_LEVEL_INFO, f"Multiple matches found for {field_name} string '{args.marke if field_name == 'marke' else args.typ}'.")
            log_message(LOG_LEVEL_INFO, f"Please refine your search using one of the following IDs with --{field_name}_id:")
            for match in clarification_info["matches"]:
                log_message(LOG_LEVEL_INFO, f"  ID: {match['id']}, Value: {match['value']}")
        elif results:
            log_message(LOG_LEVEL_INFO, f"Search returned {len(results)} result(s).")
            log_message(LOG_LEVEL_INFO, "Details of the first result (non-None values):")
            first_result = results[0]
            for key, value in first_result.items():
                if value is not None:
                    log_message(LOG_LEVEL_INFO, f"  {key}: {value}")
            if len(results) > 1:
                log_message(LOG_LEVEL_INFO, f"(Plus {len(results) - 1} more result(s) not shown in detail here.)")
        else:
            log_message(LOG_LEVEL_INFO, "No results found for your criteria.")
    else:
        run_example_searches()