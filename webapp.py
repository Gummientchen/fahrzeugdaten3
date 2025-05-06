from flask import Flask, render_template, request

from config import DATABASE_PATH, STANDARDIZED_TG_CODE_COL # Added STANDARDIZED_TG_CODE_COL for clarity
from search import search_car_data 
from logger import set_log_level, LOG_LEVEL_INFO 

app = Flask(__name__)

# Optional: Set a log level for webapp operations if different from default
# set_log_level(LOG_LEVEL_INFO) 

# --- Display Labels for Known Columns ---
DISPLAY_LABELS = {
    "cars_tg_code": "TG-Code (Typengenehmigungsnummer)",
    "cars_col_01_fahrzeugart_value": "Fahrzeugart",
    "cars_col_02_fahrzeugsubart_code_value": "Fahrzeugsubart Code",
    "cars_col_02_fahrzeugsubart_value": "Fahrzeugsubart",
    "cars_col_03_fahrzeugklasse_value": "Fahrzeugklasse",
    "cars_col_04_marke_value": "Marke",
    "cars_col_04_typ_value": "Typ",
    "cars_col_06_vorziffer_value": "Vorziffer",
    "cars_col_07_karosserieform_code_value": "Karosserieform Code",
    "cars_col_07_karosserieform_value": "Karosserieform",
    "cars_col_09_eu_gesamtgenehmigung_value": "EU Gesamtgenehmigung",
    "cars_col_10_hersteller_value": "Hersteller (Code 10)",
    "cars_col_10_efko_code_value": "EFKO Code",
    "cars_col_11_herstellerplakette_value": "Herstellerplakette",
    "cars_col_12_fahrgestellnummer_struktur_value": "Fahrgestellnummer Struktur", # Corrected from _value
    "cars_col_12_fahrgestellnummer_value": "Fahrgestellnummer", # Assuming this might exist if not normalized
    "cars_col_14_achsen_raeder_value": "Achsen/Räder",
    "cars_col_15_federung_value": "Federung",
    "cars_col_16_lenkung_value": "Lenkung",
    "cars_col_17_achsantrieb_value": "Achsantrieb",
    "cars_col_18_getriebe_1_value": "Getriebe 1 Art",
    "cars_col_18_zuordnung_1_value": "Getriebe 1 Zuordnung",
    "cars_col_18_getriebe_2_value": "Getriebe 2 Art",
    "cars_col_18_zuordnung_2_value": "Getriebe 2 Zuordnung",
    "cars_col_18_getriebe_3_value": "Getriebe 3 Art",
    "cars_col_18_zuordnung_3_value": "Getriebe 3 Zuordnung",
    "cars_col_18_getriebe_4_value": "Getriebe 4 Art",
    "cars_col_18_zuordnung_4_value": "Getriebe 4 Zuordnung",
    "cars_col_20_betriebsbremse_z1_value": "Betriebsbremse Z1",
    "cars_col_20_betriebsbremse_z2_value": "Betriebsbremse Z2",
    "cars_col_20_betriebsbremse_z3_value": "Betriebsbremse Z3",
    "cars_col_21_hilfsbremse_z1_value": "Hilfsbremse Z1",
    "cars_col_21_hilfsbremse_z2_value": "Hilfsbremse Z2",
    "cars_col_22_feststellbremse_value": "Feststellbremse",
    "cars_col_25_motor_marke_value": "Motor Marke",
    "cars_col_25_motor_typ_value": "Motor Typ",
    "cars_col_26_bauart_value": "Motor Bauart",
    "cars_col_26_bauart_treibstoff_value": "Treibstoff (Motor)",
    "cars_col_30_abgasreinigung_value": "Abgasreinigung",
    "cars_col_31_schalldaempfer_1_art_value": "Schalldämpfer 1 Art",
    "cars_col_31_schalldaempfer_1_bez_value": "Schalldämpfer 1 Bez.",
    "cars_col_32_schalldaempfer_2_art_value": "Schalldämpfer 2 Art",
    "cars_col_32_schalldaempfer_2_bez_value": "Schalldämpfer 2 Bez.",
    "cars_col_33_schalldaempfer_3_art_value": "Schalldämpfer 3 Art",
    "cars_col_33_schalldaempfer_3_bez_value": "Schalldämpfer 3 Bez.",
    "cars_col_34_motorkennzeichen_art_value": "Motorkennzeichen Art", # Corrected from _value
    "cars_col_34_motorkennzeichen_value": "Motorkennzeichen", # If not normalized
    "cars_col_34_motorkennzeichen_anbringungsort_value": "Motorkennzeichen Anbringungsort",
    "cars_col_35_geraeuschdaempfung_z1_value": "Geräuschdämpfung Z1",
    "cars_col_35_geraeuschdaempfung_z2_value": "Geräuschdämpfung Z2",
    "cars_col_35_geraeuschdaempfung_z3_value": "Geräuschdämpfung Z3",
    "cars_col_35_geraeuschdaempfung_z4_value": "Geräuschdämpfung Z4",
    "cars_col_37_anzahl_plaetze_vorne": "Anzahl Plaetze Vorne",
    "cars_col_37_anzahl_plaetze_total": "Anzahl Plaetze Total",
    "cars_col_38_anzahl_tueren_value": "Anzahl Türen",
    "cars_col_39_rueckspiegel_value": "Rückspiegel Art",
    "cars_col_55_keine_dachlast_value": "Keine Dachlast", # This one is kept
    "cars_col_69_reifen_felgen_kombination_value": "Reifen/Felgen Kombi (69)", # Corrected from _value
    "cars_col_69_reifen_felgen_value": "Reifen/Felgen (69)", # If not normalized
    "cars_col_70_reifen_felgen_kombination_value": "Reifen/Felgen Kombi (70)", # Corrected from _value
    "cars_col_70_reifen_felgen_value": "Reifen/Felgen (70)", # If not normalized
    "cars_col_71_reifen_felgen_kombination_value": "Reifen/Felgen Kombi (71)", # Corrected from _value
    "cars_col_71_reifen_felgen_value": "Reifen/Felgen (71)", # If not normalized
    "cars_bemerkungen_z1_value": "Bemerkung Z1", "cars_bemerkungen_z2_value": "Bemerkung Z2", "cars_bemerkungen_z3_value": "Bemerkung Z3", "cars_bemerkungen_z4_value": "Bemerkung Z4", "cars_bemerkungen_z5_value": "Bemerkung Z5", "cars_bemerkungen_z6_value": "Bemerkung Z6", "cars_bemerkungen_z7_value": "Bemerkung Z7", "cars_bemerkungen_z8_value": "Bemerkung Z8", "cars_bemerkungen_z9_value": "Bemerkung Z9", "cars_bemerkungen_z10_value": "Bemerkung Z10", "cars_bemerkungen_z11_value": "Bemerkung Z11", "cars_bemerkungen_z12_value": "Bemerkung Z12", "cars_bemerkungen_z13_value": "Bemerkung Z13", "cars_bemerkungen_z14_value": "Bemerkung Z14", "cars_bemerkungen_z15_value": "Bemerkung Z15", "cars_bemerkungen_z16_value": "Bemerkung Z16", "cars_bemerkungen_z17_value": "Bemerkung Z17", "cars_bemerkungen_z18_value": "Bemerkung Z18", "cars_bemerkungen_z19_value": "Bemerkung Z19", "cars_bemerkungen_z20_value": "Bemerkung Z20", "cars_bemerkungen_z21_value": "Bemerkung Z21", "cars_bemerkungen_z22_value": "Bemerkung Z22", "cars_bemerkungen_z23_value": "Bemerkung Z23", "cars_bemerkungen_z24_value": "Bemerkung Z24",

    # Von/Bis pairs - base names for combined fields
    "cars_col_40_laenge": "Länge", "cars_col_41_breite": "Breite", "cars_col_42_hoehe": "Höhe",
    "cars_col_44_abstand_achse_1_2": "Abstand Achse 1-2", "cars_col_45_abstand_achse_2_3": "Abstand Achse 2-3", "cars_col_46_abstand_achse_3_4": "Abstand Achse 3-4",
    "cars_col_47_spur_achse_1": "Spur Achse 1", "cars_col_48_spur_achse_2": "Spur Achse 2", "cars_col_49_spur_achse_3": "Spur Achse 3", "cars_col_50_spur_achse_4": "Spur Achse 4",
    "cars_col_52_leergewicht": "Leergewicht", "cars_col_53_garantiegewicht": "Garantiegewicht",
    "cars_col_43_ueberhang_hinten": "Überhang Hinten", # New
    "cars_col_54_achsgarantie_v": "Achsgarantie Vorne", # New
    "cars_col_54_achsgarantie_h": "Achsgarantie Hinten", # New
    
    # Emissions table fields
    "emissions_abgascode_value": "Abgascode", "emissions_emissionscode_value": "Emissionscode",
    "emissions_geraeuschcode_value": "Geräuschcode", "emissions_bemerkung_value": "Bemerkung (Emissionen)",
    "emissions_vmax": "Vmax", # Combined from emissions_vmax_von/bis
    "emissions_hubraum": "Hubraum", "emissions_leistung": "Leistung", "emissions_leistung_bei_n_min": "Leistung bei n/min",
    "emissions_drehmoment": "Drehmoment", "emissions_drehmoment_bei_n_min": "Drehmoment bei n/min",
    "emissions_fahrgeraeusch": "Fahrgeräusch", "emissions_standgeraeusch": "Standgeräusch",
    "emissions_standgeraeusch_bei_n_min": "Standgeräusch bei n/min",
    "emissions_tc_consumption": "Verbrauch (TC)",
    "emissions_iachse": "I-Achse", # Corrected key: removed _value
    # Consumption table fields
    "consumption_treibstoff_value": "Treibstoff", "consumption_getriebe_value": "Getriebe (Verbrauch)",
    "consumption_hinweis_value": "Hinweis (Verbrauch)",
    "consumption_el_reichweite_wltp": "Elektrische Reichweite WLTP", # Combined from _von/_bis
    "consumption_el_verbrauch_wltp": "Elektrischer Verbrauch WLTP",
    "cars_col_55_dachlast": "Dachlast", # New (assuming this is the actual load value)
    "consumption_energieeffizienzkategorie_value": "Energieeffizienzkategorie",
}

# --- Data Grouping for Display ---
DATA_GROUPS_ORDER = [
    "Fahrzeug", "Sitze & Türen", "Motor", "Getriebe", "Bremsen", "Masse & Abmessungen", "Achsen & Spur", 
    "Gewichte", "Räder & Reifen", "Leistungsdaten", "Verbrauch & Emissionen", 
    "Bemerkungen", "Sonstige Daten" # Added Sonstige Daten
]

DATA_GROUPS_MAPPING = {
    # Fahrzeug
    "TG-Code (Typengenehmigungsnummer)": "Fahrzeug", "Fahrzeugart": "Fahrzeug", "Fahrzeugklasse": "Fahrzeug",
    "Marke": "Fahrzeug", "Typ": "Fahrzeug", "Karosserieform": "Fahrzeug", "EU Gesamtgenehmigung": "Fahrzeug",
    "cars_col_05_typ_variante_version": "Fahrzeug", "Hersteller (Code 10)": "Fahrzeug", "Herstellerplakette": "Fahrzeug", # Assuming cars_col_05_typ_variante_version is a direct key or its _value variant
    "Fahrgestellnummer Struktur": "Fahrzeug", "Fahrgestellnummer": "Fahrzeug",
    "Rückspiegel Art": "Fahrzeug", "Vorziffer": "Fahrzeug", "EFKO Code": "Fahrzeug",

    # Sitze & Türen
    "Anzahl Türen": "Sitze & Türen", "Anzahl Plaetze Vorne": "Sitze & Türen", "Anzahl Plaetze Total": "Sitze & Türen",

    # Motor
    "Motor Marke": "Motor", "Motor Typ": "Motor", "Motor Bauart": "Motor", "Treibstoff (Motor)": "Motor",
    "Motorkennzeichen Art": "Motor", "Motorkennzeichen": "Motor", "Motorkennzeichen Anbringungsort": "Motor",
    "Hubraum": "Motor", "Leistung bei n/min": "Motor", # "Leistung" moved to Leistungsdaten
    "Drehmoment": "Motor", "Drehmoment bei n/min": "Motor",

    # Getriebe
    "Getriebe 1 Art": "Getriebe", "Getriebe 1 Zuordnung": "Getriebe",
    "Getriebe 2 Art": "Getriebe", "Getriebe 2 Zuordnung": "Getriebe",
    "Getriebe 3 Art": "Getriebe", "Getriebe 3 Zuordnung": "Getriebe",
    "Getriebe 4 Art": "Getriebe", "Getriebe 4 Zuordnung": "Getriebe", "I-Achse (Emissionen)": "Getriebe",
    "Achsantrieb": "Getriebe", # Could also be Achsen

    # Bremsen
    "Betriebsbremse Z1": "Bremsen", "Betriebsbremse Z2": "Bremsen", "Betriebsbremse Z3": "Bremsen",
    "Hilfsbremse Z1": "Bremsen", "Hilfsbremse Z2": "Bremsen", "Feststellbremse": "Bremsen",

    # Masse & Abmessungen
    "Länge": "Masse & Abmessungen", "Breite": "Masse & Abmessungen", "Höhe": "Masse & Abmessungen",
    "Überhang Hinten": "Masse & Abmessungen",
    # Achsen & Spur
    "Achsen/Räder": "Achsen & Spur", "Federung": "Achsen & Spur", "Lenkung": "Achsen & Spur",
    "Abstand Achse 1-2": "Achsen & Spur", "Abstand Achse 2-3": "Achsen & Spur", "Abstand Achse 3-4": "Achsen & Spur",
    "Spur Achse 1": "Achsen & Spur", "Spur Achse 2": "Achsen & Spur", "Spur Achse 3": "Achsen & Spur", "Spur Achse 4": "Achsen & Spur",

    # Gewichte
    "Leergewicht": "Gewichte", "Garantiegewicht": "Gewichte", "Keine Dachlast": "Gewichte",
    "Achsgarantie Vorne": "Gewichte", "Achsgarantie Hinten": "Gewichte", "Dachlast": "Gewichte",

    # Räder & Reifen
    "Reifen/Felgen Kombi (69)": "Räder & Reifen", "Reifen/Felgen (69)": "Räder & Reifen",
    "Reifen/Felgen Kombi (70)": "Räder & Reifen", "Reifen/Felgen (70)": "Räder & Reifen",
    "Reifen/Felgen Kombi (71)": "Räder & Reifen", "Reifen/Felgen (71)": "Räder & Reifen",

    # Leistungsdaten (Vmax from emissions is already here, adding the new one)
    "Vmax": "Leistungsdaten", "Leistung": "Leistungsdaten",

    # Verbrauch & Emissionen
    "Abgasreinigung": "Verbrauch & Emissionen", "Schalldämpfer 1 Art": "Verbrauch & Emissionen", "Schalldämpfer 1 Bez.": "Verbrauch & Emissionen",
    "Schalldämpfer 2 Art": "Verbrauch & Emissionen", "Schalldämpfer 2 Bez.": "Verbrauch & Emissionen",
    "Schalldämpfer 3 Art": "Verbrauch & Emissionen", "Schalldämpfer 3 Bez.": "Verbrauch & Emissionen",
    "Geräuschdämpfung Z1": "Verbrauch & Emissionen", "Geräuschdämpfung Z2": "Verbrauch & Emissionen",
    "Geräuschdämpfung Z3": "Verbrauch & Emissionen", "Geräuschdämpfung Z4": "Verbrauch & Emissionen",
    "Abgascode": "Verbrauch & Emissionen", "Emissionscode": "Verbrauch & Emissionen", "Geräuschcode": "Verbrauch & Emissionen",
    "Fahrgeräusch": "Verbrauch & Emissionen", "Standgeräusch": "Verbrauch & Emissionen", "Standgeräusch bei n/min": "Verbrauch & Emissionen",
    "Verbrauch (TC)": "Verbrauch & Emissionen", "Elektrischer Verbrauch WLTP": "Verbrauch & Emissionen",
    "Elektrische Reichweite WLTP": "Verbrauch & Emissionen", "Energieeffizienzkategorie": "Verbrauch & Emissionen",
    "Treibstoff (Verbrauch)": "Verbrauch & Emissionen", "Getriebe (Verbrauch)": "Verbrauch & Emissionen", "Hinweis (Verbrauch)": "Verbrauch & Emissionen",
    "Consumption El Reichweite Von Wltp": "Verbrauch & Emissionen", # Added per user request

    # Bemerkungen (will be caught by fallback if labels match "Bemerkung Zx")
}

# --- Labels to Exclude from Output ---
LABELS_TO_EXCLUDE = {
    "Keine Anhängelast",
    "Leistung (Fahrzeug)",
    "Fahrzeug Vmax Mech",
    "Emissions Leergewicht",
    "Emissions Garantiegewicht",
    "TG-Code (Verbrauch)",
    "TG-Code (Emissionen)",
    "Consumption Laenge",
    "Consumption Breite",
    "Consumption Hoehe",
    "Consumption Iachse",
    "Marke (Emissionen)",
    "Typ (Emissionen)",
    "Getriebe (Emissionen)",
    "Motor Marke (Emissionen)",
    "Motor Typ (Emissionen)",
    "Bauart (Emissionen)",
    "Treibstoff (Emissionen)",
    "Achsen/Räder (Emissionen)",
    "Antrieb (Emissionen)",
    "Marke (Verbrauch)",
    "Typ (Verbrauch)",
}

# --- Field Units ---
FIELD_UNITS = {
    "Länge": "mm",
    "Breite": "mm",
    "Höhe": "mm",
    "Abstand Achse 1-2": "mm",
    "Abstand Achse 2-3": "mm",
    "Abstand Achse 3-4": "mm",
    "Spur Achse 1": "mm",
    "Spur Achse 2": "mm",
    "Spur Achse 3": "mm",
    "Spur Achse 4": "mm",
    "Leergewicht": "kg",
    "Garantiegewicht": "kg",
    "Vmax": "km/h",
    "Hubraum": "cm³",
    "Leistung": "kW",
    "Leistung bei n/min": "rpm", # Unit for the n/min part
    "Drehmoment": "Nm",
    "Drehmoment bei n/min": "rpm", # Unit for the n/min part
    "Fahrgeräusch": "dB(A)",
    "Standgeräusch": "dB(A)",
    "Standgeräusch bei n/min": "rpm", # Unit for the n/min part
    "Überhang Hinten": "mm", # New
    "Achsgarantie Vorne": "kg", # New
    "Achsgarantie Hinten": "kg", # New
    "Verbrauch (TC)": "l/100km", # Assuming liquid fuel consumption
    "Dachlast": "kg", # New
    "Elektrische Reichweite WLTP": "km", # This seems correct
    "Consumption El Reichweite Von Wltp": "km", # Added per user request
    "Elektrischer Verbrauch WLTP": "Wh/km", # Changed to Wh/km as primary
}

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

    display_results = []
    if results_from_search:
        for row_dict in results_from_search:
            grouped_data = {group_name: {} for group_name in DATA_GROUPS_ORDER}
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
                    # Check if it's a TG-Code field (e.g., "cars_tg_code")
                    is_a_tg_code_field = STANDARDIZED_TG_CODE_COL in db_key 
                    
                    # Skip _id fields unless they are TG-Codes or explicitly in DISPLAY_LABELS
                    if db_key.endswith("_id") and not is_a_tg_code_field and db_key not in DISPLAY_LABELS:
                        continue 

                    is_val_valid, processed_val = _is_value_valid_for_display(value)
                    if is_val_valid:
                        value_to_display_final = str(processed_val)
                
                if value_to_display_final is not None:
                    final_display_label = DISPLAY_LABELS.get(key_for_display_lookup, 
                                                             key_for_display_lookup.replace("_", " ").title())
                    
                    if final_display_label in LABELS_TO_EXCLUDE:
                        continue

                    value_for_group = value_to_display_final # Initialize with the base value string

                    if final_display_label == "Elektrischer Verbrauch WLTP":
                        parts_original_str = value_to_display_final.split(' - ')
                        
                        formatted_wh_km_parts = []
                        formatted_kwh_100km_parts = []
                        formatted_km_kwh_parts = [] # Changed for km/kWh
                        conversion_possible_for_all = True

                        for part_str in parts_original_str:
                            try:
                                val_numeric_wh_km = float(part_str)
                                
                                # Format Wh/km part
                                if val_numeric_wh_km == int(val_numeric_wh_km):
                                    formatted_wh_km_parts.append(str(int(val_numeric_wh_km)))
                                else:
                                    formatted_wh_km_parts.append(f"{val_numeric_wh_km:.1f}")

                                # Calculate and format kWh/100km part
                                val_kwh_100km = val_numeric_wh_km / 10.0
                                if val_kwh_100km == int(val_kwh_100km):
                                    formatted_kwh_100km_parts.append(str(int(val_kwh_100km)))
                                else:
                                    formatted_kwh_100km_parts.append(f"{val_kwh_100km:.1f}")

                                # Calculate and format km/kWh part
                                # _is_value_valid_for_display should prevent val_numeric_wh_km from being 0 here
                                val_km_kwh = 1000.0 / val_numeric_wh_km if val_numeric_wh_km != 0 else 0 # Avoid division by zero
                                formatted_km_kwh_parts.append(f"{val_km_kwh:.1f}")

                            except ValueError:
                                conversion_possible_for_all = False
                                break 
                        
                        if conversion_possible_for_all:
                            display_wh_km_str = " - ".join(formatted_wh_km_parts)
                            display_kwh_100km_str = " - ".join(formatted_kwh_100km_parts)
                            display_km_kwh_str = " - ".join(formatted_km_kwh_parts) # Changed variable name
                            value_for_group = f"{display_wh_km_str} Wh/km | {display_kwh_100km_str} kWh/100km | {display_km_kwh_str} km/kWh" # Changed unit label
                        else: # Fallback if conversion failed (e.g., value was "N/A")
                            primary_unit = FIELD_UNITS.get(final_display_label) # Should be "Wh/km"
                            if primary_unit:
                                value_for_group = f"{value_to_display_final} {primary_unit}"
                    else: # Generic unit addition for other fields
                        unit = FIELD_UNITS.get(final_display_label)
                        if unit:
                            value_for_group = f"{value_to_display_final} {unit}"

                    group_name = DATA_GROUPS_MAPPING.get(final_display_label) # Grouping based on label
                    if not group_name: # Fallback if the final_display_label is not in DATA_GROUPS_MAPPING
                        key_lower = key_for_display_lookup.lower()
                        if "bemerkung" in key_lower: # Catches "bemerkungen_zX_value" and "emissions_bemerkung_value"
                            group_name = "Bemerkungen"
                        else:
                            group_name = "Sonstige Daten"
                    
                    if group_name not in grouped_data: # Should be pre-filled by DATA_GROUPS_ORDER
                        # This ensures that if a new group name is derived, it can still be added,
                        # though it might not respect DATA_GROUPS_ORDER if the group isn't in it.
                        # It's best if DATA_GROUPS_ORDER contains all possible group names.
                        grouped_data[group_name] = {} # Ensure group dict exists
                    grouped_data[group_name][final_display_label] = value_for_group
            
            # Custom reordering for "Fahrzeug" group removed as items moved to "Sitze & Türen"
            ordered_single_result_groups = {
                group: grouped_data[group]
                for group in DATA_GROUPS_ORDER 
                if group in grouped_data and grouped_data[group]
            }
            if ordered_single_result_groups:
                display_results.append(ordered_single_result_groups)

    return render_template('search_results.html', 
                           results=display_results, 
                           search_performed=search_performed,
                           error_message=error_message,
                           data_groups_order=DATA_GROUPS_ORDER) # Pass order to template

if __name__ == '__main__':
    app.run(debug=True)
