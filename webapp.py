from flask import Flask, render_template, request

from config import DATABASE_PATH
from search import search_car_data # Assuming your search function is in search.py
from logger import set_log_level, LOG_LEVEL_INFO # Optional: for webapp specific logging

app = Flask(__name__)

# Optional: Set a log level for webapp operations if different from default
# set_log_level(LOG_LEVEL_INFO) 

# --- Display Labels for Known Columns ---
# This is a manual mapping. For a fully dynamic solution,
# you'd need to store original headers during import.
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
    "cars_col_10_hersteller_value": "Hersteller (Code 10)", # Example, adjust if this is from efko_code
    "cars_col_10_efko_code_value": "EFKO Code",
    "cars_col_11_herstellerplakette_value": "Herstellerplakette",
    "cars_col_12_fahrgestellnummer_struktur_value": "Fahrgestellnummer Struktur",
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
    "cars_col_34_motorkennzeichen_art_value": "Motorkennzeichen Art",
    "cars_col_34_motorkennzeichen_anbringungsort_value": "Motorkennzeichen Anbringungsort",
    "cars_col_35_geraeuschdaempfung_z1_value": "Geräuschdämpfung Z1",
    "cars_col_35_geraeuschdaempfung_z2_value": "Geräuschdämpfung Z2",
    "cars_col_35_geraeuschdaempfung_z3_value": "Geräuschdämpfung Z3",
    "cars_col_35_geraeuschdaempfung_z4_value": "Geräuschdämpfung Z4",
    "cars_col_38_anzahl_tueren_value": "Anzahl Türen",
    "cars_col_39_rueckspiegel_value": "Rückspiegel Art",
    "cars_col_55_keine_dachlast_value": "Keine Dachlast",
    "cars_col_56_keine_anhaengelast_value": "Keine Anhängelast",
    "cars_col_69_reifen_felgen_kombination_value": "Reifen/Felgen Kombi (69)",
    "cars_col_70_reifen_felgen_kombination_value": "Reifen/Felgen Kombi (70)",
    "cars_col_71_reifen_felgen_kombination_value": "Reifen/Felgen Kombi (71)",
    "cars_bemerkungen_z1_value": "Bemerkung Z1",
    "cars_bemerkungen_z2_value": "Bemerkung Z2",
    # ... (continue for all bemerkungen_zX_value from cars)
    "cars_bemerkungen_z24_value": "Bemerkung Z24",

    "emissions_tg_code": "TG-Code (Emissionen)", # If you want to show this explicitly
    "emissions_marke_value": "Marke (Emissionen)",
    "emissions_typ_value": "Typ (Emissionen)",
    "emissions_getriebe_value": "Getriebe (Emissionen)",
    "emissions_motormarke_value": "Motor Marke (Emissionen)",
    "emissions_motortyp_value": "Motor Typ (Emissionen)",
    "emissions_bauart_value": "Bauart (Emissionen)",
    "emissions_treibstoff_value": "Treibstoff (Emissionen)",
    "emissions_abgasreinigung_value": "Abgasreinigung (Emissionen)",
    "emissions_anzahl_achsen_raeder_value": "Achsen/Räder (Emissionen)",
    "emissions_antrieb_value": "Antrieb (Emissionen)",
    "emissions_abgascode_value": "Abgascode",
    "emissions_emissionscode_value": "Emissionscode",
    "emissions_geraeuschcode_value": "Geräuschcode",
    "emissions_bemerkung_value": "Bemerkung (Emissionen)",

    "consumption_tg_code": "TG-Code (Verbrauch)", # If you want to show this explicitly
    "consumption_marke_value": "Marke (Verbrauch)",
    "consumption_typ_value": "Typ (Verbrauch)",
    "consumption_treibstoff_value": "Treibstoff (Verbrauch)",
    "consumption_getriebe_value": "Getriebe (Verbrauch)",
    "consumption_hinweis_value": "Hinweis (Verbrauch)",
    "consumption_energieeffizienzkategorie_value": "Energieeffizienzkategorie",

    # Labels for _id fields if you decide to show them (less common for end-user display)
    # "cars_col_04_marke_id": "Marke ID (DB)",
    # "cars_col_04_typ_id": "Typ ID (DB)",
}

# --- Data Grouping for Display ---
# Maps display labels (as defined in DISPLAY_LABELS or original db_keys) to group names.
# The order of groups in this dict will influence display order if iterated directly.
DATA_GROUPS_ORDER = [
    "Fahrzeug", "Motor", "Türen/Sitzplätze", "Felgen", "Masse", "Achsen", "Spur", 
    "Gewichte", "Leistungsdaten", "Verbrauch", "Emissionen", "Bemerkungen"
]

DATA_GROUPS_MAPPING = {
    # Fahrzeug
    "TG-Code (Typengenehmigungsnummer)": "Fahrzeug",
    "Fahrzeugart": "Fahrzeug",
    "Fahrzeugklasse": "Fahrzeug",
    "Marke": "Fahrzeug",
    "Typ": "Fahrzeug",
    "Karosserieform": "Fahrzeug",
    "EU Gesamtgenehmigung": "Fahrzeug",
    "cars_col_05_typ_variante_version": "Fahrzeug", # Assuming this is a direct db_key not in DISPLAY_LABELS yet
    "Hersteller (Code 10)": "Fahrzeug",
    "Herstellerplakette": "Fahrzeug",
    "cars_col_12_fahrgestellnummer_value": "Fahrzeug", # Assuming this is a direct db_key
    "Achsen/Räder": "Fahrzeug",
    "Federung": "Fahrzeug",
    "Lenkung": "Fahrzeug",
    "Achsantrieb": "Fahrzeug",
    "Getriebe 1 Art": "Fahrzeug",
    # Motor
    "Motor Typ": "Motor",
    "Motor Bauart": "Motor",
    "Treibstoff (Motor)": "Motor",
    # Türen/Sitzplätze
    "Anzahl Türen": "Türen/Sitzplätze",
    # Felgen
    "cars_col_69_reifen_felgen_value": "Felgen", # Assuming this is a direct db_key
    # Masse
    "cars_col_40_laenge_von": "Masse", "cars_col_40_laenge_bis": "Masse",
    "cars_col_41_breite_von": "Masse", "cars_col_41_breite_bis": "Masse",
    "cars_col_42_hoehe_von": "Masse", "cars_col_42_hoehe_bis": "Masse",
    # Achsen (Sub-group of Masse in your list, handled by iterating keys)
    "cars_col_44_abstand_achse_1_2_von": "Achsen", "cars_col_44_abstand_achse_1_2_bis": "Achsen",
    "cars_col_45_abstand_achse_2_3_von": "Achsen", "cars_col_45_abstand_achse_2_3_bis": "Achsen",
    "cars_col_46_abstand_achse_3_4_von": "Achsen", "cars_col_46_abstand_achse_3_4_bis": "Achsen",
    # Spur (Sub-group of Masse in your list)
    "cars_col_47_spur_achse_1_von": "Spur", "cars_col_47_spur_achse_1_bis": "Spur",
    "cars_col_48_spur_achse_2_von": "Spur", "cars_col_48_spur_achse_2_bis": "Spur",
    "cars_col_49_spur_achse_3_von": "Spur", "cars_col_49_spur_achse_3_bis": "Spur",
    "cars_col_50_spur_achse_4_von": "Spur", "cars_col_50_spur_achse_4_bis": "Spur",
    # Gewichte
    "cars_col_52_leergewicht_von": "Gewichte", "cars_col_52_leergewicht_bis": "Gewichte",
    "cars_col_53_garantiegewicht_von": "Gewichte", "cars_col_53_garantiegewicht_bis": "Gewichte",
    "Keine Dachlast": "Gewichte", # Assuming col_55_keine_dachlast_value maps to this
    # Leistungsdaten
    "emissions_vmax_von": "Leistungsdaten", "emissions_vmax_bis": "Leistungsdaten",
    "emissions_hubraum": "Leistungsdaten", "emissions_leistung": "Leistungsdaten",
    "emissions_leistung_bei_n_min": "Leistungsdaten", "emissions_drehmoment": "Leistungsdaten",
    "emissions_drehmoment_bei_n_min": "Leistungsdaten",
    # Verbrauch
    "emissions_tc_consumption": "Verbrauch", "consumption_el_verbrauch_wltp": "Verbrauch",
    "consumption_el_reichweite_von_wltp": "Verbrauch", "consumption_el_reichweite_bis_wltp": "Verbrauch",
    # Emissionen
    "emissions_fahrgeraeusch": "Emissionen", "emissions_standgeraeusch": "Emissionen",
    "emissions_standgeraeusch_bei_n_min": "Emissionen",
    "Abgascode": "Emissionen", "Emissionscode": "Emissionen", "Geräuschcode": "Emissionen",
    "Energieeffizienzkategorie": "Emissionen",
    # Bemerkungen - This will catch all "Bemerkung Zx" if DISPLAY_LABELS is set up for them
    # Or you can list them individually if their display labels are unique
}

@app.route('/', methods=['GET', 'POST'])
def index():
    results = []
    search_performed = False
    error_message = None

    if request.method == 'POST':
        search_performed = True
        tg_code = request.form.get('tg_code')
        marke_str = request.form.get('marke')
        typ_str = request.form.get('typ')
        
        marke_id_str = request.form.get('marke_id')
        typ_id_str = request.form.get('typ_id')

        col_04_marke_id = int(marke_id_str) if marke_id_str and marke_id_str.strip() else None
        col_04_typ_id = int(typ_id_str) if typ_id_str and typ_id_str.strip() else None

        tg_code = tg_code if tg_code and tg_code.strip() else None
        marke_str = marke_str if marke_str and marke_str.strip() else None
        typ_str = typ_str if typ_str and typ_str.strip() else None

        if not any([tg_code, marke_str, typ_str, col_04_marke_id is not None, col_04_typ_id is not None]):
            error_message = "Please enter at least one search criterion."
        else:
            try:
                results = search_car_data(
                    db_path=DATABASE_PATH,
                    tg_code=tg_code,
                    marke_str=marke_str,
                    typ_str=typ_str,
                    col_04_marke_id=col_04_marke_id,
                    col_04_typ_id=col_04_typ_id
                    # Add other search params from your search_car_data function if needed
                )
            except Exception as e:
                error_message = f"An error occurred during the search: {e}"
                app.logger.error(f"Search error: {e}", exc_info=True)

    display_results = []
    if results:
        for row_dict in results:
            # Initialize grouped_data with empty dicts for each group to maintain order
            grouped_data = {group_name: {} for group_name in DATA_GROUPS_ORDER}
            
            for db_key, value in row_dict.items():
                # Skip _id columns unless they are the main tg_code or explicitly in DISPLAY_LABELS
                is_tg_code_key = any(db_key.endswith(tg_code_suffix) for tg_code_suffix in ['_tg_code', '_Typengenehmigungsnummer']) # crude check for tg_code keys
                
                if db_key.endswith("_id") and not is_tg_code_key and db_key not in DISPLAY_LABELS:
                    continue # Skip this _id column
                
                if value is not None and (not isinstance(value, str) or value.strip()):
                    display_key = DISPLAY_LABELS.get(db_key, db_key) # Get display label if available, else use db_key
                    
                    # Determine group
                    group_name = "Sonstige Daten" # Default group
                    if display_key in DATA_GROUPS_MAPPING:
                        group_name = DATA_GROUPS_MAPPING[display_key]
                    elif "Bemerkung" in display_key: # Catch-all for bemerkungen
                        group_name = "Bemerkungen"
                    
                    if group_name not in grouped_data: # Should not happen if DATA_GROUPS_ORDER is comprehensive
                        grouped_data[group_name] = {}
                    grouped_data[group_name][display_key] = value
            
            # Filter out empty groups before appending
            display_results.append({group: data for group, data in grouped_data.items() if data})


    return render_template('search_results.html', 
                           results=display_results, 
                           search_performed=search_performed,
                           error_message=error_message)

if __name__ == '__main__':
    app.run(debug=True)
