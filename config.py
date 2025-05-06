from pathlib import Path
import os

# --- Base Paths ---
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATABASE_DIR = BASE_DIR / "database"
DATABASE_PATH = os.getenv('DATABASE_PATH', os.path.join(DATA_DIR, "fahrzeugdaten.db"))

# Ensure the directory for the database exists, especially if DATABASE_PATH is absolute
db_dir = os.path.dirname(DATABASE_PATH)
if db_dir and not os.path.exists(db_dir): # Check if db_dir is not empty (for relative paths)
    os.makedirs(db_dir, exist_ok=True)
    print(f"Created database directory: {db_dir}") # For visibility

# --- TG-Code Configuration ---
TG_CODE_COLUMN_NAMES = ["TG-Code", "Typengenehmigungsnummer"]
STANDARDIZED_TG_CODE_COL = "tg_code"

# --- Normalization Configuration ---
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

# --- File Processing Configuration ---
FILES_TO_PROCESS = [
    {
        "url": "https://opendata.astra.admin.ch/ivzod/2000-Typengenehmigungen_TG_TARGA/2200-Basisdaten_TG_ab_1995/TG-Automobil.txt",
        "local_name": "TG-Automobil.txt",
        "table_name": "cars",
        "is_primary_key_table": True,
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