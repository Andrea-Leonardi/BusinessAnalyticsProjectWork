import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config as cfg


TABLES_TO_EXPORT = ("indicatori", "industrie", "mercato")

DEFAULT_DB_SETTINGS = {
    "host": "localhost",
    "port": "5432",
    "name": "project_business_analytics",
    "user": "postgres",
    "password": "Gorilla2026!",
    "sslmode": None,
}


def carica_env_da_file(env_path):
    valori = {}

    if not env_path.exists():
        return valori

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        chiave, valore = line.split("=", 1)
        valori[chiave.strip()] = valore.strip().strip('"').strip("'")

    return valori


def leggi_configurazione_database():
    env_file_values = carica_env_da_file(cfg.PROJECT_ROOT / ".env")

    host = os.getenv("POSTGRES_HOST") or os.getenv("DB_HOST") or env_file_values.get("POSTGRES_HOST") or env_file_values.get("DB_HOST") or DEFAULT_DB_SETTINGS["host"]
    port = os.getenv("POSTGRES_PORT") or os.getenv("DB_PORT") or env_file_values.get("POSTGRES_PORT") or env_file_values.get("DB_PORT") or DEFAULT_DB_SETTINGS["port"]
    db_name = os.getenv("POSTGRES_DB") or os.getenv("DB_NAME") or env_file_values.get("POSTGRES_DB") or env_file_values.get("DB_NAME") or DEFAULT_DB_SETTINGS["name"]
    user = os.getenv("POSTGRES_USER") or os.getenv("DB_USER") or env_file_values.get("POSTGRES_USER") or env_file_values.get("DB_USER") or DEFAULT_DB_SETTINGS["user"]
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("DB_PASSWORD") or env_file_values.get("POSTGRES_PASSWORD") or env_file_values.get("DB_PASSWORD") or DEFAULT_DB_SETTINGS["password"]
    sslmode = os.getenv("POSTGRES_SSLMODE") or os.getenv("DB_SSLMODE") or env_file_values.get("POSTGRES_SSLMODE") or env_file_values.get("DB_SSLMODE") or DEFAULT_DB_SETTINGS["sslmode"]

    return {
        "host": host,
        "port": port,
        "name": db_name,
        "user": user,
        "password": password,
        "sslmode": sslmode,
    }


def costruisci_database_uri(db_settings):
    user = quote_plus(db_settings["user"])
    password = quote_plus(db_settings["password"])
    host = db_settings["host"]
    port = db_settings["port"]
    db_name = db_settings["name"]
    database_uri = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

    if db_settings["sslmode"]:
        database_uri = f"{database_uri}?sslmode={db_settings['sslmode']}"

    return database_uri


def mappa_tipo_valore(sql_type):
    tipo = str(sql_type).lower()

    if "int" in tipo:
        return "intero"
    if "date" in tipo and "time" not in tipo:
        return "data"
    if "timestamp" in tipo or "datetime" in tipo or "time" in tipo:
        return "data_ora"
    if any(parola in tipo for parola in ("float", "double", "real", "numeric", "decimal")):
        return "decimale"
    if "bool" in tipo:
        return "booleano"
    if any(parola in tipo for parola in ("char", "text", "string", "varchar")):
        return "testo"
    return tipo


def estrai_catalogo_variabili(engine, table_names):
    inspector = inspect(engine)
    righe = []

    for table_name in table_names:
        colonne = inspector.get_columns(table_name)
        for colonna in colonne:
            righe.append(
                {
                    "nome_variabile": colonna["name"],
                    "tabella_provenienza": table_name,
                    "tipo_valore": mappa_tipo_valore(colonna["type"]),
                }
            )

    return pd.DataFrame(righe)


def main():
    db_settings = leggi_configurazione_database()
    database_uri = costruisci_database_uri(db_settings)
    engine = create_engine(database_uri)
    catalogo_variabili = estrai_catalogo_variabili(engine, TABLES_TO_EXPORT)
    catalogo_variabili.to_csv(cfg.RELATIONAL_DATABASE_VARIABLES_CATALOG, index=False)
    print(f"Catalogo variabili salvato in: {cfg.RELATIONAL_DATABASE_VARIABLES_CATALOG}")


if __name__ == "__main__":
    main()
