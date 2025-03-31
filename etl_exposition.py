import psycopg2
from supabase import create_client, Client
from datetime import date, datetime
import math

# Configuration de la base de données source
SOURCE_DB_CONFIG = {
    "host": "localhost",
    "dbname": "silver_dw_health_db",
    "user": "postgres",
    "password": "salma2001",
    "port": "5432"
}

# Configuration de Supabase
SUPABASE_URL = "http://127.0.0.1:54321"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZS1kZW1vIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImV4cCI6MTk4MzgxMjk5Nn0.EGIM96RAZx35lJzdJsyH-qQwv8Hdp7fsn3W0YpN81IU"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Mapping des tables source -> destination
TABLE_MAPPING = {
    "silver_patient_demographics": "patient_demographics",
    "silver_lab_result": "lab_result",
    "silver_lab_medication": "medication",
    "silver_visits": "visits",
    "silver_physicians_assignment": "physicians_assignment"
}

def convert_dates(data):
    # Convertit les objets de type date ou datetime en chaînes formatées
    for key, value in data.items():
        if isinstance(value, (date, datetime)):
            data[key] = value.strftime('%Y-%m-%d')  # ou un autre format si nécessaire
    return data

def clean_data(data):
    # Remplace les valeurs NaN et infinies par des valeurs valides (None ou autre)
    for key, value in data.items():
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                data[key] = None  # Remplace NaN ou infini par None
    return data

def migrate_table(source_table, dest_table, cursor):
    cursor.execute(f"SELECT * FROM {source_table}")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    
    for row in rows:
        data = dict(zip(columns, row))
        data = convert_dates(data)  # Convertit les dates en chaînes
        data = clean_data(data)  # Nettoie les données pour les NaN et infinis
        response = supabase.table(dest_table).insert(data).execute()
        print(f"Migrated {source_table} -> {dest_table}:", response)

def migrate_data():
    try:
        conn = psycopg2.connect(**SOURCE_DB_CONFIG)
        cursor = conn.cursor()
        
        for source_table, dest_table in TABLE_MAPPING.items():
            print(f"Migrating {source_table} to {dest_table}...")
            migrate_table(source_table, dest_table, cursor)
        
        cursor.close()
        conn.close()
    except Exception as e:
        print("Error during migration:", e)

if __name__ == "__main__":
    migrate_data()
