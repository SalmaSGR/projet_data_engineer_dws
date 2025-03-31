import psycopg2
import pandas as pd
import logging

#----------partie de postgresql
def connect_postgres():
    return psycopg2.connect(
        host="localhost",
        dbname="silver_dw_health_db",
        user="postgres",
        password="salma2001",
        port="5432"
    )

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    
    # Convertir en DataFrame pandas pour manipulation facile
    df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
    
    return df

query_postgresql = "SELECT * FROM silver_visits WHERE patient_id = 'P003';"

# PostgreSQL
conn_postgres = connect_postgres()
df_postgres = execute_query(conn_postgres, query_postgresql)
print("Résultats PostgreSQL:\n", df_postgres)

#------------partie de supabase
def connect_supabase():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="127.0.0.1",
        port="54322"
    )

query_supabase = "SELECT * FROM visits WHERE patient_id = 'P003';"
# Supabase
conn_supabase = connect_supabase()
df_supabase = execute_query(conn_supabase, query_supabase)
print("Résultats Supabase:\n", df_supabase)


#-----------Gérer les erreurs et logs
# Configuration du log : Un seul fichier "query_log.log"
logging.basicConfig(
    filename="query_log.log",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def safe_execute_query(connection, query, db_name):
    """
    Exécute une requête SQL en toute sécurité.
    - `db_name` permet d'identifier la base de données (PostgreSQL ou Supabase).
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Convertir en DataFrame pandas
        return pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

    except Exception as e:
        # Enregistrer l'erreur avec le nom de la base concernée
        logging.error(f"Erreur sur {db_name} : {e}")
        return None
    

# 🔹 PostgreSQL
conn_postgres = connect_postgres()
df_postgres = safe_execute_query(conn_postgres, query_postgresql, "PostgreSQL")

# 🔹 Supabase
conn_supabase = connect_supabase()
df_supabase = safe_execute_query(conn_supabase, query_supabase, "Supabase")