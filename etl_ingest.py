import pandas as pd
import psycopg2
from datetime import datetime
import os
import logging

# Configuration du logging
log_file = "etl_log.txt"  # Fichier de log
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,  # Niveau de log (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',
)

# Fonction pour se connecter à la base de données
def connect_db():
    try:
        connection = psycopg2.connect(
            host="localhost",
            dbname="raw_health_db",  # Nom de ta base de données
            user="postgres",  #  utilisateur PostgreSQL
            password="salma2001",  #  mot de passe PostgreSQL
            port="5432"  # Port par défaut
        )
        return connection
    except Exception as e:
        logging.error(f"Erreur de connexion à la base de données : {e}")
        return None

# Vérification de l'existence des fichiers avant de les charger
def check_file_exists(file_path):
    if not os.path.exists(file_path):
        logging.warning(f"ALERTE : Le fichier {file_path} est manquant.")  # Alerte dans le fichier log
        return False
    return True

# Fonction pour créer la table, supprimer si elle existe déjà, et insérer les données
def create_table_and_load_data(df, table_name):
    # Connexion à la base de données
    connection = connect_db()
    if connection is None:
        return

    cursor = connection.cursor()

    # Supprimer la table si elle existe déjà
    drop_table_query = f"DROP TABLE IF EXISTS raw_{table_name};"
    logging.info(f"Exécution de la requête pour supprimer la table si elle existe : {drop_table_query}")
    cursor.execute(drop_table_query)

    # Générer la liste des colonnes et leur type (ici, TEXT par défaut)
    columns = ', '.join([f"{col} TEXT" for col in df.columns])  # Ajout de 'TEXT' pour chaque colonne

    # Requête pour créer la table si elle n'existe pas avec le préfixe 'raw_'
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS raw_{table_name} (
        {columns}
    );
    """
    logging.info(f"Exécution de la requête pour créer la table : {create_table_query}")
    cursor.execute(create_table_query)

    # Insertion des données dans la table
    for _, row in df.iterrows():
        insert_query = f"""
        INSERT INTO raw_{table_name} ({', '.join(df.columns)}) 
        VALUES ({', '.join([f"'{str(value)}'" for value in row])});
        """
        logging.info(f"Insertion des données dans la table raw_{table_name}: {insert_query}")
        cursor.execute(insert_query)

    # Commit des changements et fermeture de la connexion
    connection.commit()
    cursor.close()
    connection.close()

    logging.info(f"Table raw_{table_name} mise à jour avec succès!")

# Fonction pour charger un fichier CSV dans un DataFrame et ajouter la colonne alim_date
def load_file(file_path):
    # Chargement du fichier CSV dans un DataFrame avec le séparateur ';'
    df = pd.read_csv(file_path, sep=';')
    
    # Ajouter la colonne alim_date
    df['alim_date'] = date_today  # Ajoute la date d'alimentation
    
    # Afficher un aperçu du DataFrame
    logging.info(f"Aperçu du fichier {file_path}:")
    logging.info(f"{df.head()}")  # Affiche les 5 premières lignes du DataFrame
    logging.info("-" * 50)  # Séparateur pour plus de clarté
    
    return df

# Date du jour
date_today = datetime.today().strftime('%Y%m%d')

# Dictionnaire des chemins des fichiers
file_paths = {
    "patient_demographics": f"C:/Users/33753/Desktop/salma/RAW/{date_today}_patient_demographics.csv",
    "patient_lab_results": f"C:/Users/33753/Desktop/salma/RAW/{date_today}_patient_lab_results.csv",
    "patient_medications": f"C:/Users/33753/Desktop/salma/RAW/{date_today}_patient_medications.csv",
    "patient_visits": f"C:/Users/33753/Desktop/salma/RAW/{date_today}_patient_visits.csv",
    "physician_assignments": f"C:/Users/33753/Desktop/salma/RAW/{date_today}_physician_assignments.csv"
}

# Liste pour stocker les fichiers manquants
missing_files = []

# Boucle pour charger chaque fichier et insérer les données dans la base de données
for file_name, file_path in file_paths.items():
    logging.info(f"Traitement du fichier : {file_name}")
    
    # Vérifier si le fichier existe avant de continuer
    if not check_file_exists(file_path):
        missing_files.append(file_name)  # Ajouter le fichier manquant à la liste
        continue  # Passer au fichier suivant

    # Charger le fichier et afficher un aperçu
    df = load_file(file_path)
    
    # Créer la table et insérer les données dans la base de données
    create_table_and_load_data(df, file_name)

logging.info("Chargement et insertion des données terminés.")

# Si des fichiers sont manquants, afficher une alerte à la fin
if missing_files:
    logging.warning(f"\nALERTE : Les fichiers suivants sont manquants :")
    for file in missing_files:
        logging.warning(f"- {file}")
else:
    logging.info("\nTous les fichiers sont présents et ont été traités avec succès.")
