
import psycopg2
import pandas as pd
import psycopg2.extras
from supabase import create_client, Client
# Paramètres de connexion
conn = psycopg2.connect(
    host="localhost",
    dbname="raw_health_db",
    user="postgres",
    password="salma2001",
    port="5432"
)

cursor = conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = cursor.fetchall()
print("Tables disponibles :", tables)

#----------------------------------extraction---------------------------------------------------

def extract_table(table_name):
    conn = psycopg2.connect(
        host="localhost",
        dbname="raw_health_db",
        user="postgres",
        password="salma2001",
        port="5432"
    )
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, conn)
    conn.close()
    print(f" Aperçu des données de {table_name} :")
    print(df.head(), "\n")
    return df

df_demographics = extract_table("raw_patient_demographics")
df_lab_results = extract_table("raw_patient_lab_results")
df_medications = extract_table("raw_patient_medications")
df_visits = extract_table("raw_patient_visits")
df_physicians = extract_table("raw_physician_assignments")

#-----------------------------------transformation-------------------------------------

# fonction de transformation patient_demographic
def transform_patient_demographics(df):
    """Transformation des données patient_demographics avec changement de types de données."""

    # Convertir alim_date en format date
    df['alim_date'] = pd.to_datetime(df['alim_date'], format='%Y%m%d', errors='coerce')

    #  Remplacer les valeurs nulles de 'gender' par 'inconnu'
    df['gender'] = df['gender'].fillna('inconnu')

    # Convertir 'age' en numérique (type entier)
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)

    #  Nettoyer 'other_fields' en supprimant espaces et mettant en minuscules
    df['other_fields'] = df['other_fields'].astype(str).str.strip().str.lower().replace('nan', None)

    # Remplir smoker, healthy et chronic_diseases correctement
    df['smoker'] = df['other_fields'].apply(lambda x: True if x == "smoker" else (False if x == "non-smoker" else None))
    df['healthy'] = df['other_fields'].apply(lambda x: True if x == "healthy" else (False if x == "non-healthy" else None))

    # Identifier les maladies chroniques (toutes les autres valeurs)
    mask = ~df['other_fields'].isin(["smoker", "non-smoker", "healthy", "non-healthy"]) & df['other_fields'].notna()
    df.loc[mask, 'chronic_diseases'] = df.loc[mask, 'other_fields']

    #  Création des tranches d'âge
    df['age_group'] = pd.cut(df['age'], bins=[0, 18, 35, 50, 100], labels=['enfant', 'jeune adulte', 'adulte', 'senior'])

    #  Changer les types de données des colonnes
    df['patient_id'] = df['patient_id'].astype(str)  # Assurez-vous que patient_id est bien de type text
    df['smoker'] = df['smoker'].astype('boolean')  # Booléen pour smoker
    df['healthy'] = df['healthy'].astype('boolean')  # Booléen pour healthy
    df['chronic_diseases'] = df['chronic_diseases'].astype(str)  # Assurez-vous que chronic_diseases est bien de type text
    df['age_group'] = df['age_group'].astype(str)  # Age group comme texte

    #  Vérification des types de données
    #print("🔍 Types des colonnes après transformation :")
    #print(df.dtypes)

    return df


#fonction de transformation patient_lab_results
def transform_lab_results(df):
    """Transformation des données lab_results."""
    print("🔍 Données AVANT transformation :")
    print(df.head(), "\n")

    #  Convertir alim_date en format date
    df['alim_date'] = pd.to_datetime(df['alim_date'], format='%Y%m%d', errors='coerce')

    #  Convertir result_value en numérique
    df['result_value'] = pd.to_numeric(df['result_value'], errors='coerce')

    #  Vérifier les unités et adapter les valeurs (uniquement pour g/dL)
    g_dl_mask = df['result_unit'] == 'g/dL'

    # Multiplier result_value par 1000 pour les unités g/dL
    df.loc[g_dl_mask, 'result_value'] *= 1000

    # Adapter reference_range pour g/dL (multiplication par 1000)
    def adjust_reference_range(row):
        if row['result_unit'] == 'g/dL' and isinstance(row['reference_range'], str):
            try:
                # Split la chaîne de caractères "X-Y" et multiplier par 1000
                min_range, max_range = map(float, row['reference_range'].split('-'))
                return [min_range * 1000, max_range * 1000]
            except:
                return [None, None]
        return row['reference_range']

    df['reference_range'] = df.apply(adjust_reference_range, axis=1)

    #  Normaliser result_unit en mg/dL (appliqué à tout le dataframe)
    df['result_unit'] = 'mg/dL'

    # Créer la colonne result_categorie
    def classify_result(row):
        # Si result_value est NaN, attribuer 'Missing result'
        if pd.isna(row['result_value']):
            return 'Missing result'

        # S'assurer que reference_range est une liste de 2 éléments
        if isinstance(row['reference_range'], str):
            # Si reference_range est une chaîne "X-Y", la convertir en liste de floats
            try:
                min_range, max_range = map(float, row['reference_range'].split('-'))
                reference_range = [min_range, max_range]
            except:
                reference_range = [None, None]
        else:
            reference_range = row['reference_range']

        # Vérification que reference_range est bien une liste de 2 éléments numériques
        if isinstance(reference_range, list) and len(reference_range) == 2:
            min_range, max_range = reference_range
            # Vérification que les valeurs de reference_range sont des nombres
            if isinstance(min_range, (int, float)) and isinstance(max_range, (int, float)):
                if row['result_value'] < min_range:
                    return 'low'
                elif row['result_value'] > max_range:
                    return 'high'
                else:
                    return 'normal'
        return 'inconnu'

    df['result_categorie'] = df.apply(classify_result, axis=1)

    #  Renommer la colonne notes en result_categorie_description
    df.rename(columns={'notes': 'result_categorie_description'}, inplace=True)

    #  Fixer les types des colonnes
    df = df.astype({
        'lab_test_id': 'string',
        'patient_id': 'string',
        'visit_id': 'string',
        'test_date': 'datetime64[ns]',
        'test_name': 'string',
        'result_value': 'float64',
        'result_unit': 'string',
        'result_categorie_description': 'string',
        'alim_date': 'datetime64[ns]',
        'result_categorie': 'string'
    })

    print(" Données APRÈS transformation :")
    print(df.head())

    return df

#fonction de transformation patient_medications
def transform_medications(df):
    # Nettoyer les noms de colonnes
    df.columns = df.columns.str.strip().str.lower()

    #  Convertir alim_date en format date
    df['alim_date'] = pd.to_datetime(df['alim_date'], format='%Y%m%d', errors='coerce')

    # Renommer la colonne 'medication' en 'medication_name'
    df.rename(columns={'medication': 'medication_name'}, inplace=True)

    # Assurer que les dates sont au bon format
    df['start_date'] = pd.to_datetime(df['start_date'], format='%d/%m/%Y', errors='coerce')
    df['end_date'] = pd.to_datetime(df['end_date'], format='%d/%m/%Y', errors='coerce')

    # Séparer la colonne 'dosage' en 'dosage_value' et 'dosage_unit'
    df[['dosage_value', 'dosage_unit']] = df['dosage'].str.extract(r'([0-9.]+)(mg|g|ml|mcg)')
    df['dosage_value'] = df['dosage_value'].astype(float)

    # Ajouter une colonne 'medication_duration' (durée en jours)
    df['medication_duration'] = (df['end_date'] - df['start_date']).dt.days
    df['medication_duration'] = df['medication_duration'].astype(int)  # Assurer que la durée est un entier

    # Conserver la colonne 'notes' sans modification
    df['notes'] = df['notes'].astype(str)  # Garantir que la colonne notes est de type TEXT

    # Créer la colonne 'medication_description' à remplir
    df['medication_description'] = ''

    # Dictionnaire pour mémoriser les derniers médicaments et dosages pour chaque patient
    previous_meds = {}

    # Remplir la colonne 'medication_description' en fonction des conditions
    for index, row in df.iterrows():
        patient_id = row['patient_id']
        medication_name = row['medication_name']
        dosage_value = row['dosage_value']

        # Si c'est le premier médicament pour ce patient
        if (patient_id, medication_name) not in previous_meds:
            df.at[index, 'medication_description'] = 'Initial prescription'
        else:
            # Si le médicament est le même que la visite précédente
            previous_dosage = previous_meds[(patient_id, medication_name)]
            if dosage_value == previous_dosage:
                df.at[index, 'medication_description'] = 'Review dosage'
            elif dosage_value > previous_dosage:
                df.at[index, 'medication_description'] = 'Increase in dosage'
            elif dosage_value < previous_dosage:
                df.at[index, 'medication_description'] = 'Decrease in dosage'

        # Mettre à jour le dernier médicament et dosage du patient
        previous_meds[(patient_id, medication_name)] = dosage_value

    # Assurer les types des colonnes de sortie
    df['medication_id'] = df['medication_id'].astype(str)  # TEXT
    df['patient_id'] = df['patient_id'].astype(str)  # TEXT
    df['visit_id'] = df['visit_id'].astype(str)  # TEXT
    df['medication_name'] = df['medication_name'].astype(str)  # TEXT
    df['dosage'] = df['dosage'].astype(str)  # TEXT
    df['start_date'] = df['start_date'].dt.date  # DATE
    df['end_date'] = df['end_date'].dt.date  # DATE
    df['dosage_unit'] = df['dosage_unit'].astype(str)  # TEXT
    df['medication_description'] = df['medication_description'].astype(str)  # TEXT

    # Retourner le DataFrame avec les types fixes
    return df


#fonction de transformation Patient_visits 
def transform_visits(df_visits, df_medications):
    #  Nettoyer les noms de colonnes
    df_visits.columns = df_visits.columns.str.strip().str.lower()
    df_medications.columns = df_medications.columns.str.strip().str.lower()

    #  Convertir alim_date en format date
    df_visits['alim_date'] = pd.to_datetime(df_visits['alim_date'], format='%Y%m%d', errors='coerce')

    #  Assurer que les identifiants sont bien formatés (éviter les espaces parasites)
    for col in ['patient_id', 'visit_id']:
        df_visits[col] = df_visits[col].astype(str).str.strip()
        df_medications[col] = df_medications[col].astype(str).str.strip()

    #  Assurer que les dates sont bien au format datetime
    df_visits['visit_date'] = pd.to_datetime(df_visits['visit_date'], format='%d/%m/%Y')

    #  Fusionner avec df_medications pour récupérer medication_name
    df_visits = df_visits.merge(
        df_medications[['patient_id', 'visit_id', 'medication_name']], 
        on=['patient_id', 'visit_id'], 
        how='left'
    )

    #  Identifier la première visite de chaque patient
    df_visits['first_visit'] = df_visits.groupby('patient_id')['visit_date'].transform('min') == df_visits['visit_date']

    # Supprimer la colonne 'medication' si elle existe
    if 'medication' in df_visits.columns:
        df_visits = df_visits.drop('medication', axis=1)

    #  Fixer les types explicites selon la structure de la base de données
    df_visits['visit_id'] = df_visits['visit_id'].astype(str)  # TEXT
    df_visits['patient_id'] = df_visits['patient_id'].astype(str)  # TEXT
    df_visits['visit_date'] = pd.to_datetime(df_visits['visit_date']).dt.date  # DATE
    df_visits['diagnosis'] = df_visits['diagnosis'].astype(str)  # TEXT (si nécessaire)
    df_visits['other_fields'] = df_visits['other_fields'].astype(str)  # TEXT (si nécessaire)
    df_visits['alim_date'] = pd.to_datetime(df_visits['alim_date']).dt.date  # DATE
    df_visits['medication_name'] = df_visits['medication_name'].astype(str)  # TEXT
    df_visits['first_visit'] = df_visits['first_visit'].astype(bool)  # BOOLEAN

    return df_visits


#fonction de transformation physician_assignments
def transform_physician_assignments(df_assignments, df_visits):
    # Nettoyer les colonnes
    df_assignments.columns = df_assignments.columns.str.strip().str.lower()
    df_visits.columns = df_visits.columns.str.strip().str.lower()

    # Convertir alim_date en format date
    df_assignments['alim_date'] = pd.to_datetime(df_assignments['alim_date'], format='%Y%m%d', errors='coerce')

    #  Assurer le bon format des identifiants
    for col in ['patient_id', 'visit_id']:
        df_assignments[col] = df_assignments[col].astype(str).str.strip()
        df_visits[col] = df_visits[col].astype(str).str.strip()

    # Convertir les dates en format datetime
    df_assignments['assignment_date'] = pd.to_datetime(df_assignments['assignment_date'], format='%d/%m/%Y')
    df_visits['visit_date'] = pd.to_datetime(df_visits['visit_date'], format='%Y-%m-%d')  # Format déjà transformé avant

    #  Fusionner avec df_visits pour obtenir visit_date
    df_assignments = df_assignments.merge(df_visits[['patient_id', 'visit_id', 'visit_date']], 
                                          on=['patient_id', 'visit_id'], 
                                          how='left')

    #  Vérifier la cohérence des dates
    df_assignments['valid_assignment'] = df_assignments['assignment_date'] <= df_assignments['visit_date']

    #  Assigner les types de données aux colonnes
    df_assignments['physician_id'] = df_assignments['physician_id'].astype('str')  # TEXT
    df_assignments['visit_id'] = df_assignments['visit_id'].astype('str')  # TEXT
    df_assignments['patient_id'] = df_assignments['patient_id'].astype('str')  # TEXT
    df_assignments['physician_name'] = df_assignments['physician_name'].astype('str')  # TEXT
    df_assignments['assignment_date'] = df_assignments['assignment_date'].astype('datetime64[ns]')  # DATE
    df_assignments['department'] = df_assignments['department'].astype('str')  # TEXT
    df_assignments['alim_date'] = df_assignments['alim_date'].astype('datetime64[ns]')  # DATE
    df_assignments['visit_date'] = df_assignments['visit_date'].astype('datetime64[ns]')  # DATE
    df_assignments['valid_assignment'] = df_assignments['valid_assignment'].astype('bool')  # BOOLEAN

    return df_assignments



#appel de la fonction de transformation avec le dataframe concerné dans les paramètre 
df_demographics_transformed = transform_patient_demographics(df_demographics)
print("df_demographics_transformed\n",df_demographics_transformed)  
df_lab_result_transformed = transform_lab_results(df_lab_results)
print("df_lab_result_transformed\n",df_lab_result_transformed)
df_lab_medication_transformed = transform_medications(df_medications)
print("df_lab_medication_transformed\n",df_lab_medication_transformed)
#df_lab_medication_transformed.to_csv("output.csv", index=False, encoding="utf-8")
df_visits_transformed = transform_visits(df_visits,df_lab_medication_transformed)
print("df_visits_transformed\n",df_visits_transformed)
df_physicians_transformed = transform_physician_assignments(df_physicians,df_visits_transformed)
print("df_physicians_transformed\n",df_physicians_transformed)

#---------------------------------------load------------------------------------------

# Paramètres de connexion à la base de données Silver
conn_silver = psycopg2.connect(
    host="localhost",
    dbname="silver_dw_health_db",
    user="postgres",
    password="salma2001",
    port="5432"
)

cursor_silver = conn_silver.cursor()

# Créer la table 'silver_patient_demographics'
cursor_silver.execute("""
    CREATE TABLE IF NOT EXISTS silver_patient_demographics (
        patient_id TEXT ,
        alim_date DATE,
        gender TEXT ,
        age INT,
        other_fields TEXT,
        smoker BOOLEAN,
        healthy BOOLEAN,
        chronic_diseases TEXT,
        age_group TEXT
    );
""")

# Créer la table 'silver_lab_result_transformed'
cursor_silver.execute("""
    CREATE TABLE IF NOT EXISTS silver_lab_result (
        lab_test_id TEXT,
        patient_id TEXT ,
        visit_id TEXT,
        test_date DATE,
        test_name TEXT ,
        result_value float,
        result_unit TEXT,
        reference_range TEXT,
        result_categorie_description TEXT,
        alim_date DATE,
        result_categorie TEXT
    );
""")

# Créer la table 'silver_lab_medication'
cursor_silver.execute("""
    CREATE TABLE IF NOT EXISTS silver_lab_medication (
        medication_id TEXT,
        patient_id TEXT ,
        visit_id TEXT,
        medication_name TEXT,
        dosage TEXT ,
        start_date DATE,
        end_date DATE,
        notes TEXT,
        alim_date DATE,
        dosage_value FLOAT,
        dosage_unit TEXT,
        medication_duration INT,
        medication_description TEXT
    );
""")

# Créer la table 'silver_visits'
cursor_silver.execute("""
    CREATE TABLE IF NOT EXISTS silver_visits (
        visit_id TEXT,
        patient_id TEXT ,
        visit_date DATE,
        diagnosis TEXT ,
        other_fields TEXT,
        alim_date DATE,
        medication_name TEXT,
        first_visit BOOLEAN
    );
""")

# Créer la table 'silver_physicians_assignment'
cursor_silver.execute("""
    CREATE TABLE IF NOT EXISTS silver_physicians_assignment (
        physician_id TEXT,
        visit_id TEXT,
        patient_id TEXT ,
        physician_name TEXT ,
        assignment_date DATE,
        department TEXT,
        alim_date DATE,
        visit_date DATE,
        valid_assignment BOOLEAN
    );
""")

# Fonction pour insérer les données dans la table Silver
def insert_silver_patient_demographics(df):
    # Remplacer explicitement NaN et pd.NA par None dans le DataFrame
    df = df.applymap(lambda x: None if pd.isna(x) else x)

    # Préparer la commande d'insertion avec gestion des doublons
    insert_query = """
    INSERT INTO silver_patient_demographics (
        patient_id, alim_date, gender, age, other_fields, smoker, healthy, chronic_diseases, age_group
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
   
    """
    
    # Convertir les DataFrame en liste de tuples pour l'insertion
    records = df[['patient_id', 'alim_date', 'gender', 'age', 'other_fields', 'smoker', 'healthy', 'chronic_diseases', 'age_group']].values.tolist()
    
    # Insérer les données dans la base de données
    psycopg2.extras.execute_batch(cursor_silver, insert_query, records)

def insert_silver_lab_result(df):
    # Remplacer explicitement NaN et pd.NA par None uniquement pour les colonnes contenant des types simples (float, int)
    df = df.applymap(lambda x: None if isinstance(x, (float, int)) and pd.isna(x) else x)
    
    # Préparer la commande d'insertion avec gestion des doublons
    insert_query = """
    INSERT INTO silver_lab_result (
        lab_test_id, patient_id, visit_id, test_date, test_name, result_value,
        result_unit, reference_range, result_categorie_description, alim_date, result_categorie
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Convertir les DataFrame en liste de tuples pour l'insertion
    records = df[['lab_test_id', 'patient_id', 'visit_id', 'test_date', 'test_name', 'result_value', 
                  'result_unit', 'reference_range', 'result_categorie_description', 'alim_date', 'result_categorie']].values.tolist()
    
    # Insérer les données dans la base de données
    psycopg2.extras.execute_batch(cursor_silver, insert_query, records)

# Fonction pour insérer les données dans la table 'silver_lab_medication'
def insert_silver_lab_medication(df):
    # Remplacer explicitement NaN et pd.NA par None dans le DataFrame
    df = df.applymap(lambda x: None if pd.isna(x) else x)

    # Préparer la commande d'insertion avec gestion des doublons
    insert_query = """
    INSERT INTO silver_lab_medication (
        medication_id, patient_id, visit_id, medication_name, dosage, 
        start_date, end_date, notes, alim_date, dosage_value, 
        dosage_unit, medication_duration, medication_description
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Convertir les DataFrame en liste de tuples pour l'insertion
    records = df[['medication_id', 'patient_id', 'visit_id', 'medication_name', 'dosage', 
                  'start_date', 'end_date', 'notes', 'alim_date', 'dosage_value', 
                  'dosage_unit', 'medication_duration', 'medication_description']].values.tolist()
    
    # Insérer les données dans la base de données
    try:
        psycopg2.extras.execute_batch(cursor_silver, insert_query, records)
        print(f"✅ {len(records)} enregistrements insérés dans 'silver_lab_medication'.")
    except Exception as e:
        print("Erreur lors de l'insertion des données dans 'silver_lab_medication':", e)

# Fonction pour insérer les données dans la table 'silver_visits'
def insert_silver_visits(df):
    # Remplacer explicitement NaN et pd.NA par None dans le DataFrame
    df = df.applymap(lambda x: None if pd.isna(x) else x)

    # Préparer la commande d'insertion avec gestion des doublons
    insert_query = """
    INSERT INTO silver_visits (
        visit_id, patient_id, visit_date, diagnosis, other_fields, alim_date, medication_name, first_visit
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Convertir les DataFrame en liste de tuples pour l'insertion
    records = df[['visit_id', 'patient_id', 'visit_date', 'diagnosis', 'other_fields', 
                  'alim_date', 'medication_name', 'first_visit']].values.tolist()
    
    # Insérer les données dans la base de données
    psycopg2.extras.execute_batch(cursor_silver, insert_query, records)

def insert_silver_physicians_assignment(df):
    # Remplacer explicitement NaN et pd.NA par None dans le DataFrame
    df = df.applymap(lambda x: None if pd.isna(x) else x)

    # Préparer la commande d'insertion avec gestion des doublons
    insert_query = """
    INSERT INTO silver_physicians_assignment (
        physician_id, visit_id, patient_id, physician_name, 
        assignment_date, department, alim_date, visit_date, valid_assignment
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Convertir les DataFrame en liste de tuples pour l'insertion
    records = df[['physician_id', 'visit_id', 'patient_id', 'physician_name', 
                  'assignment_date', 'department', 'alim_date', 'visit_date', 'valid_assignment']].values.tolist()
    
    # Insérer les données dans la base de données
    psycopg2.extras.execute_batch(cursor_silver, insert_query, records)




#  Insertion des données dans SILVER
insert_silver_patient_demographics(df_demographics_transformed)
#  Insertion des données dans la table 'silver_lab_result'
insert_silver_lab_result(df_lab_result_transformed)
# Insertion des données transformées dans la table 'silver_lab_medication'
insert_silver_lab_medication(df_lab_medication_transformed)
# Insérer les données dans la table silver_visits après transformation
insert_silver_visits(df_visits_transformed)
# Insérer les données dans la table silver_physicians_assignment 
insert_silver_physicians_assignment(df_physicians_transformed)

#  Validation et fermeture
conn_silver.commit()
cursor_silver.close()
conn_silver.close()

print("Données insérées dans la table 'silver_patient_demographics'.")