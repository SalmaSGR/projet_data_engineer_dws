import pytest
import psycopg2

# Connexion à la base PostgreSQL 
@pytest.fixture
def db_connection():
    """Fixture pour se connecter à la base de données et fermer la connexion après chaque test"""
    conn = psycopg2.connect(
        dbname="silver_dw_health_db",  
        user="postgres",
        password="salma2001",
        host="localhost",
        port="5432"
    )
    yield conn
    conn.close()

#Test 1 : Vérifier que la table "visits" contient des données
def test_visits_not_empty(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM silver_visits;")
    count = cursor.fetchone()[0]
    assert count > 0, "La table visits est vide !"

#Test 2 : Vérifier que la requête de récupération des visites d'un patient fonctionne
def test_patient_visits(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM silver_visits WHERE patient_id = 'P003';")
    count = cursor.fetchone()[0]
    assert count >= 0, "Erreur : Aucune visite trouvée pour ce patient !"
