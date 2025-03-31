Description du Projet
Ce projet a pour objectif de construire un data warehouse dans le domaine de la santé, permettant de centraliser les données synthétiques d'études cliniques dans une base de données PostgreSQL autonome et dans une instance Supabase.

Le projet prend la forme d'un pipeline de bout en bout, permettant d'extraire les données brutes à partir des fichiers sources, de les historiser, de les nettoyer, de les transformer et de les charger dans une base de données PostgreSQL ainsi que dans une instance locale Supabase.
################################################################################################
Explication fonctionnelle du projet :
Le projet se compose principalement de trois couches qui s'enchaînent dans l'ordre suivant :

- Couche raw : contient les données brutes.

- Couche silver : contient les données nettoyées, transformées et prêtes à être analysées et utilisées.

- Couche gold : expose les données et les met à la disposition des utilisateurs finaux des applications.

Chaque couche du projet est représentée par un ETL :

- Couche raw : ETL d'ingestion
    Cet ETL permet de récupérer les données brutes à partir des fichiers sources, de les stocker dans une base de données tout en conservant leur format brut. Un champ alim_dat (date d'alimentation) est ajouté pour tracer la date de disponibilité des données et les historiser.

- Couche silver : ETL de modélisation
    Cet ETL permet d'extraire les données brutes de la couche raw en se connectant à la base de données raw. Il nettoie, transforme les données dans les formats cibles et les charge dans une base de données silver. Dans cette couche, les données sont traitées et prêtes à être analysées et utilisées.

- Couche gold : ETL d'exposition
    Cet ETL permet de récupérer les données de la couche silver et de les exposer dans une instance locale Supabase, afin de les mettre à la disposition des utilisateurs finaux et des applications.
################################################################################################
Fonctionnalités des ETL :
- ETL d'ingestion :
    Extraire les données à partir des fichiers sources.

    Ajouter une colonne pour stocker la date d'alimentation des données.

    Créer des tables dans la base de données raw dans PostgreSQL.

    Charger les données dans la base raw.

- ETL de modélisation :
    Se connecter à la base raw.

    Récupérer les données brutes.

    Nettoyer et transformer les données brutes.

    Créer des tables dans la base de données silver.

    Charger les données transformées dans la base de données silver.

- ETL d'exposition :
    Se connecter à la base de données silver.

    Se connecter à la base de données Supabase.

    Migrer les données de la base silver à la base gold.

Librairies Python requises :
psycopg2
pandas
supabase
logging
datetime
os