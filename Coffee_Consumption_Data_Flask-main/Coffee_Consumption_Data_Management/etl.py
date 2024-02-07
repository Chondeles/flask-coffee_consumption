import pandas as pd
import mysql.connector
from mysql.connector import Error

# Chemin d'accès au fichier CSV
csv_file_path = 'C:/Users/lenovo/Desktop/ESISA Workspace/3eme Année/S5/Python/Python_Flask/Coffee_domestic_consumption.csv'

# Lecture et nettoyage des données
df = pd.read_csv(csv_file_path)
df = df[df['Total_domestic_consumption'] != 0]  # Supprimer les lignes avec une consommation de 0
df.drop_duplicates(inplace=True)  # Supprimer les doublons

# Connexion à la base de données
try:
    connection = mysql.connector.connect(
        host='localhost',
        database='coffeedb',
        user='root',
        password='10122002'
    )

    if connection.is_connected():
        cursor = connection.cursor()

        # Insertion des pays uniques
        countries = df['Country'].unique()
        for country in countries:
            cursor.execute("SELECT id_country FROM T_country WHERE country_name = %s", (country,))
            if not cursor.fetchone():
                insert_country_query = "INSERT INTO T_country (country_name) VALUES (%s)"
                cursor.execute(insert_country_query, (country,))

        # Insertion des types de café uniques
        coffee_types = df['Coffee type'].unique()
        for coffee_type in coffee_types:
            cursor.execute("SELECT id_coffee FROM T_Coffee WHERE coffee_type = %s", (coffee_type,))
            if not cursor.fetchone():
                insert_coffee_type_query = "INSERT INTO T_Coffee (coffee_type) VALUES (%s)"
                cursor.execute(insert_coffee_type_query, (coffee_type,))

        # Insertion des données de consommation pour chaque pays et type de café
        for index, row in df.iterrows():
            country_name = row['Country']
            coffee_type = row['Coffee type']

            # Récupérer l'ID du pays et du type de café
            cursor.execute("SELECT id_coffee FROM T_Coffee WHERE coffee_type = %s", (coffee_type,))
            coffee_id = cursor.fetchone()[0]
            
            cursor.execute("SELECT id_country FROM T_country WHERE country_name = %s", (country_name,))
            country_id = cursor.fetchone()[0]

            # Insérer les données annuelles
            for year in range(1990, 2020):  # Ajustez ici la plage des années
                year_column = f"{year}/{str(year+1)[-2:]}"
                if year_column in df.columns:  # Vérifiez si la colonne existe
                    consumption = row[year_column]
                    if pd.notna(consumption):  # Vérifier si la consommation n'est pas NaN
                        insert_consumption_query = """
                            INSERT INTO T_consumption (id_coffee, id_country, year, consumption) 
                            VALUES (%s, %s, %s, %s)
                        """
                    cursor.execute(insert_consumption_query, (coffee_id, country_id, year, consumption))

        connection.commit()

except Error as e:
    print("Erreur lors de la connexion à MySQL", e)
finally:
    if (connection.is_connected()):
        cursor.close()
        connection.close()
        print("Connexion MySQL est fermée")
