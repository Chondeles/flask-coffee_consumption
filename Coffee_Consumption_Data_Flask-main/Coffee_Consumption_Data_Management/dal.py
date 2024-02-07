import mysql.connector as mysql
from mysql.connector import Error
from model import User, Country, Coffee, Consumption

class CoffeeDatabase:

    def __init__(self, user, password, host, database):
        self.connection = mysql.connect(
            user=user,
            password=password,
            host=host,
            database=database
        )
        self.cursor = self.connection.cursor()

    def close_db_connection(self):
        if self.connection.is_connected():
            self.connection.close()
            print("MySQL connection is closed")

    def insert_country(self, country_name):
        try:
            self.cursor.execute("INSERT INTO T_country (country_name) VALUES (%s)", (country_name,))
            self.connection.commit()
            print(f"Country '{country_name}' added successfully.")
        except Error as err:
            print(f"Error: '{err}'")

    def get_country_by_id(self, country_id):
        try:
            self.cursor.execute("SELECT * FROM T_country WHERE id_country = %s", (country_id,))
            result = self.cursor.fetchone()
            if result:
                return Country(country_id=result[0], country_name=result[1])
            return None
        except Error as err:
            print(f"Error: '{err}'")
            return None

    def insert_coffee_type(self, coffee_type):
        try:
            self.cursor.execute("INSERT INTO T_Coffee (coffee_type) VALUES (%s)", (coffee_type,))
            self.connection.commit()
            print(f"Coffee type '{coffee_type}' added successfully.")
        except Error as err:
            print(f"Error: '{err}'")

    def get_coffee_by_id(self, coffee_id):
        try:
            self.cursor.execute("SELECT * FROM T_Coffee WHERE id_coffee = %s", (coffee_id,))
            result = self.cursor.fetchone()
            if result:
                return Coffee(coffee_id=result[0], coffee_type=result[1])
            return None
        except Error as err:
            print(f"Error: '{err}'")
            return None

    def insert_consumption_data(self, coffee_id, country_id, year, consumption):
        try:
            self.cursor.execute("""
                INSERT INTO T_consumption (id_coffee, id_country, year, consumption) 
                VALUES (%s, %s, %s, %s)
            """, (coffee_id, country_id, year, consumption))
            self.connection.commit()
            print(f"Consumption data for year {year} added successfully.")
        except Error as err:
            print(f"Error: '{err}'")

    def get_extended_consumption_data(self):
        try:
            query = """
            SELECT T_consumption.id, T_Coffee.coffee_type, T_country.country_name, T_consumption.year, T_consumption.consumption 
            FROM T_consumption
            JOIN T_Coffee ON T_consumption.id_coffee = T_Coffee.id_coffee
            JOIN T_country ON T_consumption.id_country = T_country.id_country
            """
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results  # Each result will include coffee type and country name along with consumption data
        except Error as err:
            print(f"Error: '{err}'")
            return []

    def update_consumption_data(self, consumption_id, new_consumption):
        try:
            self.cursor.execute("""
                UPDATE T_consumption 
                SET consumption = %s 
                WHERE id = %s
            """, (new_consumption, consumption_id))
            self.connection.commit()
            print(f"Consumption data updated successfully.")
        except Error as err:
            print(f"Error: '{err}'")

    def delete_consumption_data(self, consumption_id):
        try:
            self.cursor.execute("DELETE FROM T_consumption WHERE id = %s", (consumption_id,))
            self.connection.commit()
            print(f"Consumption data deleted successfully.")
        except Error as err:
            print(f"Error: '{err}'")

    def get_user_by_email(self, email):
        try:
            self.cursor.execute("SELECT * FROM users WHERE email = %s", (email,))
            record = self.cursor.fetchone()
            if record:
                return User(user_id=record[0], email=record[1], password=record[2], is_admin=record[3])
            return None
        except Error as err:
            print(f"Error: '{err}'")
            return None

    def create_user(self, email, password):
        try:
            self.cursor.execute("INSERT INTO users (email, password) VALUES (%s, %s)", (email, password))
            self.connection.commit()
            print("User created successfully.")
        except Error as err:
            print(f"Error: '{err}'")


    def insert_coffee_data(self, coffee_type):
        try:
            self.cursor.execute("INSERT INTO T_Coffee (coffee_type) VALUES (%s)", (coffee_type,))
            self.connection.commit()
        except Error as err:
            print(f"Error: '{err}'")
            self.connection.rollback()

    def insert_full_coffee_data(self, coffee_type, country_name, year, consumption):
        try:
            # Insert country if it doesn't exist
            self.cursor.execute("INSERT INTO T_country (country_name) VALUES (%s) ON DUPLICATE KEY UPDATE country_name=country_name", (country_name,))
            self.cursor.execute("SELECT id_country FROM T_country WHERE country_name = %s", (country_name,))
            country_id = self.cursor.fetchone()[0]

            # Insert coffee type if it doesn't exist
            self.cursor.execute("INSERT INTO T_Coffee (coffee_type) VALUES (%s) ON DUPLICATE KEY UPDATE coffee_type=coffee_type", (coffee_type,))
            self.cursor.execute("SELECT id_coffee FROM T_Coffee WHERE coffee_type = %s", (coffee_type,))
            coffee_id = self.cursor.fetchone()[0]

            # Insert consumption data
            self.cursor.execute("""
                INSERT INTO T_consumption (id_coffee, id_country, year, consumption) 
                VALUES (%s, %s, %s, %s)
            """, (coffee_id, country_id, year, consumption))
            self.connection.commit()
        except Error as err:
            print(f"Error: '{err}'")
            self.connection.rollback()

    def update_coffee_data(self, consumption_id, coffee_type, country_name, year, consumption):
        try:
            # Ensure country exists in T_country
            self.cursor.execute("INSERT INTO T_country (country_name) VALUES (%s) ON DUPLICATE KEY UPDATE country_name=country_name", (country_name,))
            self.cursor.execute("SELECT id_country FROM T_country WHERE country_name = %s", (country_name,))
            country_id = self.cursor.fetchone()[0]
            
            # Ensure coffee type exists in T_Coffee
            self.cursor.execute("INSERT INTO T_Coffee (coffee_type) VALUES (%s) ON DUPLICATE KEY UPDATE coffee_type=coffee_type", (coffee_type,))
            self.cursor.execute("SELECT id_coffee FROM T_Coffee WHERE coffee_type = %s", (coffee_type,))
            coffee_id = self.cursor.fetchone()[0]

            # Update the consumption record in T_consumption
            self.cursor.execute("""
                UPDATE T_consumption 
                SET id_coffee = %s, id_country = %s, year = %s, consumption = %s
                WHERE id = %s
            """, (coffee_id, country_id, year, consumption, consumption_id))
            self.connection.commit()
        except Error as err:
            print(f"Error: '{err}'")
            self.connection.rollback()

    def get_specific_coffee_data(self, id):
        try:
            self.cursor.execute("""
                SELECT T_consumption.id, T_Coffee.coffee_type, T_country.country_name, T_consumption.year, T_consumption.consumption 
                FROM T_consumption
                JOIN T_Coffee ON T_consumption.id_coffee = T_Coffee.id_coffee
                JOIN T_country ON T_consumption.id_country = T_country.id_country
                WHERE T_consumption.id = %s
            """, (id,))
            result = self.cursor.fetchone()
            if result:
                return {
                    'consumption_id': result[0], 
                    'coffee_type': result[1], 
                    'country_name': result[2], 
                    'year': result[3], 
                    'consumption': result[4]
                }
            return None
        except Error as err:
            print(f"Error: '{err}'")
            return None
    
    def delete_coffee_data(self, consumption_id):
        try:
            self.cursor.execute("DELETE FROM T_consumption WHERE id = %s", (consumption_id,))
            self.connection.commit()
            
            print(f"Deleted coffee data with ID: {consumption_id}")  # Debugging print

        except Error as err:
            print(f"Error: '{err}'")
            self.connection.rollback()