import os

import psycopg2


class DatabaseConnector:
    def __init__(self):
        self.db_params = {
            'dbname': os.getenv("DB_NAME", "patientrack"),
            'user': os.getenv("DB_USERNAME", "postgres"),
            'password': os.getenv("DB_PASSWORD"),
            'host': os.getenv("DB_HOST"),
            'port': os.getenv("DB_PORT", 5432),
        }
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.db_params)
        except Exception as e:
            print(f"ERROR CONNECTING TO DATABASE: {e}")

    def close_connection(self):
        if self.connection:
            self.connection.close()

    def fetch_one(self, query, params=dict):
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor.fetchone()

    def fetch_all(self, query, params=dict):
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    def insert(self, query, params=dict):
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.connection.commit()
        cursor.close()
