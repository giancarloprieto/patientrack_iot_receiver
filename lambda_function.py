import os

from db_connector import DatabaseConnector
from dotenv import load_dotenv
from processing import Processing


def lambda_handler(event, context):
    current_directory = os.path.dirname(os.path.abspath(__file__))
    dotenv_path = os.path.join(current_directory, ".env")
    load_dotenv(dotenv_path)
    db_connector = DatabaseConnector()
    try:
        db_connector.connect()
        processor = Processing(event['data'], db_connector)
        processor.run()
    except Exception as e:
        print(f'ERROR {e}')
        db_connector.close_connection()
    else:
        db_connector.close_connection()
