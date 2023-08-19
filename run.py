from Scripts.helpers import create_logs_and_results_tables
from Scripts.formatting import process_and_save_filtered_data
from Scripts.scraping_parcels_data import scrape_and_save_data
from pathlib import Path
import pandas as pd
import sqlite3


def write_csv_to_table(csv_file_path, table_name, connection, cursor):
    """
    Writes data from polygon_vertices to a CSV file.
    Additionally, writes data from a CSV file to an SQLite table in the database.

    :param csv_file_path: The file path of the CSV file containing the data to be inserted.
    :param table_name: The name of the SQLite table where the data will be inserted.
    :param connection: The SQLite connection to the database.
    :param cursor: The SQLite cursor to execute SQL queries.
    :return: None
    """
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS input (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT CHECK(status IN ('TODO', 'PROCESSING', 'DONE')),
            lat REAL,
            lng REAL
        )
    ''')

    df = pd.read_csv(csv_file_path)

    # Set 'status' to 'TODO' for all rows (assuming this is intended behavior)
    df['status'] = 'TODO'

    # Write data to SQLite table
    df.to_sql(table_name, connection, if_exists='append', index=False)


def initialize_data(connection, cursor, input_csv_file_path):
    """
    Initialize data by creating the 'input' table and populating it with data from a CSV file.

    :param connection: SQLite database connection.
    :param cursor: SQLite database cursor.
    :param input_csv_file_path: Path to the input CSV file.
    :return: None
    """
    # Check if the 'input' table already exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='input'")
    table_exists = cursor.fetchone() is not None

    # If the table exists, skip the function
    if table_exists:
        print("Table 'input' already exists. Skipping initialization.")
        return

    # If the table doesn't exist, create it and initialize data
    input_df = pd.read_csv(input_csv_file_path)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS input (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT CHECK(status IN ('TODO', 'PROCESSING', 'DONE')),
            lat REAL,
            lng REAL
        )
    ''')
    # Set 'status' to 'TODO' for all rows (assuming this is intended behavior)
    input_df['status'] = 'TODO'
    # Write data to SQLite table
    input_df.to_sql('input', connection, if_exists='append', index=False)
    # write_csv_to_table(input_csv_file_path, 'input', polygon_vertices, connection, cursor)
    create_logs_and_results_tables(cursor)


def main():
    connection = sqlite3.connect('Database/parcel_data.db')
    cursor = connection.cursor()

    input_csv_file_path = Path('./Input/parcel_lat_lng_data.csv')
    initialize_data(connection, cursor, input_csv_file_path)
    scrape_and_save_data(connection, cursor)

    # Formatting response from database and saving it to CSV file
    output_file = Path('Output/formatted_parcel_data.csv')
    process_and_save_filtered_data(output_file, cursor)

    connection.commit()
    connection.close()


if __name__ == "__main__":
    main()
