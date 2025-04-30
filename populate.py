import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid

# Connect to Cassandra
def connect_to_cassandra():
    # If authentication is required, specify your username and password
    # auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')
    cluster = Cluster(['127.0.0.1'])  # Cassandra cluster IP address
    session = cluster.connect()
    return session

# Create keyspace and table if not already created
def create_keyspace_and_table(session):
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)
    
    session.set_keyspace('my_keyspace')
    
    session.execute("""
    CREATE TABLE IF NOT EXISTS my_table (
        id UUID PRIMARY KEY,
        name TEXT,
        age INT,
        city TEXT
    );
    """)

# Insert data from CSV with selected columns
def insert_data_from_csv(session, csv_file_path):
    # Read only specific columns from CSV
    df = pd.read_csv(csv_file_path, usecols=['name', 'age', 'city'])  # Specify the columns you want to use
    
    # Prepare an insert statement
    insert_statement = session.prepare("""
    INSERT INTO my_table (id, name, age, city) VALUES (?, ?, ?, ?)
    """)
    
    # Iterate through rows and insert data
    for index, row in df.iterrows():
        # Generate a new UUID for each row
        row_id = uuid.uuid4()
        session.execute(insert_statement, (row_id, row['name'], row['age'], row['city']))

# Main function
def main():
    session = connect_to_cassandra()
    create_keyspace_and_table(session)
    insert_data_from_csv(session, 'your_data.csv')  # Replace with your CSV file path
    print("Data insertion completed!")

if __name__ == "__main__":
    main()
