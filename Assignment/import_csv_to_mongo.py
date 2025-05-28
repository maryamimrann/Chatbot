import pandas as pd
from pymongo import MongoClient

# Path to your CSV file
csv_file = 'PURCHASE ORDER DATA EXTRACT 2012-2015_0.csv'

# Read the CSV file
print(f"Reading CSV file: {csv_file}")
df = pd.read_csv(csv_file)
print(f"Loaded {len(df)} rows from CSV.")

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['orderData']
collection = db['sample']

# Clear the collection first
print("Deleting existing documents in 'sample' collection...")
collection.delete_many({})

# Insert new data
print("Inserting new documents...")
collection.insert_many(df.to_dict('records'))
print(f"Inserted {collection.count_documents({})} documents into 'sample' collection.")

