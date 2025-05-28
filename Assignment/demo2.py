import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['orderData']
collection = db['sample']

# Retrieve all documents
data = list(collection.find())

# Convert to Pandas DataFrame
df = pd.DataFrame(data)

count = collection.count_documents({})
print(f"Total rows: {count}")

