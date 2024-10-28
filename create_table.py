import os
import csv
import happybase

# Replace 'localhost' with your HBase server address if it's different
connection = happybase.Connection('localhost', port=9090)

# Define the table name
table_name = 'my_reviews'

# Column families
column_families = {
    'data': dict()  # Adjust as needed for your dataset
}

# Check if the table already exists, otherwise create it
if table_name.encode() in connection.tables():
    print(f"Table {table_name} already exists.")
else:
    # Create the table with column families
    connection.create_table(table_name, column_families)
    print(f"Table {table_name} created successfully with 'data' column family.")

# Define the path to the CSV file
csv_file_path = r"D:\amazon_reviews\amazon_reviews\data\modified_reviews.csv"

# Access the table
table = connection.table(table_name)

# Read the CSV file
with open(csv_file_path, 'r',encoding='utf-8') as csv_file:
    reader = csv.reader(csv_file)
    
    # Get the header (column names)
    headers = next(reader)  # Assumes first row is the header
    
    # Iterate through the CSV rows
    for row in reader:
        row_key = row[0]  # You can adjust the row key based on your dataset structure
        
        # Create a dictionary for HBase data, mapping column family to each value
        data = {f'data:{headers[i]}': row[i] for i in range(1, len(row))}  # Skips the row key
        
        # Insert the row into HBase
        table.put(row_key, data)
        
        print(f"Inserted row '{row_key}' into HBase.")

# Close the connection
connection.close()
