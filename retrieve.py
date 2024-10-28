import happybase

def retrieve_first_row_from_hbase():
    try:
        # Replace 'localhost' with your HBase server address if it's different
        connection = happybase.Connection('localhost', port=9090)

        # Define the table name
        table_name = 'my_reviews'

        # Access the table
        table = connection.table(table_name)

        # Scan the table and get the first row (CSV data)
        row_keys = table.scan(limit=1)

        # Retrieve and display the first row of CSV data
        for row_key, data in row_keys:
            print(f"Row Key: {row_key.decode('utf-8')}")

            # Iterate over each column family and column qualifier in the row
            for column, value in data.items():
                column_family, qualifier = column.decode('utf-8').split(':')
                print(f"Column Family: {column_family}, Qualifier: {qualifier}, Value: {value.decode('utf-8')}")

            # Exit after displaying the first row of data
            break  # Remove this line if you want to scan more rows

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection to avoid warnings
        if 'connection' in locals():
            connection.close()

# Call the function
retrieve_first_row_from_hbase()
