import happybase

def test_hbase_connection(host='localhost', port=9090, table_name='my_reviews'):
    try:
        # Attempt to connect to HBase
        connection = happybase.Connection(host=host, port=port)
        print("Successfully connected to HBase.")

        # List tables
        tables = connection.tables()
        print(f"Available tables: {tables}")

        # Open the specific table
        table = connection.table(table_name)
        print(f"Successfully opened table: {table_name}")

        # Retrieve the first row from HBase
        retrieve_first_row_from_hbase(table)

        # Close the connection
        connection.close()
        print("HBase connection test completed successfully.")
    except Exception as e:
        print(f"Error during HBase connection test: {type(e).__name__}: {str(e)}")


def retrieve_first_row_from_hbase(table):
    try:
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
        print(f"An error occurred while retrieving the row: {e}")


if __name__ == "__main__":
    test_hbase_connection()
