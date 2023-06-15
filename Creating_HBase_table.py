import happybase

#create a connection to the hbase
connection = happybase.Connection('localhost', port=9090 , autoconnect=False)

#opening a connection to the hbase
def open_connection():
    connection.open()

#closing an opened connection
def close_connection():
    connection.close()

#Listing the tables present in the hbase
def list_tables():
    print("fetching all the tables")
    open_connection() # opens a connection to the hbase

    # storing the names of the tables
    tables_list = connection.tables()

    close_connection()# closes an opened connection
    print("all the tables have been fetched")
    return tables_list

#creating a table by passing name and column family as  parameters
def create_table(name,cf):
    print("creating table " + name)
    tables = list_tables()
    if name not in tables:
        open_connection() # opens a connection to the hbase
        connection.create_table(name, cf)
        close_connection() # closes an opened connection
        print(f"table {name} is created")
    else:
        print(f"table {name} is already present")
    
create_table('yellow_trip_hb', {'yellow_trip_columns' : dict() })
