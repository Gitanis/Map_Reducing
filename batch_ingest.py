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

#get the pointer to a table
def get_table(name):
    open_connection() # opens a connection to the hbase
    #getting a pointer to the table
    table = connection.table(name)
    close_connection()# closes an opened connection
    return table

def batch_data_insert(filename,start_count):
    print("starting batch data insert of yellow_trip_hb")
    file = open(filename, "r")#opening a file
    table = get_table("yellow_trip_hb")# get the pointer to a table
    open_connection()  # opens a connection to the hbase

    row_key = start_count
    i = 0
    with table.batch(batch_size=1000) as b:
        for line in file:
            if i!=0:
                dummy = line.strip().split(",")
                # this put() will result in two mutations (two cells)
                b.put(row_key, { "yellow_trip_columns:VendorID": dummy[0], "yellow_trip_columns:tpep_pickup_datetime": dummy[1], "yellow_trip_columns:tpep_dropoff_datetime": dummy[2],"yellow_trip_columns:passenger_count": dummy[3],"yellow_trip_columns:trip_distance": dummy[4],"yellow_trip_columns:RatecodeID": dummy[5],"yellow_trip_columns:store_and_fwd_flag": dummy[6], "yellow_trip_columns:pulocationid": dummy[7], "yellow_trip_columns:dolocationid": dummy[8], "yellow_trip_columns:payment_type": dummy[9],"yellow_trip_columns:fare_amount": dummy[10],"yellow_trip_columns:extra": dummy[11],"yellow_trip_columns:mta_tax": dummy[12],"yellow_trip_columns:tip_amount": dummy[13],"yellow_trip_columns:tolls_amount": dummy[14],"yellow_trip_columns:improvement_surcharge": dummy[15],"yellow_trip_columns:total_amount": dummy[16],"yellow_trip_columns:congestion_surcharge": dummy[17],"yellow_trip_columns:airport_fee": dummy[18]})  
                row_key+= 1
            
            i+=1
               
                          

    file.close()# closing the opened file
    print("batch data insert is done")
    close_connection() # closes an opened connection
    return row_key
    
rows1 = batch_data_insert('yellow_tripdata_2017-03.csv', '18880589')
rows2 = batch_data_insert('yellow_tripdata_2017-04.csv', rows1)