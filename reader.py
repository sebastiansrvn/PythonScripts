import pandas
import mysql.connector
import math
import logging

#
# 
# FILL DATA HERE
# 
#
user = ""
password = ""
host = ""
database_name = ""
table_name = ""
csv_name = ""

# Logging Config    
logging.basicConfig(filename='reader.log', filemode='w', format='%(asctime)s - %(message)s')

def columns_match(columns_arr, cursor):
    sql = "SHOW columns FROM " + table_name   
    cursor.execute(sql)
    col_names = cursor.fetchall()
    
    for col in col_names:
        if col[0] not in columns_arr:            
            error = "On file: " + csv_name + ". Column " + col[0] + " is not in DB or in CSV. Check DB and CSV fields."
            print(error)
            logging.warning(error)
            return "Column " + col[0] + " is not in DB or in CSV"

    return False

def insert_row(info, header, cursor, cnx): 

    for i in range(len(info)):         
        if isinstance(info[i], float) and math.isnan(info[i]): # Checks if cell is empty and assigns it empty string      
            info[i] = None
        if isinstance(info[i], str) and info[i].lower() == "null": # Checks for null and assigns it None  
            info[i] = None
        if isinstance(info[i], str):
            temp = info[i].replace(" ", "")

            if temp.lower() == "notfoundonline":
                info[i] = None

    s = ""
    for i in range(len(info)):
        s += " %s,"     

    s = s[:-1]    
    sql = "INSERT INTO " + table_name + " (" + header + ") VALUES (" + s + ")"
    
    
    #     
    # Tries to insert Null to DB. If the table entry is not nullable, it will go to the next try case
    #
    try:
        cursor.execute(sql, info)        
        cnx.commit()
        print("Imported")
        return
    except Exception as inst:        
        pass

    try:    
        for i in range(len(info)):
            if info[i] is None:
                info[i] = "Null"
        cursor.execute(sql, info)        
        cnx.commit()
        print("Imported")
    except Exception as inst:
        print(inst)
        logging.warning("On file: " + csv_name + " ." + str(inst))
        cnx.rollback()    
        
def main():
    
    # Connect to mysql
    try:
        #cnx = mysql.connector.connect(user=user, password=password, host=host, database=database_name, unix_socket='/Applications/MAMP/tmp/mysql/mysql.sock')
        cnx = mysql.connector.connect(user=user, password=password, host=host, database=database_name)
    except mysql.connector.Error as err:        
        print(err)
        logging.warning("Something went wrong: {}".format(err))
        return
        
    cursor = cnx.cursor()

    # Read CSV
    df = pandas.read_csv(csv_name)    

    # Column name
    row_one = df[:0]        
    
    
    # Check if column names from file match columns from DB
    column_match = columns_match(row_one, cursor)
    if column_match: 
        raise Exception(column_match)
     
    columns = ""
    for val in row_one:        
        if val == "":
            print(" ")
        #print(len(val))
        columns += (str(val) + ', ')
    
    # Column name string
    columns = columns[:-2]        
    
    # Loop through rows
    for i, row in df.iterrows():
        row_info = []        
        for key, val in row.iteritems(): # Loop through values
            row_info.append(val)        
        insert_row(row_info, columns, cursor, cnx) # Insert row to db            

    cursor.close()

    cnx.close()


if __name__ == "__main__":
    main()