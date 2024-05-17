# this code just build 10  tables with 50 columns, you should build two 200 columns in DataBase with SQL query(like query we use in this code)

from asyncio.windows_events import NULL
import sqlite3
import pandas as pd
import os
import time
from tqdm import tqdm
##################################################################################################################################
tech_terms_id_dict = {}
all_tables_ID = []
data_base_TF = os.getcwd() + "\\data\\Robotics_tech_terms_Frequency_Ntables.db"
data_base_TF_total =  os.getcwd() + f"\\data\\Robotics_tech_terms_TF_Nfifty_tables_and_two_200Col_tables.db"
tech_terms_list_file = os.getcwd() + r"\\Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + "\\logs\\Robotics_tech_terms_TF_Nfifty_tables_and_two_200Col_tables.log"
log_file = open(tech_terms_log_file, 'a', encoding = 'utf-8')
##################################################################################################################################
def createConnection(db_file):
    #create a database connection to a SQLite database
    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except Exception as ex:
        log_file.write("Connection failed.\n")
        print("\nConnection failed.", ex)
    return connection
##################################################################################################################################
def createDataBaseTables():
    sql_create_tech_terms_ID_table = """CREATE TABLE IF NOT EXISTS Tech_Terms_ID (
                                        ID integer PRIMARY KEY,
                                        Term text NOT NULL
                                    ); """ 

    connection = createConnection(data_base_TF_total)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(sql_create_tech_terms_ID_table)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create table in Freq data base: {ex}, Exiting from program ...\n")
            print(f"\nCan't create tables in Freq data base{ex}, Exiting from program ...")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't create tables in Freq data base. Exiting from program...\n")
        print("\nConnection is failed. Can't create tables in Freq data base. Exiting from program...")
        quit()
##################################################################################################################################
def writeTermsInDataBase(tech_terms):
    #writing terms-id in Tech_Terms_ID table
    connection = createConnection(data_base_TF_total)  
    if connection != None:
        crsr = connection.cursor()
        try:
            for i in range(len(tech_terms)):
                crsr.execute(f"""INSERT INTO Tech_Terms_ID VALUES({i}, '{tech_terms[i]}') """)    
            connection.commit()  
        except Exception as ex:
            log_file.write(f"writing tech terms in table failed.(probably be written before): {ex}\n")
            print(f"\nwriting tech terms in table failed.(probably be written before): {ex}")
        
        connection.close()
    else:
        log_file.write("Connection is failed. Can't write terms-id in tables. Exiting from program...\n")
        print("\nConnection is failed. Can't write terms-id in tables. Exiting from program...")
        quit()
##################################################################################################################################
def getAllTablesID(idx):
    all_tables_ID.clear()
    connection = createConnection(data_base_TF)
    if connection != None:
        crsr = connection.cursor()
        try:
            for id in range(idx * 50, (idx +1) * 50):
                crsr.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='FreqTech_{id}'")
                if crsr.fetchall()[0][0] == 1:
                    all_tables_ID.append(id)
        except Exception as ex:
            log_file.write(f"Can't get Tables ID, {ex}. Exiting program...\n")
            quit()
        connection.close()
    
    else:
        log_file.write(f"Connection failed. Can't get Tables ID, {ex}. Exiting program...\n")
        quit()
##################################################################################################################################
def buildQuery():
    query = ""
    freq_statement = ""
    for id in all_tables_ID:
        freq_statement = freq_statement + f", FreqTech_{id}.Freq_{id}"

    for id in all_tables_ID:
        tmp = f"SELECT FreqTech_{id}.Term {freq_statement} FROM FreqTech_{id}\n"
        
        for id2 in [d for d in all_tables_ID if d != id]:
            tmp = tmp + f"LEFT JOIN FreqTech_{id2} ON FreqTech_{id}.Term = FreqTech_{id2}.Term\n"
        
        tmp2 = ""
        for id2 in all_tables_ID:
            if id2 < id:
                tmp2 = tmp2 + f"FreqTech_{id2}.Freq_{id2} IS NULL"
                if all_tables_ID.index(id2) < all_tables_ID.index(id) - 1:
                    tmp2 = tmp2 + " AND "
        if tmp2 != "":
            tmp = tmp + "WHERE " + tmp2 + "\n"

        if id != max(all_tables_ID):
            tmp = tmp + "UNION ALL\n"

        query = query + tmp

    return query
##################################################################################################################################
def createTFFiftyTable(idx):
    sql_TF_table_create_query = f"""CREATE TABLE IF NOT EXISTS Terms_Frequency_{idx} (
                                        Term text NOT NULL """
    tmp = ""
    for id in all_tables_ID:
        tmp = tmp + f", FreqTech_{id} integer"
    
    sql_TF_table_create_query = sql_TF_table_create_query + tmp + ");"

    connection = createConnection(data_base_TF_total)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(sql_TF_table_create_query)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create table in Freq data base: {ex}, Exiting from program ...\n")
            print(f"\nCan't create tables in Freq data base{ex}, Exiting from program ...")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't create tables in Freq data base. Exiting from program...\n")
        print("\nConnection is failed. Can't create tables in Freq data base. Exiting from program...")
        quit()    
##################################################################################################################################
def writeQueryResultToTable(query, idx):

    records = []
    connection = createConnection(data_base_TF)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(query)
            records = crsr.fetchall()
        except Exception as ex:
            log_file.write(f"Can't get reslut of query, {ex}. Exiting ...\n")
            quit()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get reslut of query, {ex}. Exiting ...\n")
        quit()

    connection = createConnection(data_base_TF_total)
    if connection != None:
        crsr = connection.cursor()
        try:          
            sql_query = f"""INSERT INTO Terms_Frequency_{idx} (Term"""
            str_tmp = ""
            str_tmp2 = "VALUES(?"
            for id in all_tables_ID:
                str_tmp = str_tmp + ", FreqTech_" + str(id)
                str_tmp2 = str_tmp2 + ",?"
            sql_query = sql_query + str_tmp + ")" + str_tmp2 + ")"

            crsr.execute("BEGIN TRANSACTION")
            crsr.executemany(sql_query, records)
            crsr.execute("COMMIT")
        except Exception as ex:
            log_file.write(f"Can't write records to TF total, {ex}. Exiting ...\n")
            quit()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't write records to TF total, {ex}. Exiting ...\n")
        quit()
##################################################################################################################################
def main():
    print("----------------------------------------------------------------------------------------------")

    # Create empty database
    createDataBaseTables()
    
    # Read terms from file
    tech_terms = pd.read_csv(tech_terms_list_file)
    tech_terms_list = [x for x in tech_terms['Tech_Terms']]
    #print(tech_terms)

    # Assign id to every term
    global tech_terms_id_dict
    for i in range(len(tech_terms_list)):
        tech_terms_id_dict[tech_terms_list[i]] = i

    # Write terms-id in database
    writeTermsInDataBase(tech_terms_list)   

    # Write some info to Log file
    log_file.write("########################################################################################\n")
    log_file.write(f"Term Frequency TF_Nfifty_tables_and_two_200Col_tables. 10 pack of fifty tech term.\n")
    log_file.write("########################################################################################\n")

    for idx in tqdm(range(10)):
        TF_start = time.time()
        getAllTablesID(idx)
        log_file.write(f"The {idx}'th fifty tech terms\n\
                        Tech terms IDs: {all_tables_ID}\n")
        log_file.flush()
        createTFFiftyTable(idx)
        query = buildQuery()
        #log_file.write(query)
        #log_file.flush()
        #print(query)

        print("writing query result in table...\n")
        
        writeQueryResultToTable(query, idx)
        TF_end = time.time()
          
        TF_elapsed_time = str(int((TF_end - TF_start) / 3600)) + "h " + str(int(((TF_end - TF_start) % 3600) / 60)) + "m"
        log_file.write(f"Time: {TF_elapsed_time}\n")        
        log_file.write("----------------------------------------------------------------------\n")
        log_file.flush()
##################################################################################################################################
if __name__ == '__main__':
    main()