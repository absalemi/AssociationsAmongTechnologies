import sqlite3
import pandas as pd
import os
import time
import math
import numpy as np
from tqdm import tqdm
##################################################################################################################################
tech_terms_id_dict = {}
all_tech_ID = []
TFIDF_cosine_distance = [] #[ (--dist0--), (--dist1--),... ] dist0 is a list. dist of tech0 from other techs
normalized_percent = []
data_base_MD =  os.getcwd() + f"/data/Robotics_tech_terms_measuring_cosine_distance.db"
data_base_NP = os.getcwd() + f"/data/Robotics_tech_terms_normalized_percent.db"
tech_terms_list_file = os.getcwd() + r"/Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + "/logs/Robotics_tech_terms_normalized_percent.log"
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

    connection = createConnection(data_base_NP)
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
    connection = createConnection(data_base_NP)  
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
def getAllTechID():
    global all_tech_ID
    for rg in range(10):
        all_tech_ID.append(rg)

    #all_tech_ID.remove(48)
    #all_tech_ID.remove(82)
##################################################################################################################################
def getListOfTables():
    all_tables = set()
    finished_tables = set()

    connection = createConnection(data_base_MD)
    if connection != None:
        crsr = connection.cursor()
        try :
            crsr.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'distance_%' OR name LIKE 'cosine_%')")
            records = crsr.fetchall()
            for rg in range(len(records)):
                all_tables.add(records[rg][0])
        except Exception as ex:
            log_file.write(f"Can't get list of tables: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get list of tables: {ex}. Exiting program...\n")
        quit()

    connection = createConnection(data_base_NP)
    if connection != None:
        crsr = connection.cursor()
        try :
            crsr.execute("SELECT name FROM sqlite_master WHERE type='table' AND (name LIKE 'distance_%' OR name LIKE 'cosine_%')")
            records = crsr.fetchall()
            for rg in range(len(records)):
                finished_tables.add(records[rg][0].replace("percent_", ""))
        except Exception as ex:
            log_file.write(f"Can't get list of tables: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get list of tables: {ex}. Exiting program...\n")
        quit()
    
    return finished_tables, all_tables - finished_tables
##################################################################################################################################
def parsingTableFullName(table_full_name):
    tmp = table_full_name.split("_")
    return tmp[0], tmp[1], float(tmp[2])/ 100
##################################################################################################################################
def getData(table_full_name):
    global TFIDF_cosine_distance

    connection = createConnection(data_base_MD)
    if connection != None:
        crsr = connection.cursor()
        try :
            query = "SELECT "
            for id in all_tech_ID:
                query = query + f"Tech_{id}, "
            query = query[:-2] + f" FROM {table_full_name}"

            crsr.execute(query)
            TFIDF_cosine_distance = crsr.fetchall()
        except Exception as ex:
            log_file.write(f"Can't get data: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get data: {ex}. Exiting program...\n")
        quit()

    #for idx in range(len(TFIDF_cosine_distance)):
       # for i in range(8):
          #  print(TFIDF_cosine_distance[idx][i])
        #print("---------------")
    #print("##########################")
##################################################################################################################################
def normalizedPercentDistance(table_name):
    global normalized_percent
    normalized_percent.clear()

    min_tmp = []
    max_tmp = []
    for dist in TFIDF_cosine_distance:
        min_tmp.append(min(dist))
        max_tmp.append(max(dist))
    min_dist = min(min_tmp)
    max_dist = max(max_tmp)

    if table_name == "distance":
        for rg1 in range(len(all_tech_ID)):
            tmp = []
            for rg2 in range(len(all_tech_ID)):
                nrml_prcnt = ( (TFIDF_cosine_distance[rg1][rg2] - min_dist) / (max_dist - min_dist) ) * 100
                tmp.append(100 - nrml_prcnt)
            normalized_percent.append(tmp)

    elif table_name == "cosine":
        for rg1 in range(len(all_tech_ID)):
            tmp = []
            for rg2 in range(len(all_tech_ID)):
                nrml_prcnt = ( (TFIDF_cosine_distance[rg1][rg2] - min_dist) / (max_dist - min_dist) ) * 100
                tmp.append(nrml_prcnt)
            normalized_percent.append(tmp)
##################################################################################################################################
def writeToDataBase(table_full_name):
    query = f"CREATE TABLE IF NOT EXISTS {table_full_name} ( Technology text NOT NULL "
    for id in all_tech_ID:
        query = query + f", Tech_{id} float NOT NULL "
    query = query + ");"

    connection = createConnection(data_base_NP)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(query)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create table in data base {table_full_name}: {ex}, Exiting from program ...\n")
            print(f"\nCan't create tables data base {table_full_name}: {ex}, Exiting from program ...")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection is failed. Can't create table in data base {table_full_name}. Exiting from program ...\n")
        print(f"\nConnection is failed. CCan't create table in data base {table_full_name}. Exiting from program ...")
        quit()

    connection = createConnection(data_base_NP)
    if connection != None:
        crsr = connection.cursor()
        try:
            sql_query = f"INSERT INTO {table_full_name} (Technology"
            str_tmp = ""
            str_tmp2 = "VALUES(?"
            for id in all_tech_ID:
                str_tmp = str_tmp + f", Tech_{id}"
                str_tmp2 = str_tmp2 + ",?"
            sql_query = sql_query + str_tmp + ")" + str_tmp2 + ")"
            
            for rg in range(len(all_tech_ID)):
                normalized_percent[rg].insert(0, f"Tech_{all_tech_ID[rg]}")
            
            crsr.execute("BEGIN TRANSACTION")
            crsr.executemany(sql_query, normalized_percent)
            crsr.execute("COMMIT")  
        except Exception as ex:
            log_file.write(f"Can't write to data base: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't write to data base. Exiting program...\n")
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
    log_file.write(f"Technology normalized percent.\n")

    getAllTechID()
    log_file.write(f"Tech IDs: {all_tech_ID}\n")
    finished_tables, unfinished_tables = getListOfTables()
    log_file.write(f"Finished tables: {finished_tables}\nUnfinished tables: {unfinished_tables}\n")
    log_file.write("########################################################################################\n")
    log_file.flush()

    for tblfllnm in tqdm(unfinished_tables):
        TF_start = time.time()

        tblnm, MSTF, MDF = parsingTableFullName(tblfllnm)
        log_file.write(f"Table name: {tblfllnm}. MSTF = {MSTF}, MDF = {MDF}\n")
        getData(tblfllnm)
        normalizedPercentDistance(tblnm)
        writeToDataBase(tblfllnm)  

        TF_end = time.time()   
        TF_elapsed_time = str(int((TF_end - TF_start) / 3600)) + "h " + str(int(((TF_end - TF_start) % 3600) / 60)) + "m"
        log_file.write(f"Time: {TF_elapsed_time}\n")           
        log_file.flush()
        log_file.write("---------------------------------------------------------------------------------------------\n")
##################################################################################################################################
if __name__ == '__main__':
    main()