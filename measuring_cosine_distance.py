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
TFIDF_vectors_list = [] #[(--tfidf0--), (--tfidf1--), ..]
TFIDF_cosine_distance = [] #[ [--dist0--], [--dist1--],... ] dist0 is a list. dist of tech0 from other techs
data_base_TFIDF = os.getcwd() + f"/data/Robotics_tech_terms_TFIDF.db"
data_base_MD =  os.getcwd() + f"/data/Robotics_tech_terms_measuring_cosine_distance.db"
tech_terms_list_file = os.getcwd() + f"/Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + "/logs/Robotics_tech_terms_measuring_cosine_distance.log"
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

    connection = createConnection(data_base_MD)
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
    connection = createConnection(data_base_MD)  
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
def getListOfTables(meter):
    all_table_index = set()
    finished_table_index = set()
    tblnm = ""
    if meter == 0:
        tblnm = "distance"
    elif meter == 1:
        tblnm = "cosine"

    connection = createConnection(data_base_TFIDF)
    if connection != None:
        crsr = connection.cursor()
        try :
            crsr.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'TFIDF_%'")
            records = crsr.fetchall()
            for rg in range(len(records)):
                all_table_index.add(records[rg][0].replace("TFIDF_", ""))
        except Exception as ex:
            log_file.write(f"Can't get list of tables: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get list of tables: {ex}. Exiting program...\n")
        quit()

    connection = createConnection(data_base_MD)
    if connection != None:
        crsr = connection.cursor()
        try :
            crsr.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '{tblnm}_%'")
            records = crsr.fetchall()
            for rg in range(len(records)):
                finished_table_index.add(records[rg][0].replace(f"{tblnm}_", ""))
        except Exception as ex:
            log_file.write(f"Can't get list of tables: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get list of tables: {ex}. Exiting program...\n")
        quit()
    
    return finished_table_index, all_table_index - finished_table_index
##################################################################################################################################
def getMSTFMDF(table_index):
    tmp = table_index.split("_")
    return tmp[0], float(tmp[1])/ 100
##################################################################################################################################
def getData(table_index):
    global TFIDF_vectors_list
    connection = createConnection(data_base_TFIDF)
    if connection != None:
        crsr = connection.cursor()
        try :
            query = "SELECT "
            for id in all_tech_ID:
                query = query + f"TFIDF_{id}, "
            query = query[:-2] + f" FROM TFIDF_{table_index}"

            crsr.execute(query)
            records = crsr.fetchall()
            TFIDF_vectors_list = list(zip(*records))
        except Exception as ex:
            log_file.write(f"Can't get data: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get data: {ex}. Exiting program...\n")
        quit()

    #for idx in range(len(TFIDF_vectors_list)):
       # for i in range(8):
          #  print(TFIDF_vectors_list[idx][i])
        #print("---------------")
    #print("##########################")
##################################################################################################################################
def measuringCosineDistance(meter):
    global TFIDF_vectors_list, TFIDF_cosine_distance
    TFIDF_cosine_distance.clear()
    TFIDF_vectors = np.asarray(TFIDF_vectors_list)

    if meter == 0:
        for vec1 in TFIDF_vectors:
            tmp = []
            for vec2 in TFIDF_vectors:
                tmp.append(np.linalg.norm(vec1 - vec2))
            TFIDF_cosine_distance.append(tmp)
    elif meter == 1:
        for vec1 in TFIDF_vectors:
            tmp = []
            for vec2 in TFIDF_vectors:
                tmp.append(np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2)))
            TFIDF_cosine_distance.append(tmp)
##################################################################################################################################
def writeToDataBase(mstf, mdf, meter):
    mdf = int(mdf * 100)
    tblnm = ""
    if meter == 0:
        tblnm = "distance"
    elif meter == 1:
        tblnm = "cosine"

    query = f"CREATE TABLE IF NOT EXISTS {tblnm}_{mstf}_{mdf} ( Technology text NOT NULL "
    for id in all_tech_ID:
        query = query + f", Tech_{id} float NOT NULL "
    query = query + ");"

    connection = createConnection(data_base_MD)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(query)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create table in data base {mstf}, {mdf}: {ex}, Exiting from program ...\n")
            print(f"\nCan't create tables data base {mstf}, {mdf}: {ex}, Exiting from program ...")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection is failed. Can't create table in data base {mstf}, {mdf}. Exiting from program ...\n")
        print(f"\nConnection is failed. CCan't create table in data base {mstf}, {mdf}. Exiting from program ...")
        quit()

    connection = createConnection(data_base_MD)
    if connection != None:
        crsr = connection.cursor()
        try:
            sql_query = f"INSERT INTO {tblnm}_{mstf}_{mdf} (Technology"
            str_tmp = ""
            str_tmp2 = "VALUES(?"
            for id in all_tech_ID:
                str_tmp = str_tmp + f", Tech_{id}"
                str_tmp2 = str_tmp2 + ",?"
            sql_query = sql_query + str_tmp + ")" + str_tmp2 + ")"
            
            for rg in range(len(all_tech_ID)):
                TFIDF_cosine_distance[rg].insert(0, f"Tech_{all_tech_ID[rg]}")
            
            crsr.execute("BEGIN TRANSACTION")
            crsr.executemany(sql_query, TFIDF_cosine_distance)
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

    while True:
        meter = int(input("0 - Euclidean distance\n1 - Cosine similarity\nEnter number(0 or 1):"))
        if meter == 0 or meter == 1:
            break
    
    meter_str = ""
    if meter == 0:
        meter_str = "Euclidean distance"
    elif meter == 1:
        meter_str = "Cosine similarity"

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
    log_file.write(f"Technology measuring distance. Metric: {meter_str}\n")

    getAllTechID()
    log_file.write(f"Tech IDs: {all_tech_ID}\n")
    finished_tables, unfinished_tables = getListOfTables(meter)
    log_file.write(f"Finished tables: {finished_tables}\nUnfinished tables: {unfinished_tables}\n")
    log_file.write("########################################################################################\n")
    log_file.flush()

    for tblidx in tqdm(unfinished_tables):
        TF_start = time.time()

        MSTF, MDF = getMSTFMDF(tblidx)
        log_file.write(f"Table name: {tblidx}. MSTF = {MSTF}, MDF = {MDF}\n")
        getData(tblidx)
        measuringCosineDistance(meter)
        writeToDataBase(MSTF, MDF, meter)  

        TF_end = time.time()   
        TF_elapsed_time = str(int((TF_end - TF_start) / 3600)) + "h " + str(int(((TF_end - TF_start) % 3600) / 60)) + "m"
        log_file.write(f"Time: {TF_elapsed_time}\n")           
        log_file.flush()
        log_file.write("---------------------------------------------------------------------------------------------\n")
##################################################################################################################################
if __name__ == '__main__':
    main()