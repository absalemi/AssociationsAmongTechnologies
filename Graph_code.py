import sqlite3
import pandas as pd
import os
import time
import math
import numpy as np
from tqdm import tqdm
##################################################################################################################################
all_tech_ID = []
normalized_percent = []
data_base_src = os.getcwd() + f"/data/Robotics_tech_terms_normalized_percent.db"
graph_file_address = os.getcwd() + f"/data/graph_code.txt"
tech_terms_log_file = os.getcwd() + "/logs/Robotics_tech_terms_graph_code.log"
log_file = open(tech_terms_log_file, 'a', encoding = 'utf-8')
graph_file = open(graph_file_address, 'w', encoding='utf-8')
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
def getAllTechID():
    global all_tech_ID
    for rg in range(10):
        all_tech_ID.append(rg)

    #all_tech_ID.remove(48)
    #all_tech_ID.remove(82)
##################################################################################################################################
def getListOfTables():
    all_tables = set()

    connection = createConnection(data_base_src)
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
    
    return all_tables 
##################################################################################################################################
def parsingTableFullName(table_full_name):
    tmp = table_full_name.split("_")
    return tmp[1], float(tmp[2])/ 100
##################################################################################################################################
def getData(table_full_name):
    global normalized_percent

    connection = createConnection(data_base_src)
    if connection != None:
        crsr = connection.cursor()
        try :
            query = "SELECT "
            for id in all_tech_ID:
                query = query + f"Tech_{id}, "
            query = query[:-2] + f" FROM {table_full_name}"

            crsr.execute(query)
            normalized_percent = crsr.fetchall()
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
def writeToGraphFile(tblfllnm):
    graph_code = f"\ngraph {tblfllnm} " + "{\nnode [style=filled];\n"
    for rg in range(len(all_tech_ID)):
        for rg2 in range(rg):
            graph_code = graph_code + f"T{rg} -- T{rg2} [label = {round(normalized_percent[rg][rg2], 2)}];\n"
    graph_code = graph_code + "}"
    
    graph_file.write(graph_code)
    graph_file.write("\n-----------------------------------------------------------------------------------\n")
##################################################################################################################################
def main():
    print("----------------------------------------------------------------------------------------------")
    
    # Write some info to Log file
    log_file.write("########################################################################################\n")
    log_file.write(f"Graph code for Graphviz.\n")
    graph_file.write("########################################################################################\n")
    graph_file.write(f"Graph code for Graphviz.\n")

    getAllTechID()
    log_file.write(f"Tech IDs: {all_tech_ID}\n")
    graph_file.write(f"Tech IDs: {all_tech_ID}\n")
    all_tables = getListOfTables()
    log_file.write(f"All tables: {all_tables}\n")
    log_file.write("########################################################################################\n")
    graph_file.write(f"All tables: {all_tables}\n")
    graph_file.write("########################################################################################\n")
    log_file.flush()
    graph_file.flush()

    for tblfllnm in tqdm(all_tables):
        TF_start = time.time()

        MSTF, MDF = parsingTableFullName(tblfllnm)
        log_file.write(f"Table name: {tblfllnm}. MSTF = {MSTF}, MDF = {MDF}\n")
        graph_file.write(f"Table name: {tblfllnm}. MSTF = {MSTF}, MDF = {MDF}\n")
        getData(tblfllnm)
        writeToGraphFile(tblfllnm)  

        TF_end = time.time()   
        TF_elapsed_time = str(int((TF_end - TF_start) / 3600)) + "h " + str(int(((TF_end - TF_start) % 3600) / 60)) + "m"
        log_file.write(f"Time: {TF_elapsed_time}\n")           
        log_file.flush()
        log_file.write("---------------------------------------------------------------------------------------------\n")
##################################################################################################################################
if __name__ == '__main__':
    main()