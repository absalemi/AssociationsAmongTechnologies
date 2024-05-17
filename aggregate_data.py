import pandas as pd
import sqlite3
import ast
import os
import time
from tqdm import tqdm
##################################################################################################################################
n_fifty_terms = 1 # the n'th fifty terms
tech_terms_id_dict = {}
data_base_cleaned = os.getcwd() + f"\\data\\Robotics_tech_terms_cleaned_{n_fifty_terms}.db"
data_base_aggregate = os.getcwd() + f"\\data\\Robotics_tech_terms_aggregate_{n_fifty_terms}.db"
tech_terms_list_file = os.getcwd() + r"\\Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + f"\\logs\\Robotics_tech_terms_aggregate_{n_fifty_terms}.log"
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
    sql_create_tech_terms_ID_table = """ CREATE TABLE IF NOT EXISTS Tech_Terms_ID (
                                        ID integer PRIMARY KEY,
                                        Term text NOT NULL
                                    ); """ 

    sql_create_tech_terms_data_table ="""CREATE TABLE IF NOT EXISTS Tech_Terms_Data(
                                        Term_id integer PRIMARY KEY,
                                        Title_Keywords text,
                                        Abstract_Text text
                                    );"""
    connection = createConnection(data_base_aggregate)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(sql_create_tech_terms_ID_table)
            crsr.execute(sql_create_tech_terms_data_table)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create tables in aggregate data base: {ex}, Exiting from program ...\n")
            print(f"\nCan't create tables in aggregate data base{ex}, Exiting from program ...")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't create tables in aggregate data base. Exiting from program...\n")
        print("\nConnection is failed. Can't create tables in data base. Exiting from program...")
        quit()
##################################################################################################################################
def writeTermsInDataBase(tech_terms):
    #writing terms-id in Tech_Terms_ID table
    connection = createConnection(data_base_aggregate)  
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
def writeAggregationArticlesToDataBase(term_ID, ti_key, ab_tex):
    connection = createConnection(data_base_aggregate)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"""INSERT INTO Tech_Terms_Data VALUES({term_ID}, "{ti_key}", "{ab_tex}") """)
            connection.commit()
            log_file.write(f"Writing Data for term_ID = {term_ID} is successfully finished.\n")
        except Exception as ex:
            log_file.write(f"Can't write data to TF DB for term_ID = {term_ID}, {ex}\n")
        connection.close()
    else:
        log_file.write("Connection is failed. Can't write data to TF DB for term_ID.\n")
##################################################################################################################################
def aggregateArticlesWithSameTermID(term_ID):
    ti_key = []
    ab_tex = []
    title_empty_number = 0
    keyword_empty_number = 0
    abstract_empty_number = 0
    text_empty_number = 0

    connection = createConnection(data_base_cleaned)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"SELECT Title, Keywords, Abstract, Text FROM Tech_Terms_Data WHERE Term_ID = {term_ID}")
            records = crsr.fetchall()
            for rg in tqdm(range(len(records))):
                try:
                    ti_key = ti_key + ast.literal_eval(records[rg][0])
                except Exception as ex:
                    title_empty_number = title_empty_number + 1
                
                try:
                    ti_key = ti_key + ast.literal_eval(records[rg][1])
                except Exception as ex:
                    keyword_empty_number = keyword_empty_number + 1
                
                try:
                    ab_tex = ab_tex + ast.literal_eval(records[rg][2])
                except Exception as ex:
                    abstract_empty_number = abstract_empty_number + 1
                
                try:
                    ab_tex = ab_tex + ast.literal_eval(records[rg][3])
                except Exception as ex:
                    text_empty_number = text_empty_number + 1

            writeAggregationArticlesToDataBase(term_ID, ti_key, ab_tex)
        except Exception as ex:
            log_file.write(f"Can't aggregate data for term_ID = {term_ID}, {ex}\n")

        connection.close()
        log_file.write(f"Number of Articles: {len(records)}\
            \nNumber of emty Titles: {title_empty_number}\
            \nNumber of empty Keywords: {keyword_empty_number}\
            \nNumber of empty Abstract: {abstract_empty_number}\
            \nNumber of empty Text: {text_empty_number}\n")
    else:
        log_file.write(f"Connection to TF data-base failed. Can't aggregate data for term_ID = {term_ID}, {ex}\n")
##################################################################################################################################
def getUnFinishedTermIDs():
    all_terms_ID = set()
    finished_terms_ID = set()

    connection = createConnection(data_base_cleaned)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"SELECT Term_ID FROM Tech_Terms_Data")
            records = crsr.fetchall()
            for rg in range(len(records)):
                all_terms_ID.add(records[rg][0])
        except Exception as ex:
            log_file.write("Can't get all_term_ID set. Exiting from program ...\n")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't get all_term_ID set. Exiting from program ...\n")
        quit()
    

    connection = createConnection(data_base_aggregate)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"SELECT Term_ID FROM Tech_Terms_Data")
            records = crsr.fetchall()
            for rg in range(len(records)):
                finished_terms_ID.add(records[rg][0])
        except Exception as ex:
            log_file.write("Can't get finished_term_ID set. Exiting from program ...\n")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't get finished_term_ID set. Exiting from program ...\n")
        quit()

    return all_terms_ID - finished_terms_ID, all_terms_ID
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

    # Get set of unfinished terms-id
    unfinished_terms_ID, all_terms_ID = getUnFinishedTermIDs()

    # Write some info to Log file
    log_file.write("########################################################################################\n")
    log_file.write(f"The n'th fifty terms: {n_fifty_terms} [{(n_fifty_terms - 1) * 50} : {n_fifty_terms * 50}]\
                    \nUnfinished Terms ID ({len(unfinished_terms_ID)}): {unfinished_terms_ID}\
                    \nFinished Terms ID ({len(all_terms_ID - unfinished_terms_ID)}): {all_terms_ID - unfinished_terms_ID}\n")
    log_file.write("########################################################################################\n")

    # Main loop
    for term_ID in tqdm(unfinished_terms_ID):
        log_file.write(f"Tech Term: {[k for k, v in tech_terms_id_dict.items() if v == term_ID]}, Tech Term ID: {term_ID}\n")
        
        aggregate_start = time.time()
        aggregateArticlesWithSameTermID(term_ID)
        aggregate_end = time.time()
        
        aggreagate_elapsed_time = str(int((aggregate_end - aggregate_start) / 3600)) + "h " + str(int(((aggregate_end - aggregate_start) % 3600) / 60)) + "m"
        log_file.write(f"Time: {aggreagate_elapsed_time}\n")        
        log_file.write("----------------------------------------------------------------------\n")
        log_file.flush()

    
##################################################################################################################################
if __name__ == '__main__':
    main()