from nltk.probability import FreqDist
from numpy.core.records import record
import pandas as pd
import sqlite3
import ast
import os
import time
from tqdm import tqdm
##################################################################################################################################
tech_terms_id_dict = {}
all_tech_terms_ID = set()
finished_tech_terms_ID = set()
data_base_gather = os.getcwd() + "\\data\\Robotics_tech_terms_gathered.db"
data_base_TF = os.getcwd() + "\\data\\Robotics_tech_terms_Frequency_Ntables.db"
tech_terms_list_file = os.getcwd() + r"\\Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + "\\logs\\Robotics_tech_terms_Frequency_Ntables.log"
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

    connection = createConnection(data_base_TF)
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
    connection = createConnection(data_base_TF)  
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
def writingTermFrequencyToDataBase(freq_dict, tech_term_id):
    connection = createConnection(data_base_TF)  
    if connection != None:
        crsr = connection.cursor()
        try:
            sql_create_freq_tech_table = f"""CREATE TABLE IF NOT EXISTS FreqTech_{tech_term_id} (
                                        Term text NOT NULL,
                                        Freq_{tech_term_id} integer
                                        ); """ 
            crsr.execute(sql_create_freq_tech_table)            
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create FreqTech table. tech_term_ID: {tech_term_id}. {ex}. Exiting from program ...\n")
            quit()
        try:
            crsr.execute("BEGIN TRANSACTION")
            crsr.executemany(f"INSERT INTO FreqTech_{tech_term_id} (Term, Freq_{tech_term_id}) VALUES(?, ?)", freq_dict.items())
            crsr.execute("COMMIT")
        except Exception as ex:
            log_file.write(f"Can't write term-frequency to data base. Tech_term_ID: {tech_term_id}, {ex}. Exiting program...\n")
            quit()
        connection.close()
    else:
        log_file.write(f"Connection is failed. Can't write term-frequency to data base. Tech_term_ID: {tech_term_id}, {ex}. Exiting program...\n")
        quit()
##################################################################################################################################
def frequency(tech_term_id):
    tech_term_book = []
    freq_dict = {} # {word : freq}
    connection = createConnection(data_base_gather)  
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"SELECT Title_Keywords, Abstract_Text FROM  Tech_Terms_Data WHERE Term_ID = {tech_term_id} ")
            records = crsr.fetchall()
            tech_term_book = records[0][0][2:-2].split("', '") + records[0][1][2:-2].split("', '")
            for wrd in tqdm(tech_term_book):
                try :
                    freq_dict[wrd] = freq_dict[wrd] + 1
                except:
                    freq_dict[wrd] = 1
                
            log_file.write(f"Number of tokens: {sum(freq_dict.values())}\n")
            writingTermFrequencyToDataBase(freq_dict, tech_term_id)
        except Exception as ex:
            print(f"Error in calculate term frequency: {ex}. Exiting from program ...\n")
            log_file.write(f"Error in calculate term frequency: {ex}. Exiting from program ...\n")
            quit()

        connection.close()
    else:
        print(f"Connection is failed. Error in calculate term frequency: {ex}. Exiting from program ...\n")
        log_file.write(f"Connection is failed. Error in calculate term frequency: {ex}. Exiting from program ...\n")
        quit()
    
    return sum(freq_dict.values())
##################################################################################################################################
def getUnFinishedTechTermIDs():
    global all_tech_terms_ID 
    global finished_tech_terms_ID

    connection = createConnection(data_base_gather)
    if connection != None:
        crsr = connection.cursor()

        try:
            crsr.execute(f"SELECT Term_ID FROM Tech_Terms_Data")
            records = crsr.fetchall()
            for rg in range(len(records)):
                all_tech_terms_ID.add(records[rg][0])
        except Exception as ex:
            log_file.write(f"Can't get all_term_ID set. {ex}. Exiting from program ...\n")
            quit()
        connection.close()

    else:
        log_file.write("Connection is failed. Can't get all_term_ID set. Exiting from program ...\n")
        quit()
    

    connection = createConnection(data_base_TF)
    if connection != None:
        crsr = connection.cursor()
        try:
            for id in all_tech_terms_ID:
                crsr.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='FreqTech_{id}'")
                if crsr.fetchall()[0][0] == 1:
                    finished_tech_terms_ID.add(id)

        except Exception as ex:
            log_file.write(f"Can't get finished_term_ID set. {ex}. Exiting from program ...\n")
            quit()

        connection.close()
    else:
        log_file.write("Connection is failed. Can't get finished_term_ID set. Exiting from program ...\n")
        quit()

    return all_tech_terms_ID - finished_tech_terms_ID, all_tech_terms_ID
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
    unfinished_tech_terms_ID, all_tech_terms_ID = getUnFinishedTechTermIDs()

    # Write some info to Log file
    log_file.write("########################################################################################\n")
    log_file.write(f"Term Frequency(TF).\n\
                    \nUnfinished Terms ID ({len(unfinished_tech_terms_ID)}): {unfinished_tech_terms_ID}\
                    \nFinished Terms ID ({len(all_tech_terms_ID - unfinished_tech_terms_ID)}): {all_tech_terms_ID - unfinished_tech_terms_ID}\n")
    log_file.write("########################################################################################\n")

    all_tokens_number = 0

    # Main loop
    for tech_term_id in tqdm(unfinished_tech_terms_ID):
        log_file.write(f"Tech Term: {[k for k, v in tech_terms_id_dict.items() if v == tech_term_id]}, Tech Term ID: {tech_term_id}\n")

        TF_start = time.time()
        all_tokens_number = all_tokens_number + frequency(tech_term_id)              
        TF_end = time.time()
        
        TF_elapsed_time = str(int((TF_end - TF_start) / 3600)) + "h " + str(int(((TF_end - TF_start) % 3600) / 60)) + "m"
        log_file.write(f"Time: {TF_elapsed_time}\n")        
        log_file.write("----------------------------------------------------------------------\n")
        log_file.flush()
    
    log_file.write(f"All Tokens Number(if program not interrupted): {all_tokens_number}")
    
##################################################################################################################################
if __name__ == '__main__':
    main()