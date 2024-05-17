import sqlite3
import pandas as pd
import os
import time
import math
from tqdm import tqdm
##################################################################################################################################
MSTF = 1000 # Terms with less than MSTF (Min of Sum of Term Frequency) will be deleted. 0 means no term will be deleted(MSTF >=0)
MDF = 0.05 # In calculating of DF terms with less than MDF(Min Document Frequency) of term-freq will be ignored (0<= MDF <= 1)
tech_terms_id_dict = {}
terms_freqs_list = [] # [(term, freq0, freq1, ...), (term, freq0, ...), ...]
sum_freqs_list = [] # [sum(freq0), sum(freq1), ...]
all_tech_ID = []
TFIDF_list = [] # [[term ,tfidf0, tfidf1, ...], [term, freq0, ...], ...]
data_base_TF_total =  os.getcwd() + f"\\data\\Robotics_tech_terms_TF_Nfifty_tables_and_two_200Col_tables.db"
data_base_TFIDF = os.getcwd() + f"\\data\\Robotics_tech_terms_TTFIDF.db"
tech_terms_list_file = os.getcwd() + r"\\Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + "\\logs\\Robotics_tech_terms_TFIDF.log"
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

    connection = createConnection(data_base_TFIDF)
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
    connection = createConnection(data_base_TFIDF)  
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
    for id in range(200):
        all_tech_ID.append(id)

    all_tech_ID.remove(48)
    all_tech_ID.remove(82)
##################################################################################################################################
def buildQuery():
    filter_rows_query = "SELECT * "
    sum_columns_query = "SELECT "

    filter_rows_query = filter_rows_query + f"\nFROM Terms_Frequency_200col_1\nWHERE " 

    tmp = ""
    for id in all_tech_ID:
        tmp = tmp + f"sum(FreqTech_{id}), "

    sum_columns_query = sum_columns_query + tmp[:-2] + f"\nFROM (SELECT * FROM Terms_Frequency_200col_1\nWHERE " 

    tmp = ""
    for id in all_tech_ID:
        tmp = tmp + f"COALESCE(FreqTech_{id}, 0) + "

    filter_rows_query = filter_rows_query + tmp[:-3] + f" > {MSTF}"
    sum_columns_query = sum_columns_query + tmp[:-3] + f" > {MSTF})"

    #print(sum_columns_query)
    return filter_rows_query, sum_columns_query
##################################################################################################################################
def getData():
    filter_rows_query, sum_columns_query = buildQuery()
    #print("QUERY: ", sum_columns_query)
    global terms_freqs_list
    global sum_freqs_list

    connection = createConnection(data_base_TF_total)
    if connection != None:
        crsr = connection.cursor()
        try :
            crsr.execute(filter_rows_query)
            terms_freqs_list = crsr.fetchall()

            crsr.execute(sum_columns_query)
            sum_freqs_list = crsr.fetchall()[0]

        except Exception as ex:
            log_file.write(f"Can't get result of Queries: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't get result of Queries: {ex}. Exiting program...\n")
        quit()
##################################################################################################################################
def writingTFIDFToDataBase():
    mdf = int(MDF * 100)
    TFIDF_table_create_query = f"""CREATE TABLE IF NOT EXISTS TFIDF_{MSTF}_{mdf} (
                                        Term text NOT NULL """
    tmp = ""
    for id in all_tech_ID:
        tmp = tmp + f", TFIDF_{id} integer"
    
    TFIDF_table_create_query = TFIDF_table_create_query + tmp + ");"

    connection = createConnection(data_base_TFIDF)
    if connection != None:
        crsr = connection.cursor()
        try :
            crsr.execute(TFIDF_table_create_query)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create table: {ex}. Exiting program...\n")
            quit()
        
        try:
            sql_query = f"""INSERT INTO TFIDF_{MSTF}_{mdf} (Term"""
            str_tmp = ""
            str_tmp2 = "VALUES(?"
            for id in all_tech_ID:
                str_tmp = str_tmp + ", TFIDF_" + str(id)
                str_tmp2 = str_tmp2 + ",?"
            sql_query = sql_query + str_tmp + ")" + str_tmp2 + ")"
            
            crsr.execute("BEGIN TRANSACTION")
            crsr.executemany(sql_query, TFIDF_list)
            crsr.execute("COMMIT")           
        except Exception as ex:
            log_file.write(f"Can't write to data base: {ex}. Exiting program...\n")
            quit()
        crsr.close()
        connection.close()
    else:
        log_file.write(f"Connection failed. Can't write to data base: {ex}. Exiting program...\n")
        quit()

##################################################################################################################################
def DF(row, column_number):
    document_freq = 0
    for col in range(1, len(all_tech_ID) + 1):
        if col != column_number and row[col] != None:
                document_freq = document_freq + (row[col] > MDF * row[column_number])
    
    return document_freq
##################################################################################################################################
def TFIDF():
    tech_count = len(all_tech_ID)
    for row in tqdm(terms_freqs_list):
        tmp = []
        tmp.append(row[0])
        for col in range(1, tech_count + 1):
            if row[col] != None:
                tmp.append((row[col] / sum_freqs_list[col - 1]) *  math.log10(tech_count / (DF(row, col) + 1)))
            else: # row[col] = None
                tmp.append(0)

        TFIDF_list.append(tmp)
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
    log_file.write(f"Term Frequency Inverse Document Frequency(TF-IDF).\nMSTF = {MSTF}, MDF = {MDF}\n")

    TF_start = time.time()
    getAllTechID()
    print("\nGetting Data ...\n")
    getData()
    log_file.write(f"Tech IDs: {all_tech_ID}\n")
    log_file.write(f"Term counts after filtering: {len(terms_freqs_list)}\nSum of Freq columns: {sum_freqs_list}\n")
    log_file.flush()
    print("Getting Data is done.\nTFIDF...")
    TFIDF()
    print("TFIDF is done.\nWriting to data base...")
    writingTFIDFToDataBase()
    print("writing is done. Look at log file.")
    TF_end = time.time()
    
    TF_elapsed_time = str(int((TF_end - TF_start) / 3600)) + "h " + str(int(((TF_end - TF_start) % 3600) / 60)) + "m"
    log_file.write(f"Time: {TF_elapsed_time}\n")        
    log_file.write("########################################################################################\n")
    log_file.flush()

    #print(sum_freqs_list)
    #print("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    #for rg in range(10):
        #print(terms_freqs_list[rg])
##################################################################################################################################
if __name__ == '__main__':
    main()