import pandas as pd
import sqlite3
import ast
import os
import time
from tqdm import tqdm
##################################################################################################################################
aggregated_file_number = 3
tech_terms_id_dict_csv = {}
data_base_gathere = os.getcwd() + f"\\data\\Robotics_tech_terms_gathered.db"
tech_terms_list_file = os.getcwd() + r"\\Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + f"\\logs\\Robotics_tech_terms_gathered.log"
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
def getUnFinishedTermIDs(n_fifty_terms):
    all_terms_ID_in_Nfifty_term = set()
    all_finished_terms_ID_in_All_fifty_terms = set()

    data_base_aggregate = os.getcwd() + f"\\data\\Robotics_tech_terms_aggregate_{n_fifty_terms}.db"
    connection = createConnection(data_base_aggregate)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"SELECT Term_ID FROM Tech_Terms_Data")
            records = crsr.fetchall()
            for rg in range(len(records)):
                all_terms_ID_in_Nfifty_term.add(records[rg][0])
        except Exception as ex:
            log_file.write("Can't get all_term_ID set. Exiting from program ...\n")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't get all_term_ID set. Exiting from program ...\n")
        quit()
    

    connection = createConnection(data_base_gathere)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"SELECT Term_ID FROM Tech_Terms_Data")
            records = crsr.fetchall()
            for rg in range(len(records)):
                all_finished_terms_ID_in_All_fifty_terms.add(records[rg][0])
        except Exception as ex:
            log_file.write("Can't get finished_term_ID set. Exiting from program ...\n")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't get finished_term_ID set. Exiting from program ...\n")
        quit()

    return all_finished_terms_ID_in_All_fifty_terms, all_terms_ID_in_Nfifty_term
##################################################################################################################################
def gathering(n_fifty_terms):

    # Get set of unfinished terms-id for n'th file
    all_finfished_terms_ID_untill_now, all_terms_ID_in_Nfifty_term = getUnFinishedTermIDs(n_fifty_terms)
    unfinished_terms_ID_in_Nfifty_term = all_terms_ID_in_Nfifty_term - all_finfished_terms_ID_untill_now

    log_file.write(f"Unfinished Terms ID In This File ({len(unfinished_terms_ID_in_Nfifty_term)}): {unfinished_terms_ID_in_Nfifty_term}\
                \nAll Finished Terms ID Until Now ({len(all_finfished_terms_ID_untill_now)}): {all_finfished_terms_ID_untill_now}\
                \n----------------------------------------------------------------------\n")
      
    connection_gathere = createConnection(data_base_gathere)
    if connection_gathere != None:
        crsr_gather = connection_gathere.cursor()
        data_base_aggregate = os.getcwd() + f"\\data\\Robotics_tech_terms_aggregate_{n_fifty_terms}.db"
        connection_aggregate = createConnection(data_base_aggregate)
        if connection_aggregate != None:
            crsr_aggregate = connection_aggregate.cursor()
            for term_ID in tqdm(unfinished_terms_ID_in_Nfifty_term):
                log_file.write(f"Tech Term: {[k for k, v in tech_terms_id_dict_csv.items() if v == term_ID]}, Tech Term ID: {term_ID}\n")
                try:
                    crsr_aggregate.execute(f"SELECT * FROM Tech_Terms_Data WHERE Term_ID = {term_ID}")
                    record = crsr_aggregate.fetchall()
                    ti_key = record[0][1]
                    ab_tex = record[0][2]
                    crsr_gather.execute(f"""INSERT INTO Tech_Terms_Data VALUES({term_ID}, "{ti_key}", "{ab_tex}") """)
                    connection_gathere.commit()
                    log_file.write(f"Writing Data for term_ID = {term_ID} is successfully finished.\n")
                    log_file.write("----------------------------------------------------------------------\n")
                    log_file.flush()
                except Exception as ex:
                    print(f"Can't read(write) from(to) aggregate_DB(gather_DB) for term ID = {term_ID}. {ex}\n")
                    log_file.write(f"Can't read(write) from(to) aggregate_DB(gather_DB) for term ID = {term_ID}. {ex}\n")
            connection_aggregate.close()

        else:
            print(f"Connection_aggregate failed. Can't Read data from data base{n_fifty_terms}. Exit program ...\n")
            log_file.write(f"Connection_aggregate failed. Can't Read data from data base{n_fifty_terms}. Exit program ...\n")
            quit()
        connection_gathere.close()

    else:
        print("Connection_gathere failed. Exiting program ... \n")
        log_file.write("Connection_gathere failed. Exiting program ... \n")
        quit()

##################################################################################################################################
def main():
    
    print("----------------------------------------------------------------------------------------------")

    # Read terms from file
    tech_terms = pd.read_csv(tech_terms_list_file)
    tech_terms_list = [x for x in tech_terms['Tech_Terms']]
    #print(tech_terms)

    # Assign id to every term
    global tech_terms_id_dict_csv
    for i in range(len(tech_terms_list)):
        tech_terms_id_dict_csv[tech_terms_list[i]] = i

    # Write some info to Log file
    log_file.write("########################################################################################\n")
    log_file.write("Gathering.\nThe first fifty_terms(n'th fifty terms = 1) file is done manually)\n") 
    log_file.write("########################################################################################\n")

    # Main loop
    for n_fifty_terms in tqdm(range(2, aggregated_file_number + 1)):
        
        log_file.write(f"n'th fifty term(n'th file) = {n_fifty_terms}\n")
       
        gathering_start = time.time()
        gathering(n_fifty_terms)
        gathering_end = time.time()
            
        gathering_elapsed_time = str(int((gathering_end - gathering_start) / 3600)) + "h " + str(int(((gathering_end - gathering_start) % 3600) / 60)) + "m"
        log_file.write(f"Time: {gathering_elapsed_time}\n")        
        log_file.write("================================================================================\n")
        log_file.flush()
##################################################################################################################################
if __name__ == '__main__':
    main()