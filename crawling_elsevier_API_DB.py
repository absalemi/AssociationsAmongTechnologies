import pandas as pd
import sqlite3
import json
import os
import csv
import time

from elsapy.elsclient import ElsClient
from elsapy.elsprofile import ElsAuthor, ElsAffil
from elsapy.elsdoc import FullDoc, AbsDoc
from elsapy.elssearch import ElsSearch
from tqdm import tqdm
####################################################################################
get_all_term_search_results = False
n_fifty_terms = 1 # the n'th fifty terms
s_a_p_w = 5 #size of articles pack for writing
tech_terms_id_dict = {}
data_base = os.getcwd() + f"\\data\\Robotics_tech_terms_{n_fifty_terms}.db"
tech_terms_list_file = os.getcwd() + r"\\Robotics_tech_terms.csv"
tech_terms_log_file = os.getcwd() + f"\\logs\\Robotics_tech_terms_{n_fifty_terms}.log"
log_file = open(tech_terms_log_file, 'w', encoding = 'utf-8')
####################################################################################
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
                                        ID integer PRIMARY KEY,
                                        Term_id integer,
                                        Title text,
                                        Abstract text,
                                        Keywords text,
                                        Text text
                                    );"""
    sql_initialize_tech_terms_data_table = """ INSERT INTO Tech_Terms_Data VALUES(0, 0, 'Y', 'A', 'A', 'S')"""
    connection = createConnection(data_base)
    if connection != None:
        crsr = connection.cursor()
        crsr.execute(sql_create_tech_terms_ID_table)
        crsr.execute(sql_create_tech_terms_data_table)
        try:
            crsr.execute(sql_initialize_tech_terms_data_table)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Initializing  tech terms data table failed.(probably be Initialized before): {ex}\n")
            print(f"\nInitializing  tech terms data table failed.(probably be Initialized before): {ex}")
        connection.close()
    else:
        log_file.write("Connection is failed. Can't create tables in data base. Exiting from program...\n")
        print("\nConnection is failed. Can't create tables in data base. Exiting from program...")
        quit()
##################################################################################################################################
def writeTermsInDataBase(tech_terms):
    #writing terms-id in Tech_Terms_ID table
    connection = createConnection(data_base)  
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
def writeArticlesInDataBase(articles_title, articles_abstract, articles_keywords, articles_text, term, pack_index):
    #writing terms_id, title, abstract, keywords and in Tech_Terms_Data table
    connection = createConnection(data_base)
    if connection != None:
        crsr = connection.cursor()
        try:    
            crsr.execute("SELECT max(ID) FROM Tech_Terms_Data")
            step = crsr.fetchall()[0][0] + 1
            for i in range(len(articles_title)):
                if articles_title[i] is None:
                    articles_title[i] = " "
                if articles_abstract[i] is None:
                    articles_abstract[i] = " "
                if articles_keywords[i] is None:
                    articles_keywords[i] = " "
                if articles_text[i] is None:
                    articles_text[i] = " "
                articles_title[i] = articles_title[i].replace('"', '')
                articles_abstract[i] = articles_abstract[i].replace('"', '')
                articles_keywords[i] = articles_keywords[i].replace('"', '')
                articles_text[i] = articles_text[i].replace('"', '')
                crsr.execute(f"""INSERT INTO Tech_Terms_Data VALUES({i + step}, "{tech_terms_id_dict[term]}",
                            "{articles_title[i]}", "{articles_abstract[i]}", "{articles_keywords[i]}", "{articles_text[i]}" )""")
            connection.commit()
        except Exception as ex:
            log_file.write(f"[{term}] Can't write articles info (pack[{pack_index}]) in tables: {ex}\n")   
            print(f"\nCan't write articles info in tables: {ex}")
        
        connection.close()
    else:
        log_file.write(f"[{term}] Connection is failed. Can't write articles (pack[{pack_index}]) in tables.\n")
        print(f"\nConnection is failed. Can't write articles info in tables:{ex}")   
##################################################################################################################################
def getData(client, term):
    ## Initialize doc search object using ScienceDirect and execute search,  retrieving all results
    doc_srch = ElsSearch(term,'sciencedirect')
    #doc_srch = ElsSearch(term,'scopus')
    doc_srch.execute(client, get_all = get_all_term_search_results)
    print ("Results: ", len(doc_srch.results) )
    log_file.write(f"[{term}] Results: {len(doc_srch.results)}\n")
 
    for pack_index in tqdm(range(0, len(doc_srch.results), s_a_p_w)):
        articles_list_dict = doc_srch.results[pack_index : pack_index + s_a_p_w]        
        articles_title = []
        articles_abstract = []
        articles_keywords = []
        articles_text = []

        try:
            for article_dict in articles_list_dict:
                ## ScienceDirect (full-text) document example using DOI
                try:
                    doi_doc = FullDoc(doi = article_dict['prism:doi'])
                except Exception as ex:
                    log_file.write(f"[{term}] Full Doc Exception: {ex}\n")
                    print(f"\n[{term}] Full Doc Exception: {ex}")
                    time.sleep(5)
                    continue

                if doi_doc.read(client):
                    #print ("\ndoi_doc.title: ", doi_doc.title)   
                    #doi_doc.write()        
                    try:         
                        coredata_dict = doi_doc.data["coredata"]
                    except Exception as ex:
                        log_file.write(f"[{term}] Coredata Exception: {ex}, DOI: {article_dict['prism:doi']}\n")
                        print(f"\n[{term}] Coredata Exception: {ex}, DOI: {article_dict['prism:doi']}")
                        continue

                    try:
                        articles_title.append(coredata_dict["dc:title"])
                    except Exception as ex:
                        log_file.write(f"[{term}] Title Exception: {ex}, DOI: {article_dict['prism:doi']}\n")
                        continue

                    try:
                        articles_abstract.append(coredata_dict["dc:description"])
                    except Exception as ex:                       
                        articles_abstract.append(" ")
                        log_file.write(f"[{term}] Abstract Exception: {ex}, DOI: {article_dict['prism:doi']}\n")
                        #print("\ntitle or abstract Exception: ", ex)

                    try:
                        keyword_dict = coredata_dict['dcterms:subject']
                        keywords_str = ""
                        for key in keyword_dict:
                            keywords_str = keywords_str + key['$'] + ", "
                        articles_keywords.append(keywords_str)
                    except Exception as ex:
                        articles_keywords.append(" ")
                        log_file.write(f"[{term}] Keywords Exception: {ex}, DOI: {article_dict['prism:doi']}\n")
                        #print("\nkeywords Exception: ", ex)

                    try:
                        if type(doi_doc.data["originalText"]) == str:
                            if doi_doc.data['originalText'].find("Introduction") != -1:
                                articles_text.append(doi_doc.data['originalText'][doi_doc.data['originalText'].find("Introduction") : ])
                            else:
                                articles_text.append(doi_doc.data['originalText'])
                        else:
                            articles_text.append(" ")
                    except Exception as ex:
                        articles_text.append(" ")
                        log_file.write(f"[{term}] Text Exception: {ex}, DOI: {article_dict['prism:doi']}\n")
                        #print("\ntext Exception:", ex)              
                else:
                    log_file.write(f"[{term}] Read document failed. DOI: {article_dict['prism:doi']}\n")
                    print (f"\nRead document failed. DOI: {article_dict['prism:doi']}")
                    time.sleep(8)
                log_file.flush()
            writeArticlesInDataBase(articles_title, articles_abstract, articles_keywords, articles_text, term, pack_index)
        except Exception as ex:
            log_file.write(f"[{term}] Data Exception(pack [{pack_index}]): {ex}\n")
            print(f"\nData Exception(pack [{pack_index}]): {ex}")
##################################################################################################################################
def resume():
    #this function is for resumming getting and writing data after crashing program

    connection = createConnection(data_base)  
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute("SELECT max(Term_id) FROM Tech_Terms_Data")
            index_start_in_fifty_terms_pack = crsr.fetchall()[0][0] % 50

            y_n = input(f"Do you want to restart searching for \"{[k for k, v in tech_terms_id_dict.items() if v == (n_fifty_terms - 1) * 50 + index_start_in_fifty_terms_pack][0]}\"({(n_fifty_terms - 1) * 50 + index_start_in_fifty_terms_pack}) and delete its previous results (y/n):")
            if y_n == 'y' or y_n == 'Y':
                crsr.execute("DELETE FROM Tech_Terms_Data WHERE Term_id = (SELECT max(Term_id) FROM Tech_Terms_Data) AND ID != 0")
                connection.commit()
            else:
                index_start_in_fifty_terms_pack = index_start_in_fifty_terms_pack + 1

        except Exception as ex:
            log_file.write(f"Can't get strat index of terms pack or can't delete extera result. Exiting from program... : {ex}\n")
            print(f"\nCan't get strat index of terms pack or can't delete extera result. Exiting from program... : {ex}")
            quit()

        connection.close()
    else:
        log_file.write("Connection is failed. Can't get strat index of terms pack or can't delete extera result. Exiting from program...\n")
        print("\nConnection is failed. Can't get strat index of terms pack or can't delete extera result. Exiting from program...")
        quit()

    return index_start_in_fifty_terms_pack
    
##################################################################################################################################
def main():

    print("----------------------------------------------------------------------------------------------")
    ## Load configuration
    con_file = open(os.getcwd() + "\\config.json")
    config = json.load(con_file)
    con_file.close()

    ## Initialize client
    client = ElsClient(config['apikey'])
    client.inst_token = config['insttoken']
    #print("key: ", client.api_key, "inst_token: ", client.inst_token)
    
    ##create empty database
    createDataBaseTables()
    
    #read terms from file
    tech_terms = pd.read_csv(tech_terms_list_file)
    tech_terms_list = [x for x in tech_terms['Tech_Terms']]
    #print(tech_terms)

    #assign id to every term
    for i in range(len(tech_terms_list)):
        tech_terms_id_dict[tech_terms_list[i]] = i

    #write terms-id in database
    writeTermsInDataBase(tech_terms_list)

    index_start_in_fifty_terms_pack = 0
    index_start_in_fifty_terms_pack = resume()

    log_file.write(f"The n'th fifty terms: {n_fifty_terms} [{(n_fifty_terms - 1) * 50 + index_start_in_fifty_terms_pack} : {n_fifty_terms * 50}]\
                    \nSize of articles pack for writing: {s_a_p_w}\nGet all term search results: {get_all_term_search_results}\n")
    log_file.write("########################################################################################\n")
    
    #select 50 terms(or less in resuming)
    tech_terms_list = tech_terms_list[(n_fifty_terms - 1) * 50 + index_start_in_fifty_terms_pack : n_fifty_terms * 50]
    terms_cntr = 1
    for term in list(dict.fromkeys(tech_terms_list)):
        term_start = time.time()
        print("####################################################################################################################")
        print(f"{tech_terms_id_dict[term]} - {term}")
        log_file.write(f"[{term}] Number in list: {tech_terms_id_dict[term]}\n")

        getData(client, term)

        term_end = time.time()
        term_elapsed_time = str(int((term_end - term_start) / 3600)) + "h " + str(int(((term_end - term_start) % 3600) / 60)) + "m"
        #term_elapsed_time = term_end - term_start
        log_file.write(f"[{term}] Time: {term_elapsed_time}\n")
        log_file.write("----------------------------------------------------------------------------------------\n")
        print("Time: " , term_elapsed_time)
        terms_cntr = terms_cntr + 1
##################################################################################################################################
if __name__ == '__main__':
    main()