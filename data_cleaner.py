import pandas as pd
import sqlite3
import json
import os
import time
import re
import nltk
import spacy
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer 
from nltk.corpus import wordnet
##################################################################################################################################
n_fifty_terms = 1 # the n'th fifty terms
s_a_p_c = 500 # size of article pack for cleaning
tech_terms_id_dict = {}
tech_terms_with_dash = []
tech_terms_without_dash = []
valid_symbol = []
text_symbol = []
unique_articles_ID = ()
extra_words_should_be_deleted = []
data_base = os.getcwd() + f"\\data\\Robotics_tech_terms_{n_fifty_terms}.db"
data_base_cleaned = os.getcwd() + f"\\data\\Robotics_tech_terms_cleaned_{n_fifty_terms}.db"
tech_terms_list_file = os.getcwd() + r"\\Robotics_tech_terms.csv"
tech_terms_for_cleaning_file = os.getcwd() + r"\\Rob_Techs_for_Cleaning.csv"
symbol_list_file = os.getcwd() + r"\\Symbols.csv"
extra_words_should_be_deleted_file = open("extra_words_should_be_deleted.txt", 'r', encoding='utf-8')
tech_terms_log_file = os.getcwd() + f"\\logs\\Robotics_tech_terms_cleaned_{n_fifty_terms}.log"
log_file = open(tech_terms_log_file, 'a', encoding = 'utf-8')
lemmatizer = WordNetLemmatizer()
##################################################################################################################################
nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
nltk.download('averaged_perceptron_tagger')
en_model = spacy.load('en_core_web_sm')
spacy_stop_words = en_model.Defaults.stop_words
nltk_stop_words = stopwords.words('english')
my_stop_words={'a', 'an', 'as', 'at', 'et', 'eg', 'al', 'by', 'so', 'am', 'is', 'are', 'was', 'were', 'and', 'or', 'to', 'in', 'out', 'of', 'up', 'for', 'from', 'under', 'below', 'above', 'on', 'top', 'bottom', 'left', 'right', 'the', 'nth', ' but', 'be',\
            'may', 'shall', 'should', 'will', 'would', 'can', 'could', 'have', 'has', 'do', 'does', 'doesn', 'i', 'we', 'he', 'she', 'it', 'its', 'this', 'these', 'that', 'they', 'here', 'there' \
            'my', 'me', 'him', 'his', 'her', 'our', 'their', 'then', 'what', 'where', 'who', 'whom', 'whose', 'when', 'which', 'how', 'too', 'also', 'only', 'all', 'any', 'etc', 'even', 'every', 'ever', 'never',\
            'with', 'both', 'yes', 'no', 'nor', 'none', 'not', 'true', 'false', 'if', 'else', 'such', \
            'jpg', 'jpeg', 'png', 'svg', 'pdf' \
            'many', 'much', 'more', 'some', 'before', 'after', 'previous', 'between', 'time', 'now', 'yesterday', 'tomorrow', 'day', 'days', 'week', 'weeks', 'month', 'months', 'year', 'years', 'fig',\
            'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten', 'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen', 'eighteen', 'nineteen',\
            'twenty', 'thirty', 'forty','fifty', 'sixty', 'seventy', 'eighty', 'ninety', 'hundred'}

total_stop_words = spacy_stop_words.union(nltk_stop_words, my_stop_words)
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
                                        ID integer PRIMARY KEY,
                                        Term_id integer,
                                        Title text,
                                        Abstract text,
                                        Keywords text,
                                        Text text
                                    );"""
    connection = createConnection(data_base_cleaned)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(sql_create_tech_terms_ID_table)
            crsr.execute(sql_create_tech_terms_data_table)
            connection.commit()
        except Exception as ex:
            log_file.write(f"Can't create tables in data base: {ex}. Exiting from program...\n")
            print(f"\nCan't create tables in data base: {ex}. Exiting from program...\n")
            quit()
        connection.close()
    else:
        log_file.write("Connection is failed. Can't create tables in data base. Exiting from program...\n")
        print("\nConnection is failed. Can't create tables in data base. Exiting from program...")
        quit()
##################################################################################################################################
def writeTermsInDataBase(tech_terms):
    #writing terms-id in Tech_Terms_ID table
    connection = createConnection(data_base_cleaned)  
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
# this func is used in lemma func
def get_wordnet_pos(word):
    """Map POS tag to first character lemmatize() accepts"""
    tag = nltk.pos_tag([word])[0][1][0].upper()
    tag_dict = {"J": wordnet.ADJ,
                "N": wordnet.NOUN,
                "V": wordnet.VERB,
                "R": wordnet.ADV}

    return tag_dict.get(tag, wordnet.NOUN)
##################################################################################################################################
# Lemmatizing tokens with the appropriate POS tag  
def lemma(token):
    if token in tech_terms_with_dash:
        return token
    else :
        global lemmatizer 
        return lemmatizer.lemmatize(token, get_wordnet_pos(token))
##################################################################################################################################
def cleaningData(text):
    max_len_compound_terms = 45
    max_len_simple_terms = 25
    #punctuations = r'''!()-+=[]{};:'"\,<>./?@#$%^&*_“~'''

    # Setting every word to lower
    text = text.lower()

    #Replace symbols with text
    for rg in range(len(valid_symbol)):
        text = text.replace(valid_symbol[rg], text_symbol[rg])

    # Join words of tech terms
    for rg in range(len(tech_terms_without_dash)):
        text = text.replace(tech_terms_without_dash[rg], tech_terms_with_dash[rg])
    
    # Cleaning the urls
    text = re.sub(r'https?://\S+|www\.\S+', ' ', text)

    # Cleaning the Emails
    text = re.sub(r'\w+@\S+', ' ', text)

    # Removing Apostrophe
    text = re.sub(r"'s|n't", " ", text)

    # Removing words that have numbers in midle of them
    #text = re.sub(r'[^0-9 -]+\d+\.?\d*[^0-9 ]+', ' ', text)
    
    # Removing every char except a-z, 0-9, space and '-' 
    text = re.sub(r'[^a-z0-9 -]+', ' ', text)

    # Removing words that have numbers in midle of them
    text = re.sub(r'\s+[a-z]+\d+[a-z]+\w*', ' ', text) 

    # Removing alone combinations of numbers, space and dash
    text = re.sub(r'\s+[0-9 -]+\s+', ' ', text)

    # Removing alone combinations of s, numbers and dash
    text = re.sub(r'\s+[0-9 s-]+\s+', ' ', text)

    # Removing alone combinations of t, numbers and dash
    text = re.sub(r'\s+[0-9 t-]+\s+', ' ', text)

    # Removing alone combinations of z, numbers and dash
    text = re.sub(r'\s+[0-9 z-]+\s+', ' ', text)

    # Removing 0-s0042698921000274-gr4, 0-s0042698921000274-mmc2 and like them
    text = re.sub(r'\s+0-s\w+-?\w+\s+', ' ', text)

    # Removing si1, si2, ...
    text = re.sub(r'\s+si\d+', ' ', text)

    # Removing gr1, gr2, ...
    text = re.sub(r'\s+gr\d+', ' ', text)

    # Removing ga1, ga2, ...
    text = re.sub(r'\s+ga\d+', ' ', text)

    # Removing nth
    text = re.sub(r'\s+\d+th', ' ', text)

    # Removing extra dash
    text = re.sub(r'-+', "-", text)
    
    # Cleaning the white spaces
    text = re.sub(r'\s+', ' ', text).strip()

    # Converting all our text to a list 
    text_tokens = nltk.word_tokenize(text)  #text_tokens = text.split(' ') 

    # Droping empty strings and strings with one char or two chars that contain dash
    text_tokens  = [x  for x in text_tokens if (len(x) >= 3) and (len(x) <= max_len_compound_terms) and (len(x) <= max_len_simple_terms or ('-' in x)) or (len(x) == 2 and ('-' not in x))]
    
    # Droping stop words
    text_tokens = [x for x in text_tokens if x not in total_stop_words]

    # Droping extra_words_should_be_deleted
    text_tokens = [x for x in text_tokens if x not in extra_words_should_be_deleted]

    # Lemmatizing tokens with nltk
    #text_tokens = [lemma(token) for token in text_tokens]
    #text_tokens = [lemmatizer.lemmatize(token, get_wordnet_pos(token)) for token in text_tokens]

    return text_tokens
  
##################################################################################################################################
def readArticlesFromDataBase(begin_id):
    ID = []
    Terms_id = []
    articles_title = []
    articles_abstract = []
    articles_keywords = []
    articles_text = []

    connection = createConnection(data_base)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"SELECT * FROM Tech_Terms_Data WHERE ID >= {begin_id} AND ID < {begin_id + s_a_p_c} and ID IN {unique_articles_ID}")
            records = crsr.fetchall()
            for row in records:
                ID.append(row[0])
                Terms_id.append(row[1])
                articles_title.append(row[2])
                articles_abstract.append(row[3])
                articles_keywords.append(row[4])
                articles_text.append(row[5])
            
        except Exception as ex:
            log_file.write(f"Can't read articles:{ex}\n")
        connection.close()
    else:
        log_file.write(f"Connection is failed. Can't read articles.\n")

    for rg in range(len(ID)):
        try:
            if len(articles_title[rg]) > 1:
                articles_title[rg] = cleaningData(articles_title[rg])
        except Exception as ex:
            log_file.write(f"Can't clean title: {ex}. Article ID: {ID[rg]}\n") 
        try:
            if len(articles_abstract[rg]) > 1:
                articles_abstract[rg] = cleaningData(articles_abstract[rg])
        except Exception as ex:
            log_file.write(f"Can't clean abstract: {ex}. Article ID: {ID[rg]}\n") 
        
        try:
            if len(articles_keywords[rg]) > 1:
                articles_keywords[rg] = cleaningData(articles_keywords[rg])
        except Exception as ex:
            log_file.write(f"Can't clean keywords: {ex}. Article ID: {ID[rg]}\n") 
        
        try:
            if len(articles_text[rg]) > 1:
                articles_text[rg] = cleaningData(articles_text[rg])
        except Exception as ex:
            log_file.write(f"Can't clean text: {ex}. Article ID: {ID[rg]}\n") 

    writeCleanedArticlesInDataBase(ID, Terms_id, articles_title, articles_abstract, articles_keywords, articles_text)
##################################################################################################################################
def writeCleanedArticlesInDataBase(ID, Terms_id, articles_title, articles_abstract, articles_keywords, articles_text):
    connection = createConnection(data_base_cleaned)
    if connection != None:
        crsr = connection.cursor()
        for i in range(len(ID)):
            try:
                crsr.execute(f"""INSERT INTO Tech_Terms_Data VALUES("{ID[i]}", "{Terms_id[i]}",
                            "{articles_title[i]}", "{articles_abstract[i]}", "{articles_keywords[i]}", "{articles_text[i]}" )""")
            except Exception as ex:
                log_file.write(f"Can't write article(ID = {ID[i]}) to data base: {ex}\n")
        try:
            connection.commit()
        except Exception as ex:
                log_file.write(f"Can't commit articles(last ID = {ID[i]}) to data base: {ex}\n")
        connection.close()
    else:
        log_file.write(f"Connection is failed. Can't write articles pack[{ID[0]} - {s_a_p_c}].\n")
    
##################################################################################################################################
def getStartAndEndID():
    #this function is for resumming getting and writing data after crashing program
    connection = createConnection(data_base_cleaned)  
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute("SELECT max(ID) FROM Tech_Terms_Data")
            record = crsr.fetchall()
            if record == [(None,)]:
                start_cleaning_id = 0
            else:
                start_cleaning_id = crsr.fetchall()[0][0] + 1

        except Exception as ex:
            log_file.write(f"Can't get strat ID. Exiting from program... : {ex}\n")
            print(f"\nCan't get strat ID. Exiting from program... : {ex}")
            quit()

        connection.close()
    else:
        log_file.write("Connection is failed. Can't get strat ID. Exiting from program...\n")
        print("\nConnection is failed. Can't get strat ID. Exiting from program...")
        quit()


    connection = createConnection(data_base)  
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute("SELECT max(ID) FROM Tech_Terms_Data")
            end_cleaning_id = crsr.fetchall()[0][0]

        except Exception as ex:
            log_file.write(f"Can't get end ID. Exiting from program... : {ex}\n")
            print(f"\nCan't get end ID. Exiting from program... : {ex}")
            quit()

        connection.close()
    else:
        log_file.write("Connection is failed. Can't get end ID. Exiting from program...\n")
        print("\nConnection is failed. Can't get end ID. Exiting from program...")
        quit()


    return start_cleaning_id, end_cleaning_id        
##################################################################################################################################
def getDashTermsAndTextSymbol():
    # Read terms from file
    tech_terms_clean = pd.read_csv(tech_terms_for_cleaning_file)
    terms_without_dash = [x for x in tech_terms_clean['without_dash']]
    terms_with_dash = [x for x in tech_terms_clean['with_dash']]
    
    # Read symbols from file
    symbols = pd.read_csv(symbol_list_file)
    symbls = [x for x in symbols['symbol']]
    txt_symbls = [x for x in symbols['text_symbol']]

    return terms_without_dash, terms_with_dash, symbls, txt_symbls
##################################################################################################################################
def getUniqueArticleID():
    uniqe_ID_title_dict = {}
    unique_ID = ()
    
    connection = createConnection(data_base) 
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute("SELECT ID, Term_ID, Title FROM Tech_Terms_Data")
            records = crsr.fetchall()
            current_term_ID = records[0][1]

            for rg in range(len(records)):
                if records[rg][1] == current_term_ID:
                    uniqe_ID_title_dict[records[rg][2]] = records[rg][0]
                else:
                    unique_ID = unique_ID + tuple(uniqe_ID_title_dict.values())
                    uniqe_ID_title_dict.clear()
                    uniqe_ID_title_dict[records[rg][2]] = records[rg][0]
                    current_term_ID = current_term_ID + 1

            unique_ID = unique_ID + tuple(uniqe_ID_title_dict.values())

        except Exception as ex:
            log_file.write(f"Can't get duplicate IDs. Exiting from program... : {ex}\n")
            print(f"\nCan't get duplicate IDs. Exiting from program... : {ex}")
            quit()

        connection.close()
    else:
        log_file.write("Connection is failed. Can't get duplicate IDs. Exiting from program...\n")
        print("\nConnection is failed. Can't get duplicate IDs. Exiting from program...")
        quit()

    return unique_ID
##################################################################################################################################
def removeFirstRow():
    connection = createConnection(data_base)
    if connection != None:
        crsr = connection.cursor()
        try:
            crsr.execute(f"DELETE FROM Tech_Terms_Data WHERE ID = 0")
            connection.commit()
        except Exception as ex:
            print("Can't delete first row.\n")
            log_file.write(f"Can't delete first row\n")
        connection.close()
    else:
        print("Connection is failed. Can't delete first row.\n")
        log_file.write(f"Connection is failed. Can't delete first row\n")
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

    # Read extra words should be deleted
    global extra_words_should_be_deleted
    extra_words_should_be_deleted = extra_words_should_be_deleted_file.read()
    extra_words_should_be_deleted = extra_words_should_be_deleted.split('\n')

    # Remove initialzation in downloading step
    removeFirstRow()

    # Get start and end ID for cleaning
    start_cleaning_ID, end_cleaning_ID = getStartAndEndID()
    print("\nstart-end ID: ", start_cleaning_ID, "-" , end_cleaning_ID)

    # Get terms-dash and text-symbols
    global tech_terms_with_dash, tech_terms_without_dash, valid_symbol, text_symbol
    tech_terms_without_dash, tech_terms_with_dash, valid_symbol, text_symbol = getDashTermsAndTextSymbol()
    #print("len: ", len(tech_terms_with_dash), len(text_symbol))

    # Get unique articles ID
    global unique_articles_ID
    unique_articles_ID = getUniqueArticleID()
    print(f"\nNumber of duplicate articles: {end_cleaning_ID - len(unique_articles_ID) + 1}")
    print(f"\nNumber of unique articles: {len(unique_articles_ID)}")

    # Write some info to Log file
    log_file.write(f"The n'th fifty terms: {n_fifty_terms} [{(n_fifty_terms - 1) * 50} : {n_fifty_terms * 50}]\
                    \nSize of articles pack for cleaning: {s_a_p_c}\nRow Start - End ID: {start_cleaning_ID} - {end_cleaning_ID}\n")
    log_file.write(f"Number of duplicate articles: {end_cleaning_ID - len(unique_articles_ID) + 1}\n")
    log_file.write(f"Number of unique articles: {len(unique_articles_ID)}\n")
    log_file.write("########################################################################################\n")
    
    print("\nCleaning ...")
    clean_start = time.time()

    # Main loop for cleaning
    for begin_id in tqdm(range(start_cleaning_ID, end_cleaning_ID + 1, s_a_p_c)):
        readArticlesFromDataBase(begin_id)
        log_file.flush()
    
    clean_end = time.time()
    clean_elapsed_time = str(int((clean_end - clean_start) / 3600)) + "h " + str(int(((clean_end - clean_start) % 3600) / 60)) + "m"
    #clean_elapsed_time = term_end - term_start
    log_file.write(f"Time: {clean_elapsed_time}\n")
    log_file.write("----------------------------------------------------------------------------------------\n")
    print("Time: " , clean_elapsed_time)

##################################################################################################################################
if __name__ == '__main__':
    main()
