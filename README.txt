process of the codes:

1 - crawling_elsevier_API_DB.py 
	Downloading many text for every tech-term listed in Robotics_tech_terms.csv using Scopus API. 
	For working with this API you need university IP.
	Saving texts to Robotics_tech_terms_{num}.db, num = 0, 1, ..., 10. Every .db file contains 50 tech-terms

2 - data_cleaner.py
	Cleaning useless text and tokenizing them into seprated terms.
	Saving terms to Robotics_tech_terms_cleaned_{num}.db, num = 0, 1, ..., 10. Every .db file contains 50 tech-terms
	
3 - aggregate_data.py
	Aggregating all texts of every tech-term into one row.
	Saving to Robotics_tech_terms_aggregate_{num}.db, num = 0, 1, ..., 10. Every .db file contains 50 tech-terms(50 rows)
	
4 - gathering_aggregates.py
	Gathering all data to one file.
	Saving to Robotics_tech_terms_gathered.db, this file contains almost 500 rows(almost 500 tech-terms) and every row contains a lot(2000~5000) of aggregated tokenized cleaned texts(papers)
	
5 - Frequency_NTables.py
	Calculating term frequency for every row of gathering data and saving in 500 seprated tables.
	Saving to Robotics_tech_terms_Frequency_Ntables.db
	
6 - TF_Nfifty_tables_and_two_200Col_tables.py
	Joining tables. first 50 tables, second 50 tables and so on.
	Saving to TF_Nfifty_tables_and_two_200Col_tables.db. This file has 10 tables with 50 columns(50 technologies).
	
7 - query_for_200col_TF_table.py
	Building query for SQL to join 4 50 columns tables.
	Saving to TF_Nfifty_tables_and_two_200Col_tables.db. This file has 2 big tables with N rows(N = total unique terms in 200 techs) and 200 columns(200 technologies)
	
8 - TF_IDF.py
	Calculating TF-IDF for tables with 200 technologies.
	Saving Robotics_tech_terms_TTFIDF.db. This file contains a table that every column represents numeric form of corresponding technology. It's what we want:)
	
9 - measuring_cosine_distance.py
	Comparing technologies(columns) with two metric: Cosine similarity and Euclidean distance.
	Saving to Robotics_tech_terms_measuring_cosine_distance.db
	
10 - normalized_percent.py
	Normalizing compared data and representing them percently.
	Saving to  Robotics_tech_terms_normalized_percent.db
	
11 - Graph_code.py
	Producing a code for drawing graph in https://dreampuf.github.io/GraphvizOnline
	Saving to Graph_code.txt
	

	
