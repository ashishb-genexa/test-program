import pandas as pd
import src.util.helper as helper
import src.settings.constants as const
import src.util.sqllite_helper as db_manager
import src.util.log_helper as log_helper
import src.util.api_helper as api_helper

# This is Create the Logger Instance 
logger = log_helper.set_get_logger("ner_text_extractor",helper.get_logfile_name())

def process_company_list_update_openai_ner_extraction_in_db(list_company):
	list_rsp = []
	logger.info("started Open AI NER extraction and inserting in db.")
	for cmp_name in list_company:
		str_sql = "Select id ,company_info,team_info,company_info_json,team_info_json from company Where name = ?"
		rows = db_manager.select_sql(str_sql,[cmp_name])
		cmp_info_found_in_db = False
		team_info_found_in_db = False
		for row in rows:
			rsp = {"id" : row[0]}
			if str(row[1]).strip() != "NONE":
				if str(row[3]).strip() == "NONE":
					rsp["cmp_json"] = api_helper.get_response_from_openai_json(const.PROMPT_DICT["compnay_info"].replace("{context}",row[1]))
				else:
					cmp_info_found_in_db = True
					rsp["cmp_json"] = "NO UPDATE"
			else:
				rsp["cmp_json"] = "NO UPDATE"
			if str(row[2]).strip() != "NONE":
				if str(row[4]).strip() == "NONE":
					rsp["team_json"] = api_helper.get_response_from_openai_json(const.PROMPT_DICT["team_info"].replace("{context}",row[2])) 
				else:
					team_info_found_in_db = True
					rsp["team_json"] = "NO UPDATE"
			else:
				rsp["team_json"] = "NO UPDATE"
			list_rsp.append(rsp)
		
		if cmp_info_found_in_db:
			logger.info(f"Company {cmp_name} company info json available in DB.")
		if team_info_found_in_db:
			logger.info(f"Company {cmp_name} team info json available in DB.")

		# Print Warning message for support team to investigate
		if len(rows) == 0:
			logger.warning(f"Company : {cmp_name} does not have record in DB. Support team need to investigate this.")
	
	# Now Update all records back to DB
	logger.info("Updating Open AI extraction company info JSON, team info JSON to DB")
	for rsp in list_rsp:
		if rsp["cmp_json"] != "NO UPDATE":
			str_sql = "Update company Set company_info_json = ? where id = ?"
			data = [rsp["cmp_json"],int(rsp["id"])]
			#print(str_sql,data)
			db_manager.execute_sql(str_sql,data)
		if rsp["team_json"] != "NO UPDATE":
			str_sql = "Update company Set team_info_json = ? where id = ?"
			data = [rsp["team_json"],int(rsp["id"])]
			db_manager.execute_sql(str_sql,data)

def is_extracted_text_exists_in_db(company_name : str):
	str_sql = "Select company_info from company Where name = ?"
	rsp = db_manager.select_scaler(str_sql,[company_name])
	if rsp.lower() == "none":
		return False		
	else:
		return True

def get_scraped_data_from_db(company_name : str):
	str_sql = "Select Id, name, page_url,url_type,page_text from company Where name = ?"
	rows = db_manager.select_sql(str_sql,[company_name])
	df = pd.DataFrame(rows, columns=['id', 'company_name', 'page_url','url_type','page_text'])
	return df

def process_company_list_extract_sentence_by_keyword(list_cmp_names):
	for cmp_name in list_cmp_names:
		if is_extracted_text_exists_in_db(cmp_name):
			logger.info(f"extracted text found in db for company {cmp_name}")
		else:
			df = get_scraped_data_from_db(cmp_name)
			extract_sentence_by_keyword_and_insert_in_db(df,cmp_name)


def extract_sentence_by_keyword_and_insert_in_db(df, company_name):
	list_rsp = []
	# Loop Through all page text and Find Company and team info related text
	for idx in range(len(df)):
		row = df.iloc[idx]
		rsp = {}
		rsp["id"] = row["id"]
		rsp['company_name'] = company_name
		rsp['page_url'] = row['page_url']
		page_text = str(row["page_text"]).strip()
		#print("page text length : ",len(page_text))
		if len(page_text) > 0:
			page_text = helper.clean_scrapped_text(page_text)
			rsp["page_text_words"] = len(page_text.split(" "))
			rsp = find_company_and_team_info_from_text(rsp,page_text)
			#print(rsp)
		else:
			rsp['company_info'] = "NONE"
			rsp["company_info_words"] = 0
			rsp["team_info"] = "NONE"
			rsp["team_info_words"] = 0
			rsp["page_text_words"] = 0
		list_rsp.append(rsp)

	logger.info(f"Company Name : {company_name} , Updating extracted company and team info into db.")
	for rsp in list_rsp:
		str_sql = "Update company Set page_text_words = ? ,company_info = ?,company_info_words = ?, \
			team_info = ?,team_info_words = ? where id = ?"
		data = [rsp["page_text_words"],rsp['company_info'],rsp["company_info_words"], \
					rsp["team_info"],rsp["team_info_words"],int(rsp["id"])]
		#print("rsp id data type : ",type(rsp["id"]),rsp["id"], int(rsp["id"]))
		db_manager.execute_sql(str_sql,data)

# This Code is used for Excel Testing. Later on can be removed.
def extract_sentence_by_keyword(df):
	df_cmp = df[df['is_success'] == False]
	df_cmp = df_cmp.drop(columns=['url_type','page_urls','page_text'])
	df_cmp["company_info"] = ''
	df_cmp["team_info"] = ''

	#df_cmp.head(20)

	df_grp = df[df['is_success'] == True].groupby(['company_name'], as_index=False).agg(count=('company_name', 'count'))

	list_rsp = []
	for idx in range(len(df_grp)):
		row = df_grp.iloc[idx]
		list_rsp.append(find_info_from_scrapped_text(df,row['company_name'],const.KEYWORDS_FOR_COMPANY_INFO,const.KEYWORDS_FOR_TEAM))

	df_info = pd.DataFrame.from_dict(list_rsp)

	df_info = pd.concat([df_info, df_cmp],axis = 0)
	return df_info

def split_large_sentence(list_sents):
	new_sent_list = []
	for sent in list_sents:
		words = sent.split(" ")
		#print("Total Words : ",len(words))
		if (len(words) >= const.SPLIT_LARGE_TEXT_CHUNK_SENTENCE_MAX_WORD):
			for i in range(0, len(words), const.SPLIT_LARGE_TEXT_CHUNK_SENTENCE_MAX_WORD):
				chunk = words[i:i + const.SPLIT_LARGE_TEXT_CHUNK_SENTENCE_MAX_WORD]
				#print(i,chunk)
				new_sent_list.append(helper.clean_scrapped_text(' '.join(chunk)))
		else:
			new_sent_list.append(sent)
	return new_sent_list

def find_company_and_team_info_from_text(rsp, text):
	#Sentence Splitter using split
	sents = text.replace("\n","").split(". ")
	
	sents = split_large_sentence(sents)

	# Section TO Get company Info Details
	found_sent = []
	total_words = 0
	for sent in sents:
		# Until Key word not found go through finding.				
		if len(found_sent) == 0:			
			for key in const.KEYWORDS_FOR_COMPANY_INFO:
				if sent.find(key) > -1:
					total_words += len(sent.split(" "))
					found_sent.append(sent) 
					break
		else: # We will add Sentences, till we will reach max Limit
			if  (total_words +len(sent.split(" "))) <= const.CHUNK_MAX_WORDS:
				total_words += len(sent.split(" "))
				found_sent.append(sent)
			else:
				break
	if ( len(found_sent) == 0):
		rsp['company_info'] = 'NONE'
		rsp["company_info_words"] = 0
	else:
		rsp['company_info'] = ". ".join(found_sent)
		rsp["company_info_words"] = total_words

	# Section to get  Team info
	total_words = 0
	found_sent.clear()
	found_sent = []
	for sent in sents:
		# Until Key word not found go through finding.				
		if len(found_sent) == 0:			
			for key in const.KEYWORDS_FOR_TEAM:
				if sent.find(key) > -1:
					total_words += len(sent.split(" "))
					found_sent.append(sent) 
					break
		else: # We will add Sentences, till we will reach max Limit
			if ( total_words + len(sent.split(" ")) <= const.CHUNK_MAX_WORDS):
				total_words += len(sent.split(" "))
				found_sent.append(sent)
			else:
				break
	if ( len(found_sent) == 0):
		rsp['team_info'] = 'NONE'
		rsp["team_info_words"] = 0
	else:
		rsp['team_info'] = ". ".join(found_sent)
		rsp["team_info_words"] = total_words
	return rsp	

def find_info_from_scrapped_text(df,commpany_name,list_keywords_company, list_keyword_team):
	rsp = {}
	rsp['is_success'] = True
	rsp['company_name'] = commpany_name

	df_flt = df[df['company_name'] == commpany_name]
	#print(df_flt[df_flt["url_type"] == 'root']['page_url'].values[0])
	rsp['page_url'] = df_flt[df_flt["url_type"] == 'root']['page_url'].values[0]
	for idx in range(len(df_flt)):
		row = df_flt.iloc[idx]
		if str(row["page_text"]).strip() == "":
			continue
		text = helper.clean_scrapped_text(str(row["page_text"]))
		rsp["page_text_words"] = len(text.split(" "))

		rsp = find_company_and_team_info_from_text(rsp,text)
	return rsp


