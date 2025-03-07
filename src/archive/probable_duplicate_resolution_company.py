import src.webscrapper.webscrapper as wsc
import src.ner.ner_team_company_info as ner
import src.util.sqllite_helper as db_manager
import src.util.log_helper as log_helper
import src.util.helper as helper
import src.settings.constants as const
import json
import time

logger = log_helper.set_get_logger("duplicate_resolution_company",helper.get_logfile_name())

def is_contact_available_in_team_info(rsp,contact_name, company_name, team_info_dict):
	rsp["contact_found"] = False
	rsp["source"] = "Not found"
	rsp["score"] = 0.00
	rsp["flag"] = "Duplicate"
	list_contact = team_info_dict[company_name]
	#logger.info(f"company : {company_name}, list contacts : {list_contact}")
	for contact in list_contact:
		if contact["name"].strip().lower() == contact_name.strip().lower():
			rsp["contact_found"] = True
			rsp["source"] = contact["source"]
			rsp["flag"] = "Keep"
			rsp["score"] = 100.00
			break
	return rsp	 

def get_team_info(company_name : str,team_info_dict: dict):
	if company_name in team_info_dict.keys():
		return team_info_dict
	
	list_team_info = []
	str_sql = "Select name, team_info_json, page_url from company where name = ?"
	rows = db_manager.select_sql(str_sql,[company_name])	
	for row in rows:
		if row[1] != "NONE":
			out_dict = json.loads(row[1])
			if isinstance(out_dict, dict):
				out_dict["source"] = row[2]
				list_team_info.append(out_dict)
			if isinstance(out_dict, list):
				for out_d in out_dict:
					out_d["source"] = row[2]
				list_team_info.extend(out_dict)	
	team_info_dict[company_name] = list_team_info
	return team_info_dict

def is_company_exists_in_company_master(company_name):
	str_sql = "Select id  from company_master Where name = ? COLLATE NOCASE"
	rsp = db_manager.select_scaler(str_sql,[company_name])
	if rsp == "Not Found":
		return False		
	else:
		return True

def insert_company_master(data):
	str_sql = "Insert into company_master ( name, website,robot_file,is_scraping_allowed) Values(?,?,?,?)"
	db_manager.execute_sql(str_sql,data)

def prepare_company_master(list_company):
	"""
	This function will check for each company, Get Website Link and Check For robots.txt file.
	If robots.txt file exists, mark that is_scraping_allowed will be false.
	"""
	for cmp_name in list_company:
		cmp_name = str(cmp_name).strip()
		rsp = is_company_exists_in_company_master(cmp_name)
		if rsp :
			logger.info(f"company {cmp_name} is exists in Company master.")
			continue
		url = wsc.get_company_website(cmp_name)
		if url == "Not found":
			data = [cmp_name,url,'NONE',0]
			insert_company_master(data)
			logger.info(f"Website not found for company {cmp_name}")
			continue 
		# Get Robot text From Website and then insert data into DB
		robot_file = wsc.get_website_robot_file(url,cmp_name)
		if robot_file != "Not Found":
			data = [cmp_name,url,helper.read_text_file_as_string(robot_file),0]
			insert_company_master(data)
			logger.info(f"Robot file found : {robot_file}")	
		else:			 	
			data = [cmp_name,url,robot_file,1]
			insert_company_master(data)
		time.sleep(const.SLEEP_TIME_BETWEEN_REQUEST)

def process_company_list_get_scrapped_data(list_company):
	logger.info(f"Total {len(list_company)} companies found to process..")
	logger.info("Started preparing company master table with website and robots file details")
	prepare_company_master(list_company)
	logger.info("Started getting scrap data for each company.")
	for cmp_name in list_company:
		get_scrapped_data_and_insert_in_db(cmp_name)
	logger.info("All companies processed for scraping website content.")

def get_website_robot_file_from_db(cmp_name:str):
	str_sql = "Select website,robot_file from company_master where name = ? COLLATE NOCASE"
	rsp = db_manager.select_sql(str_sql,[cmp_name])
	return rsp

def is_scrap_data_exists_in_db(company_name):
  str_sql = "Select id  from company Where name = ? COLLATE NOCASE"
  rsp = db_manager.select_scaler(str_sql,[company_name])
  if rsp == "Not Found":
    return False    
  else:
    return True

def get_scrapped_data_and_insert_in_db(company_name):
	#Step 1 get Company Website, robots file and  
	cmp_data = get_website_robot_file_from_db(company_name)
	
	if len(cmp_data) == 0:
		logger.warning(f"Company {company_name} does not exists in company master.")
		return
	
	if str(cmp_data[0][0]).lower() == "not found":
		logger.warning(f"company {company_name} website not found in company master.")
		return

	if is_scrap_data_exists_in_db(company_name):
		logger.info(f"Company {company_name} pages scrpped data exists in db")
		return

	#Step 2 Get Scrapped Content based on Pages available on website
	list_rsp = wsc.get_website_content_for_contact(company_name,str(cmp_data[0][0]).strip(),str(cmp_data[0][1]))

	#Step 3 Insert Data into Database
	logger.info("Inserting page content into db.")
	str_sql = "Insert into company ( name, page_url,url_type,page_text) Values(?,?,?,?)"
	for rec in list_rsp:
		data = [rec["company_name"],rec["page_url"],rec["url_type"],rec["page_text"]]
		db_manager.execute_sql(str_sql,data)

def assign_probable_duplicate_flag(df : any, list_exact_dup : list[int] ):
	
	logger.info("Started assignment of probable duplicate flag")
	# Remove Exact Duplicate rows from Dataframe
	df_flt = df[df['dup_group_id'] != 999999]
	for exact_dup_id in list_exact_dup:
		df_flt = df_flt[df_flt['dup_group_id'] != exact_dup_id]
	list_prob_dup = df_flt["dup_group_id"].unique().tolist()
	list_company = df_flt["CompanyName"].unique().tolist()

	# Get and Insert website data in DB
	process_company_list_get_scrapped_data(list_company)

	# Now Process all scrap Data, mark with keyword and prepare for Gen AI Task
	ner.process_company_list_extract_sentence_by_keyword(list_company)

	# #Now Update Get Ner extraction using OpenAI and update in DB
	ner.process_company_list_update_openai_ner_extraction_in_db(list_company)  

	# #Now Go through each duplicate Group and Assign Keep Delete Flag
	team_info_dict = {}
	for prob_dup_id in list_prob_dup:
		
		df_tmp = df_flt[df_flt["dup_group_id"] == prob_dup_id]
		
		list_rsp = []
		# Loop through records of probable duplicates
		for idx in range(len(df_tmp)):
			row = df_tmp.iloc[idx]
			rsp = {}
			rsp["RowNo"] = row["RowNo"]
			contact_name = str(row["FirstName"]).strip() + " " +  str(row["LastName"]).strip()
			team_info_dict = get_team_info(row["CompanyName"],team_info_dict)
			logger.info(f"company : {row['CompanyName']}, contact : {contact_name}")
			#logger.info(f"team info : {team_info_dict}")
			rsp = is_contact_available_in_team_info(rsp,contact_name,row["CompanyName"],team_info_dict)
			if rsp["contact_found"]:
				logger.info(f"dup_group_id : {prob_dup_id}, RowNo : {row['RowNo']}, Contact {contact_name} found " \
				" in company website.")
			else:
				logger.info(f"dup_group_id : {prob_dup_id}, RowNo : {row['RowNo']}, Contact {contact_name} not found " \
				" in company website.")
			list_rsp.append(rsp)

		# Assign Keep Duplicate Flag based on Contac found from Website		
		for rsp in list_rsp:
			df.loc[(df["dup_group_id"] == prob_dup_id) & (df["RowNo"] == rsp["RowNo"]), ["action","action_confidence","source"]] = [rsp["flag"],rsp["score"],rsp["source"]]

	logger.info("completed assignment of probable duplicate flag")
	return df
