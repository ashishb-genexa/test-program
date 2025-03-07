import src.settings.constants as const
import pandas as pd
from rapidfuzz import fuzz
import Levenshtein
import jellyfish as jfish
import src.util.log_helper as log_helper
import src.util.helper as helper
import os
import src.webscrapper.webscrapper as wsc
import src.ner.ner_team_company_info as ner
import src.util.sqllite_helper as db_manager
import json
import time

logger = log_helper.set_get_logger("duplicate_finder",helper.get_logfile_name())

def save_file(df: any, file_path : str, ext:str):
	if ext.lower() == ".xlsx":
		df.to_excel(file_path,index=False)
	else:
		df.to_csv(file_path,index=False)

def load_file(file_path : str, ext:str):
	if ext.lower() == ".csv":
		df = pd.read_csv(file_path,encoding='cp1252')
	else:
		df = pd.read_excel(file_path) 
	return df

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

def assign_probable_duplicate_flag_v1(df : any, list_exact_dup : list[int]):
	logger.info("Started assignment of probable duplicate flag : version V1")
	
	# Remove Exact Duplicate rows from Dataframe
	df_flt = df[df['dup_group_id'] != 999999]
	for exact_dup_id in list_exact_dup:
		df_flt = df_flt[df_flt['dup_group_id'] != exact_dup_id]
	# Sort by Dup Group ID
	df_flt = df_flt.sort_values(by=["dup_group_id"])

	for id, row in df_flt.iterrows():
		
		search_str = str(row["FirstName"]) + " " + str(row["LastName"])
		if row["Email"].find("@") > -1:
			email_domain = helper.get_domain_from_email(str(row["Email"]))
		else:
			email_domain = str(row["CompanyName"])  
		rsp = wsc.search_contact_information(search_str,email_domain, str(row["CompanyName"]))
		if rsp["is_current_org"]:
			flag = "Keep"
			source_link = rsp["source_url"]
			if rsp["linked_in_profile_url"] != const.NOT_FOUND:
				source_link += "\n " + rsp["linked_in_profile_url"]
		else:
			flag = "Not Sure"
			source_link = const.NOT_FOUND	 
		
		df.loc[(df["dup_group_id"] == row["dup_group_id"]) & (df["RowNo"] == row["RowNo"]), \
				  ["action","source","new_title","new_phone","new_email","prv_org","prv_title","data_source"]] = \
					[flag,source_link,rsp["llm_curr_title"],rsp["llm_phone"],rsp["llm_email"],rsp["llm_prv_org"],rsp["llm_prv_title"], \
						rsp["data_source"]]
	
	df = run_keep_duplicate_flag_rules(df, df_flt)
	
	logger.info("completed assignment of probable duplicate flag")
	return df

def run_keep_duplicate_flag_rules(df : any, df_flt:any):
	dup_grp_ids = df_flt["dup_group_id"].unique().tolist()
	for dup_grp_id in dup_grp_ids:
		df_tmp = df[df["dup_group_id"] == dup_grp_id][["RowNo","Email","CompanyName","action","prv_org"]]
		is_keep_exists = "Keep" in df_tmp["action"].values
		if not is_keep_exists:
			continue
		for id, row in df_tmp.iterrows():
			flag ="NO_CHANGE"	
			if row["action"]  == "Not Sure":
				keep_email = df_tmp[df_tmp["action"] == "Keep"][["Email"]].values[0][0]
				keep_prv_org = df_tmp[df_tmp["action"] == "Keep"][["prv_org"]].values[0][0]
				if keep_email == str(row["Email"]).strip():
					flag = "Duplicate"
				elif ( keep_prv_org != "Not found"):
					sim_score = get_levenshtein_similarity(keep_prv_org,str(row["CompanyName"])) * 100
					logger.info(f'row no : {row["RowNo"]}, keep prv org : {keep_prv_org}, curr org : {row["CompanyName"]}, Similarity score : {sim_score}')
					if ( sim_score > 80.0 ):
						flag = "Duplicate"	
			# Update Row
			if flag != "NO_CHANGE":
				df.loc[(df["dup_group_id"] == dup_grp_id) & (df["RowNo"] == row["RowNo"]), ["action"]] = [flag]
			
			# If All records in duplicate groups are marked as Keep
			keep_count = 	int(df_tmp["action"].value_counts()["Keep"])
			if len(df_tmp) == keep_count:
				cnt = 0
				for id, row in df_tmp.iterrows():
					flag ="NO_CHANGE"	
					if cnt == 0:
						keep_email = df_tmp[df_tmp["action"] == "Keep"][["Email"]].values[0][0]
						keep_prv_org = df_tmp[df_tmp["action"] == "Keep"][["prv_org"]].values[0][0]				
					else:
						if keep_email == str(row["Email"]).strip():
							flag = "Duplicate"
						elif ( keep_prv_org != "Not found"):
							if ( keep_prv_org == str(row["prv_org"]).strip()):
								flag = "Duplicate"	
						# Update Row
						if flag != "NO_CHANGE":
							df.loc[(df["dup_group_id"] == dup_grp_id) & (df["RowNo"] == row["RowNo"]), ["action"]] = [flag]
					cnt += 1

	return df

def assign_probable_duplicate_flag(df : any, list_exact_dup : list[int] ):
	
	logger.info("Started assignment of probable duplicate flag")
	# Remove Exact Duplicate rows from Dataframe
	df_flt = df[df['dup_group_id'] != 999999]
	for exact_dup_id in list_exact_dup:
		df_flt = df_flt[df_flt['dup_group_id'] != exact_dup_id]
	list_prob_dup = df_flt["dup_group_id"].unique().tolist()
	list_company = df_flt["CompanyName"].unique().tolist()

	# Get and Insert website data in DB
	wsc.process_company_list_get_scrapped_data(list_company)

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
		contact_found_from_team_info = False
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
				contact_found_from_team_info = True
			else:
				logger.info(f"dup_group_id : {prob_dup_id}, RowNo : {row['RowNo']}, Contact {contact_name} not found " \
				" in company website.")
			list_rsp.append(rsp)

		# Assign Keep Duplicate Flag based on Contac found from Website		
		for rsp in list_rsp:
			df.loc[(df["dup_group_id"] == prob_dup_id) & (df["RowNo"] == rsp["RowNo"]), ["action","action_confidence","source"]] = [rsp["flag"],rsp["score"],rsp["source"]]

	logger.info("completed assignment of probable duplicate flag")
	return df


def assign_keep_duplicate_flag(df : any):
	logger.info("Started assigning Keep, Duplicate flags to exact Duplicates.")
	# Add new columns required
	df["action"] = ""
	df["source"] = ""
	df["new_title"] = ""
	df["new_phone"] = ""
	df["new_email"] = ""
	df["prv_org"] = ""
	df["prv_title"] = ""
	df["data_source"] = ""
	# Assign Exact Duplicate
	df, list_exact_dup = assign_exact_duplicate_flag(df)
	# Assign Keep Delete flag to probable duplicate
	df = assign_probable_duplicate_flag_v1(df,list_exact_dup)
	return df

def assign_exact_duplicate_flag(df : any):
	df_flt = df[df['dup_group_id'] != 999999]
	df_grp = df_flt.groupby(["dup_group_id","FirstName","LastName","Email"], as_index=False).agg(count=("dup_group_id", 'count'))
	list_exact_dup = df_grp[df_grp["count"] > 1]["dup_group_id"].unique().tolist()
	logger.info(f"Total exact duplicate found : {len(list_exact_dup)}")
	for exact_dup_id in list_exact_dup:
		df_tmp = df[df["dup_group_id"] == exact_dup_id]
		df_tmp = df_tmp.sort_values(by=["FirstName","LastName"], ascending=False)		
		for idx in range(len(df_tmp)):
			row = df_tmp.iloc[idx]
			if idx == 0:
				flag = "Keep"
			else:
				flag = "Duplicate"	
			df.loc[(df["dup_group_id"] == exact_dup_id) & (df["RowNo"] == row["RowNo"]), ["action","source"]] = [flag,"Exact Duplicate"]
	
	return df, list_exact_dup

def process_contact_duplicator(file_path : str):
	start_time = time.time()	
	try:
		if not os.path.exists(file_path):
			logger.error(f"File {file_path} does not exists. Please check file path and try again.")
			return

		name, ext = helper.get_file_name_and_extension(file_path)
		logger.info(f"input file name : {name}{ext}")
		if not (ext.lower() == ".csv" or ext.lower() == ".xlsx"):
			logger.error("System support csv and xlsx file types only. Please check file extension and try again.")
			return 

		# Step 1
		file_out = const.OUTPUT_PATH + "/" +  name + "_org"+ext
		if not os.path.exists(file_out):
			logger.info(f"Started processing file {file_path}")
			df = read_file_prep_dataframe(file_path,ext)
			logger.info(f"No Of Records {len(df)} found in file {file_path}")

			logger.info(f"Saving original file {file_out}")
			save_file(df,file_out,ext)
		else:
			logger.info(f"original file {file_out} exists, It will use it")
			df = load_file(file_out,ext)
			#return df

		file_out = const.OUTPUT_PATH + "/" +  name + "_dup"+ext
		if not os.path.exists(file_out):
			#Step 2
			logger.info("Started finding probable and exact duplicate records")
			df_dup = find_dup_row_by_fname_lname(df)
			logger.info(f"No Of Duplicate Records found : {len(df_dup)} ")

			#Step 3
			logger.info("Assigning duplicate group id to duplicate rows")
			df = assign_dup_row_groups(df,df_dup)
			df = df.fillna("")
			logger.info(f"Saving file with duplicate groups file {file_out}")
			save_file(df,file_out,ext)
		else:
			logger.info(f"duplicate group file {file_out} exists, It will use it.")
			df = load_file(file_out,ext)
	
		# #Step 4 Assing the Keep / Duplicate Flag to each row based on research.
		# df = assign_keep_duplicate_flag(df)

		# file_out = const.OUTPUT_PATH + "/" +  name + "_with_flag"+ext
		# logger.info(f"Saving file with duplicate groups and Keep delete flaf {file_out}")
		# save_file(df,file_out,ext)
	except Exception as e:
		logger.exception(e)
	logger.info(helper.get_processing_time_in_seconds(start_time))

def find_group(row, row_to_group, group_id):
	if row in row_to_group:
			return row_to_group[row]
	row_to_group[row] = group_id
	return group_id

def union_groups(row1, row2, row_to_group, group_id):
	group1 = find_group(row1, row_to_group, group_id)
	group2 = find_group(row2, row_to_group, group_id)
	if group1 != group2:
			for row in row_to_group:
					if row_to_group[row] == group2:
							row_to_group[row] = group1

def assign_group_ids(pairs):
	row_to_group = {}
	group_id = 1

	for src_row, trg_row in pairs:
			if src_row not in row_to_group and trg_row not in row_to_group:
					row_to_group[src_row] = group_id
					row_to_group[trg_row] = group_id
					group_id += 1
			elif src_row in row_to_group and trg_row not in row_to_group:
					row_to_group[trg_row] = row_to_group[src_row]
			elif trg_row in row_to_group and src_row not in row_to_group:
					row_to_group[src_row] = row_to_group[trg_row]
			else:
					union_groups(src_row, trg_row, row_to_group, group_id)

	# Ensure each component has a unique group ID
	unique_groups = {}
	current_group_id = 1
	for row in row_to_group:
			old_group_id = row_to_group[row]
			if old_group_id not in unique_groups:
					unique_groups[old_group_id] = current_group_id
					current_group_id += 1
			row_to_group[row] = unique_groups[old_group_id]

	return row_to_group


def read_file_prep_dataframe(file_path, file_ext):
	if file_ext.lower() == ".csv":
		df = pd.read_csv(file_path,encoding='cp1252')
	else:
		df = pd.read_excel(file_path) 

	df = df.fillna("") # Fill Empty value as all columns are strings only
		#df.to_excel("./output/crm_contact_with_row_no.xlsx")

	# Clean text of all dataframe text data one time
	df = df.map(lambda x: helper.clean_contact_data(x) if isinstance(x, str) else x)

	## Add New Columns to Add into original Dataframe
	df.insert(0,const.COL_DUP_GROUP_ID,999999)
	if const.COL_ID not in df.columns:
		# Add RowNum to DataFrame
		df[const.COL_ID] = range(0+1,len(df)+1)
	df.insert(2,const.COL_ERROR_RATE,100.00)
	df[const.COL_FUZZ_SIMILARITY] = 0.0
	df[const.COL_LEVENSHTEIN_SIMILARITY] = 0.0
	df[const.COL_JARO_SIMILARITY] = 0.0
	df[const.COL_DUP_ROW_GROUP] = ""

	# Inserting new column from First Name, take first letter of first name 
	df['fname'] = df["FirstName"].apply(lambda x: x[0] if len(x) > 0 else '')
	#df['lname'] = df["LastName"].apply(lambda x: x[0] if len(x) > 0 else '')
	df['lname'] = df["LastName"].apply(lambda x: x[:2] if isinstance(x, str) else '')

	# Inserting new column from First Name, take first letter of first name 
	df['part_email'] = df["Email"].apply(lambda x: x.split('@')[0])
	
	return df

def assign_dup_row_groups(df,dup_df):
	dup_df = dup_df.sort_values(by=["src_row_no"])		

	# This will give Group ID assign to each row	
	group_ids = assign_group_ids(list(dup_df[['src_row_no', 'trg_row_no']].itertuples(index=False,name=None)))
	
	# In this Loop If any two record qualify or three records, One Record data is missing.
	for src_idx in range(len(dup_df)):
		row = dup_df.iloc[src_idx]
		
		if df[df[const.COL_ID] == row[const.COL_SOURCE_ID]][const.COL_DUP_GROUP_ID].values[0] == 999999:
			df.loc[df[const.COL_ID] == row[const.COL_SOURCE_ID], [const.COL_FUZZ_SIMILARITY, const.COL_LEVENSHTEIN_SIMILARITY, \
					const.COL_JARO_SIMILARITY,const.COL_ERROR_RATE,const.COL_DUP_GROUP_ID,const.COL_DUP_ROW_GROUP]] \
					= [row[const.COL_FUZZ_SIMILARITY],row[const.COL_LEVENSHTEIN_SIMILARITY],row[const.COL_JARO_SIMILARITY], \
					row[const.COL_ERROR_RATE],group_ids[row[const.COL_SOURCE_ID]],str(row[const.COL_SOURCE_ID])+"-"+str(row[const.COL_TARGET_ID])]
		else:
			df.loc[df[const.COL_ID] == row[const.COL_TARGET_ID], [const.COL_FUZZ_SIMILARITY, const.COL_LEVENSHTEIN_SIMILARITY, \
					const.COL_JARO_SIMILARITY,const.COL_ERROR_RATE,const.COL_DUP_GROUP_ID,const.COL_DUP_ROW_GROUP]] \
					= [row[const.COL_FUZZ_SIMILARITY],row[const.COL_LEVENSHTEIN_SIMILARITY],row[const.COL_JARO_SIMILARITY], \
					row[const.COL_ERROR_RATE],group_ids[row[const.COL_TARGET_ID]],str(row[const.COL_SOURCE_ID])+"-"+str(row[const.COL_TARGET_ID])]
	
	#Loop Throuh again with Dup Record for any Source Or Target Row is missing For Group ID Update
	for src_idx in range(len(dup_df)):
		row = dup_df.iloc[src_idx]
		grp_id = df[df[const.COL_ID] == row[const.COL_SOURCE_ID]][const.COL_DUP_GROUP_ID].values[0]
		if grp_id != 999999:
			df.loc[df[const.COL_ID] == row[const.COL_TARGET_ID], [const.COL_FUZZ_SIMILARITY, const.COL_LEVENSHTEIN_SIMILARITY, \
					const.COL_JARO_SIMILARITY,const.COL_ERROR_RATE,const.COL_DUP_GROUP_ID,const.COL_DUP_ROW_GROUP]] \
					= [row[const.COL_FUZZ_SIMILARITY],row[const.COL_LEVENSHTEIN_SIMILARITY],row[const.COL_JARO_SIMILARITY], \
					row[const.COL_ERROR_RATE],grp_id,str(row[const.COL_SOURCE_ID])+"-"+str(row[const.COL_TARGET_ID])]
		else:
			grp_id = df[df[const.COL_ID] == row[const.COL_TARGET_ID]][const.COL_DUP_GROUP_ID].values[0]
			df.loc[df[const.COL_ID] == row[const.COL_SOURCE_ID], [const.COL_FUZZ_SIMILARITY, const.COL_LEVENSHTEIN_SIMILARITY, \
					const.COL_JARO_SIMILARITY,const.COL_ERROR_RATE,const.COL_DUP_GROUP_ID,const.COL_DUP_ROW_GROUP]] \
					= [row[const.COL_FUZZ_SIMILARITY],row[const.COL_LEVENSHTEIN_SIMILARITY],row[const.COL_JARO_SIMILARITY], \
					row[const.COL_ERROR_RATE],grp_id,str(row[const.COL_SOURCE_ID])+"-"+str(row[const.COL_TARGET_ID])]

	return df

def get_col_value(row : any,col_list : list) -> str:
	val_list = []
	for col in col_list:
		val_list.append(str(row[col]).strip())
	return " ".join(val_list)

def compare_rows(src_row,trg_row):
	"""
		Based On Compare Columns, Calculate Levenstine, Jaro Winkle and Fuzzy raito and select
		where Error rate lowest or similarity ratio is higher
	"""
	comparesion_list = []
	id = 0
	min_error = 100.0
	min_error_id = -1
	for comp_columns in const.COMPARE_COLUMNS:
		str_src = get_col_value(src_row,comp_columns['src_cols'])
		str_trg = get_col_value(trg_row,comp_columns['trg_cols'])
		res = {}
		res["name"] = comp_columns["name"]
		res["f" + str(id)] = get_fuzzy_similarity(str_src,str_trg) 
		res["l" + str(id)] = get_levenshtein_similarity(str_src,str_trg)
		res["j" + str(id)] = get_jaro_winkler_similarity(str_src,str_trg)
		#logger.info(f"src val : {str_src}, trg val : {str_trg}, fuzz : {res['f' + str(id)] }, levst : {res['l' + str(id)]}, jero : {res['j' + str(id)]}")
		res["e" + str(id)] =  (1.0-(float(res["f" + str(id)]) + float(res["l" + str(id)]) + float(res["j" + str(id)]) )/3)*100
		if ( float(res["e" + str(id)]) < min_error):
			min_error = res["e" + str(id)]
			min_error_id = id
		comparesion_list.append(res)
		id += 1	
	#logger.info(f"min error rate : {min_error}, min error id : {min_error_id}")	
	return min_error_id, comparesion_list[min_error_id]

def find_dup_row_by_sequence(df,sort_col):
	df = df.sort_values(by=[sort_col])
	list_dup_rows = []
	# Now Iterate through Each record and try to find out Group
	for src_idx in range(len(df)):
		src_row = df.iloc[src_idx]
		for trg_idx in range(len(df)):
			trg_row = df.iloc[trg_idx]
			#logger.info(f"source index {src_idx} ,target index : {trg_idx}")
			if trg_idx > src_idx:
				dup_grp = {}
				dup_grp[const.COL_SOURCE_ID] = src_row[const.COL_ID]
				dup_grp[const.COL_TARGET_ID] = trg_row[const.COL_ID]
				dup_grp["src_lastname"] = str(src_row["LastName"])
				dup_grp["trg_lastname"] = str(trg_row["LastName"])
				id, rsp = compare_rows(src_row,trg_row)
				#logger.info(id, rsp)
				dup_grp[const.COL_FUZZ_SIMILARITY] = float(rsp["f" + str(id)])
				dup_grp[const.COL_LEVENSHTEIN_SIMILARITY] = float(rsp["l" + str(id)])
				dup_grp[const.COL_JARO_SIMILARITY] = float(rsp["j" + str(id)])
				dup_grp[const.COL_ERROR_RATE] = float(rsp["e" + str(id)])
				if dup_grp[const.COL_ERROR_RATE] <= const.MAX_ERROR_RATE:
					list_dup_rows.append(dup_grp)
	return list_dup_rows
	
def find_dup_row_by_fname(df,col_first_name="FirstName"):
	list_dup_rows = []
	df_dup = df.groupby(["fname"], as_index=False).agg(count=("fname", 'count'))
	df_dup = df_dup[df_dup["count"] > 1]
	for id, row in df_dup.iterrows():
		df_flt = df[df['fname'] == row['fname']]
		df_flt = df_flt.sort_values(by=[col_first_name])
		logger.info(f"Started processing records start with {row['fname']}.")
		list_dup_rows.extend(find_dup_row_by_sequence(df=df_flt,sort_col=col_first_name))
	logger.info("Completed processing all records.}.")
	
	# # Now Iterate through dup Rows and If last name matching Error rate is higher than threshold,
	# # Remove those rows from List.
	# list_dup_rows = remove_dup_rows_by_lastname_match_similarity(list_dup_rows)
	
	df_dup_rows = pd.DataFrame.from_dict(list_dup_rows)
	return df_dup_rows

def remove_dup_rows_by_lastname_match_similarity(list_dup_rows : list):
	logger.info(f"started removing dup rows by lastname similarity. Total row groups : {len(list_dup_rows)}")
	list_items_to_del = []	
	for idx, dup_row in enumerate(list_dup_rows):
		s1 = get_fuzzy_similarity(dup_row["src_lastname"],dup_row["trg_lastname"])
		s2 = get_levenshtein_similarity(dup_row["src_lastname"],dup_row["trg_lastname"])
		s3 = get_jaro_winkler_similarity(dup_row["src_lastname"],dup_row["trg_lastname"])
		er = (1.00-((s1+s2+s3)/3))*100
		if (er >= const.MAX_ERROR_RATE):
			list_items_to_del.append(dup_row)
		logger.debug(f"src lname : {dup_row['src_lastname']}, trg lname : {dup_row['trg_lastname']}, error rate : {er}, fuz : {s1}, lvs : {s2}, jaro : {s3}")	
	logger.info(f"Total {len(list_items_to_del)} dup rows found to be removed.")	
	# Remove rows from list dup rows
	for item in list_items_to_del:
		list_dup_rows.remove(item)		
	logger.info(f"completed removing dup rows by lastname similarity. Total row groups : {len(list_dup_rows)}")
	return list_dup_rows

def find_dup_row_by_fname_lname(df,col_first_name="FirstName"):
	list_dup_rows = []
	# Group by Fname first.
	df_dup = df.groupby(["fname"], as_index=False).agg(count=("fname", 'count'))
	df_dup = df_dup[df_dup["count"] > 1]
	for id, row in df_dup.iterrows():
		df_flt = df[df['fname'] == row['fname']]
		df_flt = df_flt.sort_values(by=[col_first_name])
		
		logger.info(f"Started processing records start with {row['fname']}.")
		# Group By lname
		df_dup_lname = df_flt.groupby(["lname"], as_index=False).agg(count=("lname", 'count'))
		df_dup_lname = df_dup_lname[df_dup_lname["count"] > 1]

		# Now Process through each lname group Records
		for id1, row in df_dup_lname.iterrows():
			df_flt_lname = df_flt[df_flt['lname'] == row['lname']]
			df_flt_lname = df_flt_lname.sort_values(by=["LastName"])
			list_dup_rows.extend(find_dup_row_by_sequence(df=df_flt_lname,sort_col="LastName"))

	logger.info("Completed processing all records.")
	df_dup_rows = pd.DataFrame.from_dict(list_dup_rows)
	return df_dup_rows

def convert_row_to_string(src_row,trg_row, compare_columns):
	str1_row = []
	str2_row = []
	for col in compare_columns["compare_cols"]:
		str1_row.append(src_row[col])
		str2_row.append(trg_row[col])
	return " ".join(str1_row), " ".join(str2_row)

def get_fuzzy_similarity(str_src,str_trg):
	if  (len(str_src) != 0 and len(str_trg) != 0):
		return (fuzz.token_set_ratio(str_src, str_trg)/100)
	else:
		return 0.0	

def get_levenshtein_similarity(str_src,str_trg):
	"""
	Levenshtein distance measures the minimum number of single-character edits required to change one string into another.
	"""
	if  (len(str_src) != 0 and len(str_trg) != 0):
		return Levenshtein.ratio(str_src, str_trg)
	else:
		return 0.0	

def get_jaro_winkler_similarity(str_src,str_trg):
	if  (len(str_src) != 0 and len(str_trg) != 0):
		return jfish.jaro_winkler_similarity(str_src, str_trg)
	else:
		return 0.0	
