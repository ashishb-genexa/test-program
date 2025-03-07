import src.util.helper as helper
import src.util.log_helper as log_helper
import src.settings.constants as const
import src.webscrapper.webscrapper as wsc
import src.duplicate_finder.company_duplicate_finder as cmp_dup_find
import src.duplicate_finder.duplicate_row_groups as dup_grps
import os
import time
import pandas as pd
import re
import src.data_enrichment.company_data as cmp_data_enrich


logger = log_helper.set_get_logger("company_deduplication",helper.get_logfile_name())

def merge_records(df:any) -> any:

	#if not os.path.exists(file_path):
			#logger.error(f"File {file_path} does not exists. Please check file path and try again.")
			#return
	#logger.info(f"Merging process started for project id : {project_id}")	

	#df = pd.read_excel(file_path)
	#df = df.fillna("")

	# Kishor Lakkad 18 Oct 2024
	# When UI implement Human review capability, Uncomment below line, So DF can be generated from DB.
	#df = pg_cmn.get_contact_dataframe_by_project_from_db(project_id, " and dup_group_id != 999999")

	# First Validation
	df_tmp = df[df["action"] == "To be reviewed"]
	group_ids = df_tmp["dup_group_id"].unique().tolist()
	if len(df_tmp) != 0:
		logger.info(f"These duplicate group {group_ids} has not been resolved yet. Please mark all groups with keep/duplicate falg.")
		return

	# Add new columns to dataframe
	new_col_list = ["Updated Company","Updated Website","Address1","Email1","Phone1"]
	for col in new_col_list:
		df[col] = ""

	df_flt = df[(df["dup_group_id"] != 999999) & (df["action"] == "Keep")]
	list_rows = []
	for id,row in df_flt.iterrows():
		list_rows.append(row["TV ID"])
		df.at[id,"Updated Website"] = helper.format_url(str(row["Website"]))
		if row["ext_name"] != const.NOT_FOUND:
			df.at[id,"Updated Company"] = row["ext_name"]
		if row["ext_address"] != const.NOT_FOUND:
			df.at[id,"Address1"] = row["ext_address"]
		if row["ext_phone"] != const.NOT_FOUND:
			df.at[id,"Phone1"] = row["ext_phone"]
		if row["ext_email"] != const.NOT_FOUND:
			df.at[id,"Email1"] = row["ext_email"]
	
	# Loop through again to update in DB

	#name, ext = helper.get_file_name_and_extension(file_path)
	#if const.SAVE_DF_TO_EXCEL:
		#file_out = const.OUTPUT_PATH + "/" +  name + "_with_merge"+ext
		#logger.info(f"Saving file with duplicate groups and Keep delete flag {file_out}")
		#helper.save_file(df,file_out,ext)

	logger.info(f"Merging process completed.")		
	return df



def clean_company_name(company_name):
    # Define keywords to be removed
    keywords = ['LLP', 'LLC', 'INC', 'PVT', 'LTD', 'CORP', 'CO']
    # Create a regex pattern to match any of the keywords
    pattern = r'\b(?:' + '|'.join(keywords) + r')\b\.?'
    # Substitute the keywords with an empty string
    cleaned_name = re.sub(pattern, '', company_name, flags=re.IGNORECASE)
    # Remove extra spaces
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip()
    return cleaned_name

def clean_url(url : str) -> str:
	if not url:
		return url
	url = helper.format_url(url)
	root_url = helper.get_root_url(url)
	if root_url == "://":
		return url
	cleaned_url = re.sub(r'^(https?://(www\.)?|www\.)', '', root_url)
	return cleaned_url

def find_and_update_company_website(df):
	for index, row in df.iterrows():
		if not row['Website']:  # Check if the website is empty
			rsp = wsc.get_company_website(str(row['Company']))
			if rsp != const.NOT_FOUND:
				df.at[index, 'Website'] = rsp
	return df

def read_file_prep_dataframe(file_path, file_ext, map_file_path):
	if file_ext.lower() == ".csv":
		df = pd.read_csv(file_path)
	else:
		df = pd.read_excel(file_path) 
	
	# Reading Mapping file prep dictionary
	df_map = pd.read_excel(map_file_path)
	if "SRC_COL" not in df_map.columns or "TRG_COL" not in df_map.columns:
		logger.error("Mapping file does not have 'SRC_COL' or 'TRG_COL'. Please correct file and re run.")
		return

	for id, row in df_map.iterrows():
		src_col = str(row["SRC_COL"]).strip()
		trg_col = str(row["TRG_COL"]).strip()
		if row["TRG_COL"] not in df.columns:
			logger.error(f"Target column {trg_col} does not exists into input file {file_path}. Please correct mapping ile and re run.")
			return
		# Add Column
		if trg_col.lower() == src_col.lower():
			df.rename(columns = {trg_col:trg_col+"_org"}, inplace = True)
			df[src_col] = df[trg_col+"_org"]
		else:
			df[src_col] = df[trg_col]	
		# Clean only SRC Columns
		df[src_col] = df[src_col].map(lambda x: helper.clean_contact_data(x) if isinstance(x, str) else x) 

	df = df.fillna("") # Fill Empty value as all columns are strings only
	
	# Clean text of all dataframe text data one time 11 Oct 2024 --> (Not Needed now.)
	#df = df.map(lambda x: helper.clean_contact_data(x) if isinstance(x, str) else x)
	df = find_and_update_company_website(df)

	df['Clean_Company']= df["Company"].apply(clean_company_name)
	df["Clean_Website"] = df["Website"].apply(clean_url)

	## Add New Columns to Add into original Dataframe
	df.insert(0,const.COL_DUP_GROUP_ID,999999)
	
	if const.COL_ID not in df.columns:
		# Add RowNum to DataFrame
		df.insert(1,const.COL_ID,range(0+1,len(df)+1))
	else:
		df.rename(columns = {const.COL_ID:const.COL_ID+"_org"}, inplace = True)
		df.insert(1,const.COL_ID,range(0+1,len(df)+1))	
	
	df.insert(2,const.COL_ERROR_RATE,100.00)
	df[const.COL_FUZZ_SIMILARITY] = 0.0
	df[const.COL_LEVENSHTEIN_SIMILARITY] = 0.0
	df[const.COL_JARO_SIMILARITY] = 0.0
	df[const.COL_DUP_ROW_GROUP] = ""
	# Add new columns required
	df["action"] = ""
	df["source"] = ""
	df["dup_group_type"] = ""
	
	df['FL_Comp_name'] = df['Company'].apply(lambda x: x[:2] if len(x) > 0 else '')
	df["ext_name"] = ""
	df["ext_address"] = ""
	df["ext_phone"] = ""
	df["ext_email"] = ""
	return df


def assign_probable_duplicate_flag(df : any):
	# Remove Exact Duplicate rows from Dataframe
	df_flt = df[df['dup_group_id'] != 999999]
	df_flt = df_flt[df_flt["dup_group_type"] != const.EXACT_DUPLICATE]
	# Sort by Dup Group ID
	df_flt = df_flt.sort_values(by=["dup_group_id"])

	for id, row in df_flt.iterrows():
		df.at[id,"source"] = row['Website']
		df.at[id,"dup_group_type"] = const.PROB_DUPLICATE
		website = str(row["Company"])
		if not website:
			if cmp_dup_find.is_company_name_exist_in_website(str(row['Website']),str(row["Company"])):
				df.at[id,"action"] = "Keep"
			else:
				df.at[id, 'action'] ="To be reviewed"
		else:
			df.at[id, 'action'] ="To be reviewed"
	
	logger.info("started getting company data enrichment fields using open AI")	
	for id, row in df_flt.iterrows():
		rsp = cmp_data_enrich.get_comapny_data(str(row['Website']),str(row["Company"]))
		if not rsp["is_success"]:
			logger.error(f"error occured during get company data. error : {rsp['error']}")
		df.loc[id, ['ext_name', 'ext_address', 'ext_phone', 'ext_email']] = [rsp["llm_company_name"], rsp["llm_address"], rsp["llm_email"], rsp["llm_phone"]]
		# df.at[id,'ext_name'] = rsp["llm_company_name"]

	return df

def assign_exact_duplicate_flag(df :any):
	logger.info("starting assignment of exact duplicate using website found..")
	df_flt = df[df["Clean_Website"] !=""]
	df_grp = df_flt.groupby(["Clean_Website"], as_index=False).agg(count=("dup_group_id", 'count'))
	df_grp = df_grp[df_grp["count"] > 1]
	df_grp['dup_group_id'] = df_grp.groupby('Clean_Website').ngroup()
	for id, row_dup in df_grp.iterrows():
		df_tmp = df[df["Clean_Website"] == row_dup["Clean_Website"]]
		prv_row = "fRow"
		for idx in range(len(df_tmp)):
			row = df_tmp.iloc[idx]
			if idx == 0:
				flag = "Keep"
			else:
				flag = "Duplicate"
			row_grp = prv_row + "-" + str(row["TV ID"])	
			prv_row = str(row["TV ID"])	
			df.loc[df["TV ID"] == row["TV ID"], \
				["action","source","dup_group_type","dup_group_id","fuzzy_similarity","levenshtein_similarity","jaro_winkler_similarity","row_group","error_rate"]] = \
      [flag,"same website",const.EXACT_DUPLICATE,int(row_dup["dup_group_id"])+1,1,1,1,row_grp,0]

	logger.info("completed assingment of exact duplicate using website found..")
	return df

def process_duplicate_resolution(df : any):
	logger.info("Started duplicate resolution.")

	# Assign Keep Delete flag to probable duplicate
	df = assign_probable_duplicate_flag(df)
	
	return df

def process_company_duplicator(file_path : str,map_file_path : str):
	logger.info("Started processing...")
	start_time = time.time()	
	try:
		
		if not os.path.exists(file_path):
			logger.error(f"File {file_path} does not exists. Please check file path and try again.")
			return

		if not os.path.exists(map_file_path):
			logger.error(f"Mapping file {map_file_path} does not exists. Please check file path and try again.")
			return
			
		name, ext = helper.get_file_name_and_extension(file_path)
		if not (ext.lower() == ".csv" or ext.lower() == ".xlsx"):
			logger.error("System support csv and xlsx file types only. Please check file extension and try again.")
			return 
		
		# Step 1
		logger.info("reading input file and preparing dataframe.")
		file_out = const.OUTPUT_PATH + "/" +  name + "_org"+ext
		if not os.path.exists(file_out):
			df = read_file_prep_dataframe(file_path,ext, map_file_path)
			logger.info(f" No of records {len(df)} found in file {file_path}")
			helper.save_file(df,file_out,ext)
		else:
			logger.info(f"original file {file_out} exists, it will use it")
			df = helper.load_file(file_out,ext)

		logger.info("Finding duplicate rows.")
		file_out = const.OUTPUT_PATH + "/" +  name + "_dup"+ext
		if not os.path.exists(file_out):
			#Step 1.5 Mark Exact Duplicate based on Website found
			#df = assign_exact_duplicate_flag(df)

			#Step 2
			df_dup = cmp_dup_find.find_dup_row_by_company_name(df)
			if len(df_dup) == 0:
				logger.info("There is no duplicate rows found..")
				return df
			else:
				logger.info(f"No Of Duplicate Records found : {len(df_dup)} ")

			#Step 3
			logger.info("Assigning group id to duplicate rows.")
			max_dup_grp_id = df[df["dup_group_id"] != 999999]["dup_group_id"].max()
			df = dup_grps.assign_dup_row_groups(df,df_dup)
			helper.save_file(df,file_out,ext)
		else:
			df = helper.load_file(file_out,ext)

		logger.info("duplicate identification " + helper.get_processing_time_in_seconds(start_time))

		#Step 4 Assing the Keep / Duplicate Flag to each row based on research.
		start_time = time.time()	
		file_out = const.OUTPUT_PATH + "/" +  name + "_with_flag"+ext
		#df = process_duplicate_resolution(df)
		logger.info(f"Saving file with duplicate groups and Keep delete flag {file_out}")
		helper.save_file(df,file_out,ext)
		

	except Exception as e:
		logger.exception(e)

	logger.info("duplicate resolution " + helper.get_processing_time_in_seconds(start_time))
	return df

