import src.settings.constants as const
import src.util.log_helper as log_helper
import src.util.helper as helper
import src.duplicate_finder.contact_duplicate_finder as dup_finder
import src.duplicate_finder.probable_duplicate_resolution_google_search as dup_res
import src.duplicate_finder.duplicate_row_groups as dup_grps
import pandas as pd
import os
import time
import re
import unicodedata
import datetime

logger = log_helper.set_get_logger("contact_deduplication",helper.get_logfile_name())


def merge_records(df: any) -> any:
    # First Validation
    df_tmp = df[df["action"] == "Not Sure"]
    group_ids = df_tmp["dup_group_id"].unique().tolist()
    if len(df_tmp) != 0:
        logger.info(f"These duplicate group {group_ids} has not been resolved yet. Please mark all groups with keep/duplicate flag.")
        return

    df_flt = df[df["dup_group_id"] != 999999]
    dup_grp_ids = df_flt["dup_group_id"].unique().tolist()

    list_rsp = []
    rsp = {}
    for grp_id in dup_grp_ids:
        org_count, email_count, phone_count, desg_count = 0, 0, 0, 0
        df_tmp1 = df_flt[(df_flt["dup_group_id"] == grp_id) & (df_flt["action"] == "Keep")]
        
        if df_tmp1.empty:
            continue
        
        keep_row = df_tmp1.iloc[0]
        if keep_row["source"] == "exact_duplicate":
            continue
        
        if keep_row["prv_org"] != const.NOT_FOUND:
            if str(keep_row["CompanyName"]).strip() != str(keep_row["prv_org"]).strip():
                org_count += 1
                rsp = {}
                rsp["TV ID"] = keep_row["TV ID"]
                rsp["col_name"] = "CompanyName" + str(org_count)
                rsp["col_val"] = str(keep_row["prv_org"]).strip()
                list_rsp.append(rsp)
        
        if keep_row["prv_title"] != const.NOT_FOUND:
            if str(keep_row["Designation"]).strip() != str(keep_row["prv_title"]).strip():
                desg_count += 1
                rsp = {}
                rsp["TV ID"] = keep_row["TV ID"]
                rsp["col_name"] = "Designation" + str(desg_count)
                rsp["col_val"] = str(keep_row["prv_title"]).strip()
                list_rsp.append(rsp)
        
        if keep_row["new_email"] != const.NOT_FOUND:
            if str(keep_row["Email"]).strip() != str(keep_row["new_email"]).strip():
                email_count += 1
                rsp = {}
                rsp["TV ID"] = keep_row["TV ID"]
                rsp["col_name"] = "Email" + str(email_count)
                rsp["col_val"] = str(keep_row["new_email"]).strip()
                list_rsp.append(rsp)
        
        df_tmp2 = df_flt[(df_flt["dup_group_id"] == grp_id) & (df_flt["action"] != "Keep")]
        for id, row in df_tmp2.iterrows():
            if str(row["Email"]).strip() != str(keep_row["Email"]).strip():
                email_count += 1
                rsp = {}
                rsp["TV ID"] = keep_row["TV ID"]
                rsp["col_name"] = "Email" + str(email_count)
                rsp["col_val"] = str(row["Email"]).strip()
                list_rsp.append(rsp)
            
            if str(row["Designation"]).strip() != str(keep_row["Designation"]).strip():
                desg_count += 1
                rsp = {}
                rsp["TV ID"] = keep_row["TV ID"]
                rsp["col_name"] = "Designation" + str(desg_count)
                rsp["col_val"] = str(row["Designation"]).strip()
                list_rsp.append(rsp)
            
            if str(row["CompanyName"]).strip() != str(keep_row["CompanyName"]).strip():
                org_count += 1
                rsp = {}
                rsp["TV ID"] = keep_row["TV ID"]
                rsp["col_name"] = "CompanyName" + str(org_count)
                rsp["col_val"] = str(row["CompanyName"]).strip()
                list_rsp.append(rsp)
    
    for rsp in list_rsp:
        if rsp["col_name"] not in df.columns:
            df[rsp["col_name"]] = ""
        df.loc[df["TV ID"] == rsp["TV ID"], [rsp["col_name"]]] = [rsp["col_val"]]
    
    return df

def extract_name_from_email(email):
    patterns = [
        r"^([a-zA-Z])\.([a-zA-Z]+)@",  # First letter of first name . last name
        r"^([a-zA-Z]+)\.([a-zA-Z]+)@",  # First name . last name
        r"^([a-zA-Z]+)_([a-zA-Z]+)@",  # First name _ last name
        r"^([a-zA-Z]+)-([a-zA-Z]+)@",  # First name - last name
        r"^([a-zA-Z]+)-([a-zA-Z]+)@",  # Last name - first name
    ]
    
    for pattern in patterns:
        match = re.match(pattern, email)
        if match:
            return match.groups() if len(match.groups()) == 2 else (match.group(1), "")
    
    return None, None

def extract_names(row):
    fname = row['FirstName'].lower() if pd.notna(row['FirstName']) else ""
    lname = row['LastName'].lower() if pd.notna(row['LastName']) else ""
    email = row['Email'].lower()
    
    extracted_first, extracted_last = extract_name_from_email(email)
    
    if extracted_first and extracted_last:
        if pd.isna(row['FirstName']) or len(row['FirstName']) < 2:
            row['FirstName'] = extracted_first
        if pd.isna(row['LastName']) or len(row['LastName']) < 2:
            row['LastName'] = extracted_last
    else:
        if len(lname) == 1 and '@' in email:
            extracted_name = email.split('@')[0]
            if len(extracted_name) > 1 and len(fname) > 0 and extracted_name[0] == fname[0] and len(extracted_name) > len(lname) and extracted_name[1:].startswith(lname):
                row['LastName'] = extracted_name[1:]
    
    return row
def read_file_prep_dataframe(file_path, file_ext, map_file_path):
	if file_ext.lower() == ".csv":
		df = pd.read_csv(file_path,encoding='cp1252')
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
			logger.error(f"Target column {trg_col} does not exists into input file {file_path}. Please correct mapping file and re run.")
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

	# Clean text of all dataframe text data one time
	#df = df.map(lambda x: helper.clean_contact_data(x) if isinstance(x, str) else x)
	# Add Data Standardization for First Name, Last Name and Email
    
	for index, row in df.iterrows():
		first_name, last_name, email = row['FirstName'], row['LastName'], row['Email']
		
		if pd.isna(first_name) or pd.isna(last_name) or len(str(first_name)) < 2 or len(str(last_name)) < 2:
			extracted_first, extracted_last = extract_name_from_email(email)
			
			if extracted_first and extracted_last:
				if isinstance(first_name, str) and first_name and first_name[0].lower() == extracted_last[0].lower():
					if len(str(df.at[index, 'FirstName'])) <= len(extracted_last):
						df.at[index, 'FirstName'] = extracted_last
					if len(str(df.at[index, 'LastName'])) <= len(extracted_last):
						df.at[index, 'LastName'] = extracted_first
				elif isinstance(last_name, str) and last_name and last_name[0].lower() == extracted_first[0].lower():
					if len(str(df.at[index, 'FirstName'])) <= len(extracted_first):
						df.at[index, 'FirstName'] = extracted_first
					if len(str(df.at[index, 'LastName'])) < len(extracted_first):
						df.at[index, 'LastName'] = extracted_last
				else:
					if pd.isna(first_name) or len(str(first_name)) <= 2:
						df.at[index, 'FirstName'] = extracted_first
					if pd.isna(last_name) or len(str(last_name)) <= 2:
						df.at[index, 'LastName'] = extracted_last
			else:
				df.loc[index] = extract_names(row)
	## Add New Columns to Add into original Dataframe
	
	if const.COL_ID not in df.columns:
		# Add RowNum to DataFrame
		df.insert(0,const.COL_ID,range(0+1,len(df)+1))
	else:
		df.rename(columns = {const.COL_ID:const.COL_ID+"_org"}, inplace = True)
		df.insert(0,const.COL_ID,range(0+1,len(df)+1))

	# df.insert(2,const.COL_ERROR_RATE,100.00)
	# df[const.COL_FUZZ_SIMILARITY] = 0.0
	# df[const.COL_LEVENSHTEIN_SIMILARITY] = 0.0
	# df[const.COL_JARO_SIMILARITY] = 0.0
	# df[const.COL_DUP_ROW_GROUP] = ""

	# Inserting new column from First Name, take first letter of first name 
	# df['fname'] = df["FirstName"].apply(lambda x: x[0] if len(x) > 0 else '')
	# #df['lname'] = df["LastName"].apply(lambda x: x[0] if len(x) > 0 else '')
	# df['lname'] = df["LastName"].apply(lambda x: x[:2] if isinstance(x, str) else '')

	# # Inserting new column from First Name, take first letter of first name 
	# df['part_email'] = df["Email"].apply(lambda x: x.split('@')[0])


	# Add new columns required
	df.insert(len(df.columns),const.COL_DUP_GROUP_ID,999999)
	df['GroupID']=0
	df['dup_group_id'] =0
	df['IsDuplicate']=None
	df["dup_group_type"] = ""	
	df["data_source"] = ""
	df["action"] = ""
	df["source"] = ""
	df["new_title"] = ""
	df["new_phone"] = ""
	df["new_email"] = ""
	df["prv_org"] = ""
	df["prv_title"] = ""

	return df

def assign_exact_duplicate_flag(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df['dup_group_id'] != 999999, 'dup_group_type'] = 'probable_duplicate'
    df_flt = df[df['dup_group_id'] != 999999]  # Exclude non-duplicate records

    # Identify groups with duplicate FirstName, LastName, and Email
    df_grp = df_flt.groupby(["dup_group_id", "FirstName", "LastName", "Email"], as_index=False).size()
    list_exact_dup = df_grp[df_grp["size"] > 1]["dup_group_id"].unique().tolist()
    df["new_group_id"] = None  # Initialize new group id column
    df["action"] = None  # Initialize action column

    for exact_dup_id in list_exact_dup:
        df_tmp = df[df["dup_group_id"] == exact_dup_id].copy()  # Create a temporary DataFrame for the group

        # Identify exact duplicate subsets
        df_tmp["dup_subset_id"] = df_tmp.groupby(["FirstName", "LastName", "Email"]).ngroup()

        for subset_id in df_tmp["dup_subset_id"].unique():
            exact_dup_records = df_tmp[df_tmp["dup_subset_id"] == subset_id]
            if len(exact_dup_records) < 2:
                continue  # Skip if no exact duplicates in this subset

            # Code to select the record with the most non-null values as "Keep"
            non_null_counts = exact_dup_records.notnull().sum(axis=1)
            keep_index = non_null_counts.idxmax()  # Find the index of the record with the highest non-null count

            # Mark this initial one as "Keep"
            df.loc[keep_index, ["action", "dup_group_type", "data_source", "new_group_id"]] = [
                "Keep", "EXACT_DUPLICATE", "Same Firstname, Lastname & Email", exact_dup_id
            ]

            # Compare each remaining record with the one marked as "Keep"
            for idx in exact_dup_records.index:
                if idx != keep_index:
                    # Check if the FirstName, LastName, and Email match the Keep record
                    if (df.at[idx, "FirstName"] == df.at[keep_index, "FirstName"] and
                        df.at[idx, "LastName"] == df.at[keep_index, "LastName"] and
                        df.at[idx, "Email"] == df.at[keep_index, "Email"]):
                        
                        # Mark as "Delete"
                        df.loc[idx, ["action", "dup_group_type", "data_source", "new_group_id"]] = [
                            "Delete", "EXACT_DUPLICATE", "Same Firstname, Lastname & Email", exact_dup_id
                        ]

    return df

def process_contact_deduplication(file_path : str,map_file_path : str ):
	start_time = time.time()
	logger.info(f">>>>>> Stage- file Upload started <<<<<<")	
	try:
		if not os.path.exists(file_path):
			logger.error(f"File {file_path} does not exists. Please check file path and try again.")
			return

		if not os.path.exists(map_file_path):
			logger.error(f"Mapping file {map_file_path} does not exists. Please check file path and try again.")
			return
		
		name, ext = helper.get_file_name_and_extension(file_path)
		logger.info(f"input file name : {name}{ext}")
		if not (ext.lower() == ".csv" or ext.lower() == ".xlsx"):
			logger.error("System support csv and xlsx file types only. Please check file extension and try again.")
			return 

		# Step 1
		timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
		file_out = const.OUTPUT_PATH + "/" +  name +"_"+timestamp+ "_org"+ext
		if not os.path.exists(file_out):
			logger.info(f"Started processing file {file_path}")
			df = read_file_prep_dataframe(file_path,ext, map_file_path)
			logger.info(f"No Of Records {len(df)} found in file {file_path}")
			#return df
			logger.info(f">>>>> Stage- Saving original file {file_out} <<<<<")
			logger.info(">"*75)
			helper.save_file(df,file_out,ext)
		else:
			logger.info(f">>>>> original file {file_out} exists, It will use Same Original File <<<<<")
			logger.info(">"*75)
			df = helper.load_file(file_out,ext)

		file_out = const.OUTPUT_PATH + "/" +  name + "_"+timestamp+"_dup"+ext
		if not os.path.exists(file_out):
			#Step 2
			logger.info(">>>>> Stage- Started duplicate identification <<<<<")
			logger.info(f">>>>> Number of records {len(df)} <<<<<")
			df.sort_values(by='LastName', inplace=True)
			logger.info(" >>>>> Stage- Find Duplicate Records By last name <<<<<")
			df_dup_l_name = dup_grps.group_by_last_name(df)
			
			logger.info(" >>>>> Stage- Find Duplicate Records By First Name <<<<<")
			df_dup_f_name = dup_grps.refine_by_first_name(df_dup_l_name)
			#Step 3
			logger.info(">>>>> Stage- Assigning duplicate group id to duplicate rows <<<<<")
			df_groups = dup_grps.assign_ids(df_dup_f_name)
			df = pd.DataFrame(df_groups)
			df = df.fillna("")
			df= assign_exact_duplicate_flag(df)
			logger.info(f">>>>> Saving file with duplicate groups file {file_out} <<<<<")
			helper.save_file(df,file_out,ext)
		else:
			logger.info(f">>>>> duplicate group file {file_out} exists, It will use the same File. <<<<<")
			df = helper.load_file(file_out,ext)
	
		logger.info(">>>>> duplicate identification " + helper.get_processing_time_in_seconds(start_time))
		start_time = time.time()	

		#Step 4 Assing the Keep / Duplicate Flag to each row based on research.
		
		# df = process_duplicate_resolution(df)
		# file_out = const.OUTPUT_PATH + "/" +  name + "_with_flag"+ext
		# logger.info(f" >>>>> Saving file with duplicate groups and Keep delete with flag {file_out} <<<<<")
		# helper.save_file(df,file_out,ext)

	except Exception as e:
		logger.exception(e)
	logger.info("duplicate resolution " + helper.get_processing_time_in_seconds(start_time))
	
	return df

def process_duplicate_resolution(df : any,):
	logger.info(">>>>> Stage- Started duplicate resolution. <<<<<")
	# Assign Keep Delete flag to probable duplicate
	df = dup_res.assign_probable_duplicate_flag(df)
	
	return df

