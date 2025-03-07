import src.settings.constants as const
import src.util.log_helper as log_helper
import src.util.helper as helper
import src.webscrapper.webscrapper as wsc
import src.util.similarity_helper as simi_helper

logger = log_helper.set_get_logger("duplicate_resolution_google_search",helper.get_logfile_name())

def assign_probable_duplicate_flag(df : any):
	logger.info(">>>>> Stage- Started assignment of probable duplicate flag : version V1 <<<<<")
	
	# Remove Exact Duplicate rows from Dataframe
	df_flt = df[(df['dup_group_id'] != 999999)& (df['action']!= "Delete") ]
	# Sort by Dup Group ID
	df_flt = df_flt.sort_values(by=["dup_group_id"])

	for id, row in df_flt.iterrows():
		
		search_str = str(row["FirstName"]) + " " + str(row["LastName"])
		if str(row["Email"]).find("@") > -1:
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
			flag = "to be reviewed"
			source_link = const.NOT_FOUND	 
		
		df.loc[(df["TV ID"] == row["TV ID"]), \
				  ["action","source","new_title","new_phone","new_email","prv_org","prv_title","data_source","new_group_id"]] = \
					[flag,source_link,rsp["llm_curr_title"],rsp["llm_phone"],rsp["llm_email"],rsp["llm_prv_org"],rsp["llm_prv_title"], \
						rsp["data_source"],str(row["dup_group_id"])]
	
	#df = run_keep_duplicate_flag_rules(df, df_flt)
	return df

def run_keep_duplicate_flag_rules(df : any, df_flt:any):
	dup_grp_ids = df_flt["dup_group_id"].unique().tolist()
	for dup_grp_id in dup_grp_ids:
		df_tmp = df[df["dup_group_id"] == dup_grp_id][["TV ID","Email","CompanyName","action","prv_org"]]
		is_keep_exists = "Keep" in df_tmp["action"].values
		if not is_keep_exists:
			continue
		for id, row in df_tmp.iterrows():
			flag ="NO_CHANGE"	
			if row["action"]  == "to be reviewed":
				keep_email = df_tmp[df_tmp["action"] == "Keep"][["Email"]].values[0][0]
				keep_prv_org = df_tmp[df_tmp["action"] == "Keep"][["prv_org"]].values[0][0]
				if keep_email == str(row["Email"]).strip():
					flag = "Delete"
				elif ( keep_prv_org != "Not found"):
					sim_score = simi_helper.get_levenshtein_similarity(keep_prv_org,str(row["CompanyName"])) * 100
					logger.info(f'row no : {row["TV ID"]}, keep prv org : {keep_prv_org}, curr org : {row["CompanyName"]}, Similarity score : {sim_score}')
					if ( sim_score > 80.0 ):
						flag = "Delete"	
			# Update Row
			if flag != "NO_CHANGE":
				df.loc[(df["dup_group_id"] == dup_grp_id) & (df["TV ID"] == row["TV ID"]), ["action"]] = [flag]
			
			# If All records in duplicate groups are marked as Keep
			try:
				keep_count = 	int(df_tmp["action"].value_counts()["Keep"])
			except Exception:
				keep_count = 0			
			if len(df_tmp) == keep_count:
				cnt = 0
				for id, row in df_tmp.iterrows():
					flag ="NO_CHANGE"	
					if cnt == 0:
						keep_email = df_tmp[df_tmp["action"] == "Keep"][["Email"]].values[0][0]
						keep_prv_org = df_tmp[df_tmp["action"] == "Keep"][["prv_org"]].values[0][0]				
					else:
						if keep_email == str(row["Email"]).strip():
							flag = "Delete"
						elif ( keep_prv_org != "Not found"):
							if ( keep_prv_org == str(row["prv_org"]).strip()):
								flag = "Delete"	
						# Update Row
						if flag != "NO_CHANGE":
							df.loc[(df["dup_group_id"] == dup_grp_id) & (df["TV ID"] == row["TV ID"]), ["action"]] = [flag]
					cnt += 1

	return df
