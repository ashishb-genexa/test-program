import src.settings.constants as const
import src.util.log_helper as log_helper
import src.util.helper as helper
import src.util.similarity_helper as simi_helper
import pandas as pd

logger = log_helper.set_get_logger("duplicate_finder",helper.get_logfile_name())

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
		res["f" + str(id)] = simi_helper.get_fuzzy_similarity(str_src,str_trg) 
		res["l" + str(id)] = simi_helper.get_levenshtein_similarity(str_src,str_trg)
		res["j" + str(id)] = simi_helper.get_jaro_winkler_similarity(str_src,str_trg)
		#logger.info(f"src val : {str_src}, trg val : {str_trg}, fuzz : {res['f' + str(id)] }, levst : {res['l' + str(id)]}, jero : {res['j' + str(id)]}")
		res["e" + str(id)] =  (1.0-(float(res["f" + str(id)]) + float(res["l" + str(id)]) + float(res["j" + str(id)]) )/3)*100
		#res["e" + str(id)] =  100-((float(res["f" + str(id)])*0.31 + float(res["l" + str(id)])*0.31 + float(res["j" + str(id)])*0.38 )*100)

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
				id, rsp = compare_rows(src_row,trg_row)
				#logger.info(id, rsp)
				dup_grp[const.COL_FUZZ_SIMILARITY] = float(rsp["f" + str(id)])
				dup_grp[const.COL_LEVENSHTEIN_SIMILARITY] = float(rsp["l" + str(id)])
				dup_grp[const.COL_JARO_SIMILARITY] = float(rsp["j" + str(id)])
				dup_grp[const.COL_ERROR_RATE] = float(rsp["e" + str(id)])
				if dup_grp[const.COL_ERROR_RATE] <= const.MAX_ERROR_RATE:
					list_dup_rows.append(dup_grp)
	return list_dup_rows

def find_dup_row_by_fname_lname(df,col_first_name="FirstName"):
	list_dup_rows = []
	# Group by Fname first.
	df = df.sort_values(by='fname')
	df_dup = df.groupby(["fname"], as_index=False).agg(count=("fname", 'count'))
	df_dup = df_dup[df_dup["count"] > 1]
	for id, row in df_dup.iterrows():
		df_flt = df[df['fname'] == row['fname']]
		df_flt = df_flt.sort_values(by=[col_first_name])
		
		logger.info(f"Started processing records start with First Name -- '{row['fname']}'")
		# Group By lname
		df_dup_lname = df_flt.groupby(["lname"], as_index=False).agg(count=("lname", 'count'))
		df_dup_lname = df_dup_lname[df_dup_lname["count"] > 1]

		# Now Process through each lname group Records
		for id1, row in df_dup_lname.iterrows():
			df_flt_lname = df_flt[df_flt['lname'] == row['lname']]
			df_flt_lname = df_flt_lname.sort_values(by=["LastName"])
			list_dup_rows.extend(find_dup_row_by_sequence(df=df_flt_lname,sort_col="LastName"))

	logger.info(">>>>> Completed processing all records. <<<<<")
	logger.info(">"*55)
	df_dup_rows = pd.DataFrame.from_dict(list_dup_rows)
	return df_dup_rows

