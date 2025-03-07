import src.settings.constants as const
import src.util.similarity_helper as sim_helper
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
# for contact information
def group_by_last_name(df, threshold=0.85):
    groups = []
    for index, row in df.iterrows():
        last_name = str(row['LastName'])
        if not last_name:  # Check if last_name is empty
            continue  # Ensure last_name is a string
        found_group = False
        for group in groups:
            # Check if the first character of the last name is the same
            if last_name[0] == str(group[0]['LastName'])[0]:
                if sim_helper.is_similar(last_name, str(group[0]['LastName']), threshold):
                    group.append(row)
                    found_group = True
                    break
        if not found_group:
            groups.append([row])
    return groups

def refine_by_first_name(groups, threshold=0.65):
    refined_groups = []
    for group in groups:
        refined_group = []
        single_letter_names = []
        empty_first_names = []  # To store contacts with empty first names
        
        for contact in group:
            first_name = str(contact.get('FirstName', ''))  # Ensure first_name is a string
            if not first_name:
                empty_first_names.append(contact)
                continue
            if len(first_name) == 1:
                single_letter_names.append(contact)
                continue
            
            found_subgroup = False
            for subgroup in refined_group:
                if first_name[0] == str(subgroup[0]['FirstName'])[0]:
                    if sim_helper.is_similar(first_name, str(subgroup[0]['FirstName']), threshold):
                        subgroup.append(contact)
                        found_subgroup = True
                        break
            if not found_subgroup:
                refined_group.append([contact])
        
        # Integrate single-letter names into the appropriate subgroups
        for contact in single_letter_names:
            first_name = str(contact.get('FirstName', ''))
            found_subgroup = False
            for subgroup in refined_group:
                if first_name[0] == str(subgroup[0]['FirstName'])[0]:
                    subgroup.append(contact)
                    found_subgroup = True
                    break
            if not found_subgroup:
                refined_group.append([contact])
        
        # Add the empty first name contacts as a separate subgroup
        if empty_first_names:
            refined_group.append(empty_first_names)
        
        refined_groups.append(refined_group)
    
    return refined_groups




def assign_ids(refined_groups):
    group_id = 1
    subgroup_id = 1  # Initialize a global counter for SubgroupID
    result = []
    for group in refined_groups:
        for subgroup in group:
            is_duplicate = len(subgroup) > 1
            for contact in subgroup:
                if is_duplicate:
                    contact['GroupID'] = group_id
                    contact['dup_group_id'] = subgroup_id
                else:
                    contact['GroupID'] = group_id
                    contact['dup_group_id'] = 999999  # No subgroup ID for unique records
                contact['IsDuplicate'] = is_duplicate
                result.append(contact)
            if is_duplicate:
                subgroup_id += 1  # Increment the global SubgroupID counter only for duplicates
        group_id += 1
    return result

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
