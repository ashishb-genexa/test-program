import src.util.log_helper as log_helper
import src.settings.constants as const
import src.util.helper as helper
import src.util.sqllite_helper as db_manager
import src.util.api_helper as api_helper
import src.util.proxycurl as pcurl
from bs4 import BeautifulSoup
from bs4.element import Comment
from rapidfuzz import fuzz
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
import pandas as pd
import re
import time
import io
import json
import random
import requests
from src.util.similarity_helper import get_fuzzy_similarity

# This is Create the Logger Instance 
logger = log_helper.set_get_logger("WebScrapper",helper.get_logfile_name())

def qualify_company_urls(urls : list[str], search_list : list[str]) -> str:
	# qualified_urls = []
	clean_urls = [re.sub(r'^https?://(www\.)?', '', url).strip() for url in urls]
	
	for clean_url, url in zip(clean_urls, urls):
		if any([clean_url.startswith(word+".com") for word in search_list]):
			return url	

	for clean_url, url in zip(clean_urls, urls):
		if any([clean_url.startswith(word) for word in search_list]):
			return url	

	for url in urls:
		if any([word in url for word in search_list]):
			return url

	return const.NOT_FOUND


def validate_website_url(url_found:str):

	rsp = const.NOT_FOUND
	if helper.search_using_regex_pattern(url_found,const.WEBSITE_URL_INVALID_PATTERN) != const.NOT_FOUND:
		return rsp
	if helper.search_using_regex_pattern(url_found,const.DO_NOT_GET_ROOT_URL_PATTERN) != const.NOT_FOUND:
		rsp = url_found
	else:
		rsp = helper.get_root_url(url_found)	

	# for el in url_found.split("/"):
	# 	el = str(el).strip().lower()  
	# 	if  el.endswith(".com") or el.endswith(".org") or el.endswith(".gov") or el.endswith(".info") or el.endswith("edu") \
	# 		or el.endswith(".gov.uk") or el.endswith(".si") or el.endswith(".us") or el.endswith(".hk") or el.endswith(".io") \
	# 		or el.endswith(".net") or el.endswith(".br") :
	# 		cmp_domain = el
	# 		web_list.append(el)
	# 		break
	# 	else:
	# 		web_list.append(el)    

	# # Company domain name should not start with this.
	# #print("company domain name : ",cmp_domain)
	# is_cmp_domain_contains_invalid_domain = False
	# for dmn in const.INVALID_COMPANY_DOMAINS:
	# 	if re.search(dmn,cmp_domain,re.IGNORECASE):
	# 		is_cmp_domain_contains_invalid_domain = True
	# 		break
	# if not is_cmp_domain_contains_invalid_domain:
	# 	rsp  = "/".join(web_list) 
	return rsp

def is_scraping_allowed_by_robots_file(url, robots_text, user_agent='*'):
	# Initialize and parse the robots.txt
	rp = RobotFileParser()
	rp.parse(robots_text.splitlines())
	# Check if the website allows scraping for the given user agent
	return rp.can_fetch(user_agent, url)

def get_website_robot_file(website_url : str, company_name :str):
	headers = {'User-Agent': random.choice(const.USER_AGENTS)}
	robot_file = ""
	str_sql = "Select robot_file  from company_master Where website = ? COLLATE NOCASE"
	robot_file = db_manager.select_scaler(str_sql,[website_url])
	if robot_file != const.NOT_EXISTS:
		logger.info(f'Company- {company_name}, and its robot file already exists in db.')
		return robot_file

	# As Robot file does not exists in DB, Download from Website and Insert into DB
	try:
		logger.info(f' Visting website to Check  robot.txt file for {website_url}')
		with requests.get(website_url + "/robots.txt",headers=headers) as r:
			r.raise_for_status()
			file_content =  io.StringIO(r.text)
			for line in file_content:
				if robot_file == const.NOT_EXISTS:
					robot_file = line
				else:
					robot_file += "\n" + line	
	except Exception as e:
		logger.exception(e)
		robot_file = const.NOT_FOUND
		logger.info(f'robot.txt file Not Found for {website_url}')

	# Now Insert Robot file and other content into DB
	str_sql = "Insert OR REPLACE into company_master ( company_name, website,robot_file) Values(?,?,?)"
	db_manager.execute_sql(str_sql,[company_name,website_url,robot_file])

	return robot_file

def is_website_exists_in_company_master(website_url):
	str_sql = "Select robot_file  from company_master Where website = ? COLLATE NOCASE"
	rsp = db_manager.select_scaler(str_sql,[website_url])
	if rsp == "Not Found":
		return False		
	else:
		return True

def clean_text(str_input : str):
	#Use for if there are multiple space are present then convert in single space 
	str_input=re.sub(r'\s+', ' ', str_input)
	#Remove non-printable ASCII characters
	str_input=re.sub(r'[^\x20-\x7E]', '', str_input)
	return str_input.strip().lower()

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body):
	#body = clean_text(body)
	soup = BeautifulSoup(body, 'html.parser')
	texts = soup.findAll(text=True)
	visible_texts = filter(tag_visible, texts)  
	return u" ".join(t.strip() for t in visible_texts)

def url_from_html(body):
	soup = BeautifulSoup(body, 'html.parser')
	a_tags = soup.find_all('a')
	# Extract the URLs from the 'href' attribute of each 'a' tag
	urls = [a['href'] for a in a_tags if 'href' in a.attrs]
	return urls

def get_company_website_and_legal(company_name :str) -> dict:
	rsp = {}
	rsp["is_success"] = False
	rsp["company_website"] = const.NOT_FOUND
	rsp["company_legal_name"] = const.NOT_FOUND
	rsp["msg"] = ""

	# Clean Company name with any Spacial character.
	search_str_company = re.sub(r'\b(Inc\.|LLC|Ltd\.|Corp\.|Corporation|Co\.|Limited|LLP|L\.P\.|PLC|GmbH|S\.A\.|Pty\. Ltd\.|BV)\b', '', company_name, flags=re.IGNORECASE).strip().lower()
	search_str_company = re.sub(r'[^a-zA-Z0-9\s]', '', search_str_company)
	logger.info(f"Company Name used for matching : {search_str_company}")

	api_rsp = get_google_api_resposne_search_items("legal name of company '" + company_name + "'")
	if not api_rsp["is_success"]:
		rsp["msg"] = api_rsp["data"]
		return rsp

	for item in api_rsp['data']:
		title = item["title"].strip().lower()
		if item.get('snippet') is not None:
			snippet = item['snippet'].strip().lower()
		logger.info(item['link'])
		logger.info(f"title : {title} and snippet : {snippet}")
	
		# Find ompany Url from title.
		if rsp["company_website"] == const.NOT_FOUND:
			if ( search_str_company in title or get_fuzzy_similarity(search_str_company,title)*100 >= const.FUZZY_RATIO_THRESHOLD ) \
					and  "linkedin" not in title:
				rsp["company_website"] = helper.get_root_url(item['link'])
		if rsp["company_website"] == const.NOT_FOUND and snippet:
			if search_str_company in snippet:
				rsp["company_website"] = helper.extract_url_from_text(snippet)
		# Now Company Legal Name from title or snippet
		if rsp["company_legal_name"] == const.NOT_FOUND:
			rsp["company_legal_name"] = helper.search_using_regex_pattern(title, const.COMPANY_LEGAL_NAME_PATTERN) 
		if rsp["company_legal_name"] == const.NOT_FOUND and snippet:
			rsp["company_legal_name"] = helper.search_using_regex_pattern(snippet, const.COMPANY_LEGAL_NAME_PATTERN) 

		# If Both value found, return
		if rsp["company_legal_name"] != const.NOT_FOUND and rsp["company_website"] != const.NOT_FOUND:
			break

	return rsp

def get_company_website(company_name :str) -> str:

	ret_url=const.NOT_FOUND
	cname_org = company_name

	# Clean Company name with any Spacial character.
	#company_name = re.sub(r'\b(LLC|INC|company|co|ltd)\b', '', company_name, flags=re.IGNORECASE).strip().lower()
	company_name = re.sub(r'\b(Inc\.|LLC|Ltd\.|Corp\.|Corporation|Co\.|Limited|LLP|L\.P\.|PLC|GmbH|S\.A\.|Pty\. Ltd\.|BV|consulting)\b', '', company_name, flags=re.IGNORECASE).strip().lower()
	company_name = re.sub(r'[^a-zA-Z0-9\s]', '', company_name)
	# Preserve & into company name
	cname_search = re.sub(r'\b(Inc\.|LLC|Ltd\.|Corp\.|Corporation|Co\.|Limited|LLP|L\.P\.|PLC|GmbH|S\.A\.|Pty\. Ltd\.|BV|consulting)\b', '', cname_org, flags=re.IGNORECASE).strip().lower()
	cname_search = re.sub(r'[^a-zA-Z0-9&\s]', '', cname_search)
	logger.info(f"Company Name used for searching : {cname_search}")

	api_rsp = get_google_api_resposne_search_items(company_name+ "+website+")
	if not api_rsp["is_success"]:
		return ret_url

	url_found = set()
	for i in range(len(api_rsp['data'])):
		title = api_rsp["data"][i]["title"].strip().lower()
		#logger.info(f"title : {title}")
		if ( cname_search in title or get_fuzzy_similarity(cname_search,title)*100 >= const.FUZZY_RATIO_THRESHOLD ):
			url_found.add(api_rsp["data"][i]['link'])

		if api_rsp['data'][i].get('snippet') is not None:
			snippet = api_rsp['data'][i]['snippet'].strip().lower()
			#logger.info(f"snippet : {snippet}")
			if cname_search in snippet:
				url_found.add(api_rsp["data"][i]['link'])

	ret_url = qualify_company_urls(list(url_found),cname_search.split(" "))

	# Get the Root URL	
	if helper.search_using_regex_pattern(ret_url,const.DO_NOT_GET_ROOT_URL_PATTERN) == const.NOT_FOUND:
		ret_url = helper.get_root_url(ret_url)	

	logger.info(f"For company {company_name} found website {ret_url}.")

	return ret_url


def validate_page_urls(root_url, page_urls):
	"""
	If page url start with root URL will keep and rest of URL can be ignored.
	Remove Duplicate URL from root page. 
	"""
	if ( root_url.endswith("/")):
		root_url = root_url[:-1]

	url_set = set()
	for p_url in page_urls:
		if str(p_url).startswith("/"):
			url_set.add(root_url+p_url)
		elif str(p_url).startswith(root_url):
			url_set.add(p_url) 
		elif str(p_url).startswith(root_url.replace("www.","")):
			url_set.add(p_url)
	return list(url_set)

def get_contact_urls(list_urls):
	url_set = set()
	for url in list_urls:
		if url == "nan":
			continue
		parsed_url = urlparse(url)
		last_section = parsed_url.path.strip('/').split('/')[-1]
		for sub_url in const.PAGE_CHILD_URLS:
			if sub_url in str(last_section).strip().lower():
				url_set.add(url)
				break

	return list(url_set)

def get_url_type(url:str)->str :
	rsp = "root"
	for sub_url in const.PAGE_CHILD_URLS:
		if str(url).strip().lower().find(sub_url) > -1:
			rsp = sub_url
	return rsp

#TODO Remove this code
# def get_website_content_for_contact(company_name,url,robot_file):
# 	list_rsp = []

# 	rsp = {}
# 	rsp["is_success"] = True 
# 	rsp["company_name"] = company_name
# 	rsp["page_url"] = url
# 	rsp["url_type"] = "root"
# 	rsp["page_urls"] = None

# 	web_url = str(url).strip().lower()
# 	if (not web_url.startswith("https://")) and ( not web_url.startswith("http://")):
# 		web_url = "https://" + web_url

# 	# Step 1 Get all Text Contect from home page and ALL relevent URLS
# 	logger.info(f"Request started for : {web_url}")
# 	try:
# 		headers = {'User-Agent': const.USER_AGENTS[0]}
# 		with requests.get(web_url,headers=headers, timeout=const.REQUEST_TIME_OUT,verify=False) as r:
# 			r.raise_for_status()
# 			rsp["page_text"] = text_from_html(r.text) 
# 			page_urls = url_from_html(r.text)
# 			#logger.info(f"Home Page All Urls : {page_urls}")
# 			rsp["page_urls"] = ",".join(validate_page_urls(web_url,page_urls))
# 	except Exception as e:
# 		rsp["is_success"] = False
# 		rsp["page_text"] = e 
# 		logger.error(f"error occured : {e}")
# 	list_rsp.append(rsp)

# 	# Now Get Content Of child URL
# 	list_urls =  str(rsp["page_urls"]).strip().lower().split(",")
# 	#logger.info(f"Compnay URLs : {list_urls}")
	
# 	# Filter Child URL with Contat, About US, Leadership etc..
# 	list_urls = get_contact_urls(list_urls)
# 	logger.info(f"Filtered Link for contact : {list_urls}")

# 	# get URL types
# 	list_url_types = []
# 	for url in list_urls:
# 		list_url_types.append(get_url_type(url))
# 	logger.info(f"url types : {list_url_types}")

# 	rsp = {}
# 	time.sleep(const.SLEEP_TIME_BETWEEN_REQUEST)
# 	for child_url,url_type in zip(list_urls,list_url_types):
# 		if robot_file == "NONE":
# 			is_url_allowed_to_scrap = True
# 		else:
# 			is_url_allowed_to_scrap = is_scraping_allowed_by_robots_file(child_url,robot_file)

# 		if not is_url_allowed_to_scrap:
# 			logger.info(f"url {child_url} is not allowed scrap by robot.txt file.")
# 			continue

# 		page_rsp = get_page_content(child_url)
# 		rsp = {}
# 		rsp["is_success"] = page_rsp["is_success"]
# 		rsp["company_name"] = company_name
# 		rsp["page_url"] = child_url
# 		rsp["url_type"] = url_type
# 		rsp["page_text"] = page_rsp["page_text"]
# 		rsp["page_urls"] = ""
# 		list_rsp.append(rsp)
# 		time.sleep(const.SLEEP_TIME_BETWEEN_REQUEST)
# 	return list_rsp

def get_page_content(company_name:str,page_url:str, robot_file ):
	"""
	This Function will go through Website specific page Find page text content.
	"""
	rsp = {}
	rsp["is_success"] = False

	str_sql = "SELECT page_text from company_pages where page_url= ? and company_name=?"
	res = db_manager.select_scaler(str_sql,[page_url,company_name])
	if res != const.NOT_EXISTS:
		rsp["page_text"]= res
		rsp["is_success"] = True
		return rsp

	if robot_file == const.NOT_FOUND:
		is_url_allowed_to_scrap = True
	else:
		is_url_allowed_to_scrap = is_scraping_allowed_by_robots_file(page_url,robot_file)

	if not is_url_allowed_to_scrap:
		logger.info(f"url {page_url} is not allowed scrap by robot.txt file.")
		rsp["page_text"] = "SCRAPPING_NOT_ALLOWED"	
		return rsp
	
	logger.info(f"Request started for : {page_url}")
	try:
		headers = {'User-Agent': random.choice(const.USER_AGENTS)}
		with requests.get(page_url,headers=headers, timeout=const.REQUEST_TIME_OUT) as r:
			r.raise_for_status()
			rsp["page_text"] = helper.clean_scrapped_text(text_from_html(r.text)) 
			rsp["is_success"] = True
	except Exception as e:
		rsp["page_text"] = "ERROR" 
		rsp["is_success"] = False
		logger.error(f"error occured : {e}")
	#logger.info(f"Request Completed for : {page_url}")
	if rsp["is_success"]:
		url_type = get_url_type(page_url)
		str_sql="INSERT INTO company_pages (company_name, page_url, page_text,url_type) VALUES (?, ?, ?,?)"
		db_manager.execute_sql(str_sql,[company_name,page_url,rsp["page_text"],url_type])		
	return rsp

#TODO Need to retest with new Google API method
def get_linkedin_profile(search_info :str) -> str:
	# Clean Company name with any Spacial character.
	search_info = search_info.strip().lower()
	ret_url = "Not Found"	
	rsp = {} #get_google_api_resposne_json(search_info)
	logger.info(f"goggle api response : {rsp}")
	validation = rsp['data']['searchInformation']

	if validation["totalResults"] == '0':
		raise Exception("Please provide parameter value for search..")
	
	search_items = rsp["data"]["items"]
	for item in search_items:
		
		print("item",item)
		if search_info in item["title"].strip().lower():
			ret_url = item["link"]
			break
		
	if not ret_url:
		if ret_url.endswith("/"):
			ret_url = ret_url[:-1]
	
	return ret_url

# #TODO Can be moved to new File.
# def get_website_content_from_csv_file(file_path,save_output_file=True):
	
# 	#df = pd.read_csv(file_path,encoding='cp1252')  it show Error becases we Encoding the Files Soulation is remove
# 	try:
# 		df = pd.read_csv(file_path)
# 	except UnicodeDecodeError:
# 		df= pd.read_csv(file_path,encoding='cp1252')
	
# 	try:
# 		list_req = df.to_dict('records')
# 		list_rsp = []
		
# 		for req in list_req:
# 			list_rsp.extend(get_website_content_for_contact(req["CompanyName"],req["Website"]))
# 			time.sleep(const.SLEEP_TIME_BETWEEN_REQUEST)

# 		if save_output_file:
# 			df_out = pd.DataFrame.from_dict(list_rsp)
# 			file_name = helper.get_file_name_and_extension(file_path)[0]
# 			df_out.to_excel("./output/" + file_name + "_output.xlsx", index=False)

# 	except Exception as e:
# 		logger.error(f"error occured : {e}")


def get_google_api_resposne_search_items(para :str):
	rsp = {}
	headers = {'User-Agent': random.choice(const.USER_AGENTS)}
	str_sql = "SELECT result FROM api_responses WHERE api_name=? AND input_para=?"
	res = db_manager.select_scaler(str_sql,["google_search_api",para])
	if res != const.NOT_EXISTS:
		logger.info(f"google_search_ap and para {para} exists in db")
		rsp["is_success"] = True       		
		rsp["data"] = json.loads(res)
		return rsp

	url = const.BASE_URL_GOOGLE_API + para
	
	# This will retry 3 times in case of status code 429. 
	retry_delay = 3 # Second
	
	for re_try in range(3):
		try:
			with requests.get(url,headers=headers) as r:
				r.raise_for_status()
				rsp["api_rsp"] = r.json()
				#logger.info(f"google api response Website : {r.json()}")
				if str(rsp["api_rsp"]["searchInformation"]["totalResults"]) == "0":
					rsp["data"] = "ERROR_NO_SEARCH_RESULT"
					Last_name = para.split(" ")[-1]
					logger.info(f"google api response Not found for this Result:{Last_name}")
					rsp["is_success"] = False       		
				else:
					rsp["is_success"] = True       		
					rsp["data"] = rsp["api_rsp"]["items"]		
		except Exception as e:
			logger.exception(e)
			if r.status_code == 429:
				logger.info(f"Retry {re_try}, Status code 429 received. Waiting for {retry_delay} seconds and then try again.")
				rsp["data"] = "ERROR_STATUS_CODE_429"
				rsp["is_success"] = False 
				time.sleep(retry_delay)
				retry_delay = retry_delay + 3		
			else:	
				rsp["data"] = f"ERROR_STATUS_CODE_{r.status_code}"
				rsp["is_success"] = False 
	
	#Insert Result into Database
	if rsp["is_success"]:
		str_sql="INSERT INTO api_responses (api_name, input_para, result) VALUES (?, ?, ?)"
		db_manager.execute_sql(str_sql,["google_search_api", para, json.dumps(rsp["data"])])
	
	
	return rsp


def is_valid_team_url(url:str, search_str:str):
	rsp = False
	
	ret = helper.search_using_regex_pattern(url,const.TEAM_URL_INVALID_PATTERN)
	#print("ret of team invalid valid pattern : ", ret)
	if ret != const.NOT_FOUND:
		return rsp

	ret = helper.search_using_regex_pattern(url, const.TEAM_URL_VALID_PATTERN)
	#print("ret of team valid pattern : ", ret)
	if ret != const.NOT_FOUND:
		rsp = True
	else: # As It is not valid Team URL, Validate with contact name present in URL.
		#print("started for search str")
		for name in search_str.split(" "):
			if url.find(name) > -1:
				rsp = True
				break

	return rsp   

def is_url_start_with_domain(url:str,domain:str):
	rsp = False
	pattern = r'^(https?://(www\.)?|www\.)'
	new_url = re.sub(pattern, '', url)
	rsp = new_url.startswith(domain.replace(".com",""))
	return rsp

def search_contact_information(full_name : str, email_domain : str, company_name : str) -> dict:
	rsp = {}
	rsp["is_current_org"] = False
	rsp["company_name"] = company_name
	rsp["llm_curr_org"]	= const.NOT_FOUND
	rsp["llm_curr_title"] = const.NOT_FOUND
	rsp["llm_prv_org"]	= const.NOT_FOUND
	rsp["llm_prv_title"] = const.NOT_FOUND
	rsp["llm_email"] = const.NOT_FOUND
	rsp["llm_phone"] = const.NOT_FOUND
	rsp["source_url"] = const.NOT_FOUND
	rsp["linked_in_profile_url"]  = const.NOT_FOUND
	rsp["team_url"] = const.NOT_FOUND
	rsp["website"] = const.NOT_FOUND
	rsp["data_source"] = ""
	rsp["msg"] = ""	
	logger.info("-"*55)
	logger.info(f"Started searching for contact {full_name} and Email domain {email_domain}")

	str_sql = "Select company_website,company_team_url,contact_bio_url, linked_in_profile_url from contact_company" \
						" where full_name = ? and company_name = ? LIMIT 1"
	db_rsp = db_manager.select_sql(str_sql,[full_name,company_name])
	if len(db_rsp) == 0:
		api_rsp = get_google_api_resposne_search_items(full_name + " " + email_domain+ " " + company_name)
		if not api_rsp["is_success"]:
			rsp["msg"] = api_rsp["data"]
			return rsp
		link=[item["link"] for item in api_rsp['data'] if "link" in item]
		logger.info(f"List of unfilter URL >>>> {link}")
		logger.info("-"*75)
		rsp = find_url_for_contact_current_org(api_rsp["data"],full_name,email_domain,rsp)
	else:
		logger.info(f"contact : {full_name}, company {company_name} google api serach info found from db.")
		if db_rsp[0][0] == const.NOT_FOUND:
			return rsp
		else:	
			rsp["is_current_org"] = True
			rsp["website"] = db_rsp[0][0]
			rsp["team_url"] = db_rsp[0][1]
			rsp["source_url"] = db_rsp[0][2]
			rsp["linked_in_profile_url"] = db_rsp[0][0]

	# If Is_current_Org = True, Now Get Robot File, and Validate that found URL is scrable or not. 
	if rsp["is_current_org"]:
		rsp = get_contact_bio_from_company_website(rsp)
		if rsp["is_contact_info_found"]:
			rsp["data_source"] = "website"
		if (not rsp["is_contact_info_found"]) and (rsp["linked_in_profile_url"] != const.NOT_FOUND):
			
			# Call Proxy Curl Linked in Profile. 
			rsp = get_linked_profile_by_proxy_curl(rsp)
			if rsp["is_contact_info_found"]:
				rsp["data_source"] = "linkedin"
		if 	(not rsp["is_contact_info_found"]):
			rsp = get_contact_info_using_third_party_db(rsp)
			if rsp["is_contact_info_found"]:
				rsp["data_source"] = "ExtDb"
	else:
		# Find link for contact, If they moved to new orgnization
		rsp = find_url_for_contact_moved_to_new_org(api_rsp["data"],full_name,email_domain,rsp)

	if len(db_rsp) == 0:
		str_sql = "Insert Into contact_company ( full_name,company_name, company_website,company_team_url,contact_bio_url," \
			 				 " linked_in_profile_url)  Values(?,?,?,?,?,?)"
		db_manager.execute_sql(str_sql,[full_name,company_name,rsp["website"],rsp["team_url"],rsp["source_url"],rsp["linked_in_profile_url"]])

	return rsp

def get_contact_info_using_third_party_db(rsp):
	logger.info("method get_contact_info_using_third_party_db is not implmented yet.")
	return rsp

def get_linked_profile_by_proxy_curl(rsp):
	logger.info("get_linked_profile_by_proxy_curl : code is commented out. PLease uncomment to include it.")
	return rsp
	# pcurl_rsp = pcurl.get_linkedin_profile(rsp["linked_in_profile_url"])
	# if pcurl_rsp["is_success"]:
	# 	cnt = 0
	# 	for exp in pcurl_rsp["data"]["experiences"]:
	# 		if cnt == 0:
	# 			rsp["llm_curr_org"]	= exp["company"]
	# 			rsp["llm_curr_title"] = exp["title"]
	# 		if cnt == 1:
	# 			rsp["llm_prv_org"]	= exp["company"]
	# 			rsp["llm_prv_title"] = exp["title"]
	# 			break
	# 		cnt += 1
	# 	rsp["is_contact_info_found"] = True
	# else:
	# 	rsp["is_contact_info_found"] = False	
	# return rsp

def get_contact_bio_from_company_website(rsp):
	
	str_sql = "select extracted_data from company_pages where company_name = ? and page_url = ?"
	contact_info = db_manager.select_scaler(str_sql,[rsp["company_name"],rsp["source_url"]])
	if (contact_info != const.NOT_EXISTS) and (contact_info != const.NONE):
		logger.info(f"company : {rsp['company_name']}, url : {rsp['source_url']} contact information {contact_info} found in db.")
		if contact_info == const.NOT_FOUND:
			rsp["is_contact_info_found"] = False
		else:
			rsp["is_contact_info_found"] = True
			contact_info = contact_info.replace("'",'"')
			openAi_rsp = json.loads(contact_info)	
			rsp["llm_curr_org"]	= openAi_rsp["curr_org"]
			rsp["llm_curr_title"] = openAi_rsp["curr_title"]
			rsp["llm_prv_org"]	= openAi_rsp["prv_org"]
			rsp["llm_prv_title"] = openAi_rsp["prv_title"]
			rsp["llm_email"] = openAi_rsp["email"]
			rsp["llm_phone"] = openAi_rsp["phone"]
		return rsp
	
	if contact_info == const.NONE:
		str_sql = "delete from company_pages where company_name = ? and page_url = ?"		
		db_manager.execute_sql(str_sql,[rsp["company_name"],rsp["source_url"]])
		
	# Get Website Robot.txt file to check for scrapping allowed or not.
	robot_file = get_website_robot_file(rsp["website"],rsp['company_name'])

	# Get Website page content for NER
	
	page_rsp = get_page_content(rsp['company_name'],rsp["source_url"],robot_file)
	if page_rsp["is_success"] and len(page_rsp["page_text"])>0:
		# Get Details about contact using LLM
		logger.info(">>>>> Started searchin using Open AI <<<<<")
		openAi_rsp = json.loads(api_helper.get_response_from_openai_json(const.PROMPT_DICT["person_info"].replace("{context}",page_rsp["page_text"])))
		logger.info(f"Open Ai response : {openAi_rsp}")
		rsp["llm_curr_org"]	= openAi_rsp["curr_org"]
		rsp["llm_curr_title"] = openAi_rsp["curr_title"]
		rsp["llm_prv_org"]	= openAi_rsp["prv_org"]
		rsp["llm_prv_title"] = openAi_rsp["prv_title"]
		rsp["llm_email"] = openAi_rsp["email"]
		rsp["llm_phone"] = openAi_rsp["phone"]
		team_info_json = json.dumps(openAi_rsp)
	else:
		team_info_json = const.NOT_FOUND

	# Insert Record in Company Database
	page_text = str(page_rsp["page_text"]).strip()
	page_text_words = len(page_text.split(" "))

	str_sql = "Insert into company_pages (company_name,page_url,url_type,page_text,page_text_words,extracted_data) Values(?,?,?,?,?,?)"
	db_manager.execute_sql(str_sql,[rsp["company_name"],rsp["source_url"],"contact",page_text,page_text_words,team_info_json])
	## Team Info remove team_info_json
	
	if (page_rsp["page_text"] == "SCRAPPING_NOT_ALLOWED") or (page_rsp["page_text"] == "ERROR"):
		rsp["is_contact_info_found"] = False
	elif 	rsp["llm_prv_org"] == const.NOT_FOUND:
		rsp["is_contact_info_found"] = False
	else:
		rsp["is_contact_info_found"] = True

	return rsp

def is_contact_name_exists_in_title_or_snippet(item, full_name):
	rsp = False
	search_text = str(item["title"]).strip().lower()
	f_l_names = full_name.split(" ")
	if (search_text.find(f_l_names[0]) > -1) and (search_text.find(f_l_names[1]) > -1):
		rsp = True
	# If Name is not present in Title search in snippet
	if not rsp:
		if item.get('snippet') is not None:
			search_text = str(item["snippet"]).strip().lower()
			if (search_text.find(f_l_names[0]) > -1) and (search_text.find(f_l_names[1]) > -1):
				rsp = True
	return rsp	

def find_url_for_contact_current_org(search_items,full_name,email_domain, rsp):
	for item in search_items:
		item_link = str(item["link"]).strip().lower()
		if (is_contact_name_exists_in_title_or_snippet(item,full_name)):		
			if (is_url_start_with_domain(item_link,email_domain)) and (is_valid_team_url(item_link,full_name) and (not rsp["is_current_org"])):
				rsp["is_current_org"] = True
				rsp["source_url"] = item_link 
				rsp["team_url"] = helper.search_using_regex_pattern(item_link,const.TEAM_URL_VALID_PATTERN)
				rsp["website"] = helper.search_using_regex_pattern(item_link,const.WEBSITE_VALID_DOMAIN_PATTERN)
				#rsp["search_text"] = search_text
			elif (item_link.find("linkedin.com/in") > -1):
				rsp["linked_in_profile_url"] = item_link
	logger.info(f"List of filter URL >>>> {rsp}")
	return rsp

#TODO Need to implement this logic after testing completed with current org.
def find_url_for_contact_moved_to_new_org(search_items,search_str,email_domain, rsp):
	return rsp


# def find_contact_current_company(search_str : str, email_domain : str) -> dict:
  
# 	rsp = {}
# 	rsp["is_current_org"] = False
# 	# rsp["curr_title"] = const.NOT_FOUND
# 	# rsp["prv_org"]	= const.NOT_FOUND
# 	# rsp["prv_title"] = const.NOT_FOUND
# 	rsp["source_url"] = const.NOT_FOUND
# 	rsp["linked_in_profile_url"]  = const.NOT_FOUND
# 	rsp["team_url"] = const.NOT_FOUND
# 	rsp["website"] = const.NOT_FOUND
# 	rsp["search_items"] = const.NOT_FOUND
# 	logger.info(f"Started searching for contact {search_str} and Email domain {email_domain}")

# 	api_rsp = get_google_api_resposne_search_items(search_str + " " + email_domain)
# 	if not api_rsp["is_success"]:
# 		rsp["source_url"] = api_rsp["data"]
# 		return rsp
# 	#logger.info("Google Api search is successfull.")
	
# 	search_items = api_rsp["data"]
# 	rsp["search_items"] = search_items
	
# 	for item in search_items:

# 		search_text = str(item["title"]).strip().lower()
# 		# if "snippet" in item:
# 		# 	search_text += " " + str(item["snippet"]).strip().lower()		
# 		item_link = str(item["link"]).strip().lower()
# 		f_l_names = search_str.split(" ")
# 		if (search_text.find(f_l_names[0]) > -1) and (search_text.find(f_l_names[1]) > -1):		
# 			if (is_url_start_with_domain(item_link,email_domain)) and (is_valid_team_url(item_link,search_str) and (not rsp["is_current_org"])):
# 				rsp["is_current_org"] = True
# 				rsp["source_url"] = item_link 
# 				rsp["team_url"] = helper.search_using_regex_pattern(item_link,const.TEAM_URL_VALID_PATTERN)
# 				rsp["website"] = helper.search_using_regex_pattern(item_link,const.WEBSITE_VALID_DOMAIN_PATTERN)
# 				#rsp["search_text"] = search_text
# 			elif (item_link.find("linkedin.com/in") > -1):
# 				rsp["linked_in_profile_url"] = item_link
	
	
	
# 	#TODO : If Linked In Profile URL is available, Use Proxy Curl to Get profile data.
# 	#TODO : If Contact moved to new company, above logic will return "Not Found" We can qualify URL and Do webscraping
# 	# and Gen AI to extract Curr Org and Prev Org. Sometimes in curr org bio, prev org is mentioned.
# 	#TODO Rerun all records which 429 Errors.

# 	# # If Not able to identify by above logic, We can use Open AI to find details from API response.
# 	# if rsp["source_url"] == const.NOT_FOUND:
# 	# 	for item in search_items:
# 	# 		search_text = str(item["title"]).strip().lower() + " " + str(item["snippet"]).strip().lower()
# 	# 		item_link = str(item["link"]).strip().lower()
# 	# 		logger.info("Searchin using Open AI")
# 	# 		openAi_rsp = json.loads(api_helper.get_response_from_openai_json(const.PROMPT_DICT["person_info"].replace("{context}",search_text)))
# 	# 		rsp["open_ai_rsp"] = openAi_rsp
# 	# 		if (openAi_rsp["curr_org"] != const.NOT_FOUND) or (openAi_rsp["prev_org"] != const.NOT_FOUND):
# 	# 			if openAi_rsp["curr_org"] != const.NOT_FOUND:
# 	# 				rsp["curr_org"] = openAi_rsp["curr_org"]
# 	# 				rsp["curr_title"] = openAi_rsp["curr_title"]
# 	# 			if openAi_rsp["prv_org"] != const.NOT_FOUND:
# 	# 				rsp["prv_org"] =  openAi_rsp["prv_org"]	
# 	# 				rsp["prv_title"] = openAi_rsp["prv_title"]
# 	# 			rsp["source_url"] = item_link
# 	# 			rsp["team_url"] = helper.search_using_regex_pattern(item_link,const.TEAM_URL_VALID_PATTERN)
# 	# 			rsp["website"] = helper.search_using_regex_pattern(item_link,const.WEBSITE_VALID_DOMAIN_PATTERN)
# 	# 			break

# 	return rsp

