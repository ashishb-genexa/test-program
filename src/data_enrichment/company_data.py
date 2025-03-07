import json
import src.webscrapper.webscrapper as wsc
import src.util.helper as helper
import src.settings.constants as const
import src.util.sqllite_helper as db_manager
import requests
from urllib.parse import urlparse
import random
import src.util.api_helper as api_helper

def get_exact_urls_from_list(list_urls : list, validation_list : str) -> list:
	url_set = set()
	for url in list_urls:
		if url == "nan":
			continue
		parsed_url = urlparse(url)
		last_section = parsed_url.path.strip('/').split('/')[-1]
		for sub_url in validation_list:
			if sub_url in str(last_section).strip().lower():
				url_set.add(url)
				break

	return list(url_set)


def get_website_child_url(website : str, company_name : str, robot_file,child_url_keywords:list) ->dict:
	rsp = {}
	rsp["is_success"] = True 
	rsp["child_urls"] = None
	rsp["error"] = ""

	website = helper.format_url(website)
	str_sql= "SELECT url_list FROM child_url_list WHERE company_name = ? AND root_url=?"
	res = db_manager.select_scaler(str_sql,[company_name,website])
	if res != const.NOT_EXISTS:
		rsp["child_urls"]= json.loads(res)
		return rsp
	
	if robot_file == const.NOT_FOUND:
		is_url_allowed_to_scrap = True
	else:
		is_url_allowed_to_scrap = wsc.is_scraping_allowed_by_robots_file(website,robot_file)

	if not is_url_allowed_to_scrap:
		#logger.info(f"url {website} is not allowed scrap by robot.txt file.")
		rsp["error"] = "SCRAPPING_NOT_ALLOWED"	
		rsp["is_success"] = False
		return rsp	
	try:
		headers = {'User-Agent': const.USER_AGENTS[0]}
		with requests.get(website,headers=headers, timeout=const.REQUEST_TIME_OUT,verify=False) as r:
			r.raise_for_status()
			page_urls = wsc.url_from_html(r.text)
			#logger.info(f"Home Page All Urls : {page_urls}")
			child_urls = ",".join(wsc.validate_page_urls(website,page_urls))
			exact_urls =  str(child_urls).strip().lower().split(",")
			exact_urls = get_exact_urls_from_list(exact_urls,child_url_keywords)
			rsp["child_urls"] = exact_urls
	
	except Exception as e:
		rsp["is_success"] = False
		rsp["error"] = e
		#logger.error(f"error occured : {e}")
	
	if rsp["is_success"]:
		str_sql="INSERT INTO child_url_list (company_name,root_url, url_list)VALUES (?, ?, ?)"
		db_manager.execute_sql(str_sql,[company_name, website, json.dumps(exact_urls)])
	return rsp

#TODO : Move this function to Webscrapper
#TODO : Insert Page content in company_pages table
def get_page_content(page_url:str, robot_file )->dict :
	"""
	This Function will go through Website specific page Find page text content.
	"""
	rsp = {}
	rsp["is_success"] = False
	str_sql = "SELECT page_text from company_pages where page_url= ?"
	res = db_manager.select_scaler(str_sql,[page_url])
	if res != const.NOT_EXISTS:
		rsp["page_text"]= res
		rsp["is_success"] = True
		return rsp


	if robot_file == const.NOT_FOUND:
		is_url_allowed_to_scrap = True
	else:
		is_url_allowed_to_scrap = wsc.is_scraping_allowed_by_robots_file(page_url,robot_file)

	if not is_url_allowed_to_scrap:
		#logger.info(f"url {page_url} is not allowed scrap by robot.txt file.")
		rsp["page_text"] = "SCRAPPING_NOT_ALLOWED"	
		return rsp
	
	#logger.info(f"Request started for : {page_url}")
	try:
		headers = {'User-Agent': random.choice(const.USER_AGENTS)}
		with requests.get(page_url,headers=headers, timeout=const.REQUEST_TIME_OUT) as r:
			r.raise_for_status()
			rsp["page_text"] = helper.clean_scrapped_text(wsc.text_from_html(r.text))
			rsp["is_success"] = True
	except Exception as e:
		rsp["page_text"] = "ERROR" 
		rsp["is_success"] = False
		#logger.error(f"error occured : {e}")
	#logger.info(f"Request Completed for : {page_url}")
	return rsp
    
def is_retirive_all(data: dict):
    for key, value in data.items():
        if value == const.NOT_FOUND:
            return False
    return True

def get_comapny_data(website : str, comapny_name : str) ->dict:
    #logger.info(f"Searchin website : {website} for company name : {comapny_name}")
    rsp ={}
    url_set=set()
    rsp["is_success"]= False
    rsp["url"]=[]
    rsp["error"]=""
    rsp["llm_company_name"]=const.NOT_FOUND
    rsp["llm_address"]=const.NOT_FOUND
    rsp["llm_email"]=const.NOT_FOUND
    rsp["llm_phone"]=const.NOT_FOUND


    website = helper.format_url(website)
    robot_file = wsc.get_website_robot_file(website, comapny_name)
    rsp_url = get_website_child_url(website,comapny_name,robot_file,const.COMPANY_CONACT_ABOUT)

    if  not rsp_url["is_success"]  :
        rsp['error']= rsp_url['error']
        return rsp
    
    for url in rsp_url['child_urls']:
        rsp_page= get_page_content(url,robot_file)
        if rsp_page['is_success']:
            openAi_rsp = json.loads(api_helper.get_response_from_openai_json(const.PROMPT_DICT["compnay"].replace("{context}",rsp_page['page_text'])))

            if openAi_rsp['company_name'] != const.NOT_FOUND:
                rsp["is_success"]= True
                rsp["llm_company_name"]= openAi_rsp["company_name"]
                url_set.add(url)
            if openAi_rsp['phone'] != const.NOT_FOUND:
                rsp["is_success"]= True
                rsp["llm_phone"]= openAi_rsp["phone"]
                url_set.add(url)
            if openAi_rsp['email'] != const.NOT_FOUND:
                rsp["is_success"]= True
                rsp["llm_email"]= openAi_rsp["email"]
                url_set.add(url)
            if openAi_rsp['address'] != const.NOT_FOUND:
                rsp["is_success"]= True
                rsp["llm_address"]= openAi_rsp["address"]
                url_set.add(url)
            if is_retirive_all(rsp):
                break

    rsp["url"]= list(url_set)

    #if rsp["is_success"]:
        #str_sql = "Insert into company (name,page_url,url_type,team_info_json) Values(?,?,?,?)"
        #db_manager.execute_sql(str_sql,[rsp["llm_company_name"],json.dumps(rsp['url']),"contact",json.dumps(rsp)])
    
    return rsp  