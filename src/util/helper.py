import pandas as pd
import textwrap
import re
import os
import datetime
import src.settings.constants as const
import time
from urllib.parse import urlparse
import unicodedata


def remove_accents(input_str):
    # Normalize the string to its decomposed form (NFKD)
    normalized = unicodedata.normalize('NFKD', input_str)
    # Build a new string without the accent characters (combining marks)
    plain_str = ''.join(
        char for char in normalized
        if not unicodedata.combining(char)
    )
    return plain_str

def get_root_url(url):
	# Parse the URL into components
	parsed_url = urlparse(url)
	# Construct the root URL using the scheme and netloc (domain)
	root_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
	if root_url == "://":
		root_url = ""
	return root_url

def extract_url_from_text(text : str):
	ret_url = const.NOT_FOUND
	match = re.search(r'https?://[^\s]+', text)
	if match:
		ret_url = match.group(0)
	return ret_url

def is_valid_url(url):
	# Parse the URL into components
	parsed_url = urlparse(url)
	# Check if the URL has a valid scheme and network location (domain)
	if parsed_url.scheme in ['http', 'https'] and parsed_url.netloc:
			return True
	return False

def extract_legal_name(text:str):
	pattern = r'\b[A-Za-z0-9\s,]+?\s(Inc\.|LLC|Ltd\.|Corp\.|Corporation|Co\.|Limited|LLP|L\.P\.|PLC|GmbH|S\.A\.|Pty\. Ltd\.|BV)\b'
	match = re.search(pattern, text,re.IGNORECASE)
	if match:
			return match.group(0)
	else:
			return None
	
def format_url(url : str):

	if ( url.startswith("https://") or url.startswith("http://") ):
		return url
	if ( url.startswith("https://www.") or url.startswith("http://www.") ):
		return url
	
	url_start = "https://"
	if url.find("http://") > -1:
		url_start = "http://"
	if url.find("https://") > -1:
		url_start = "https://"
	url = url.replace(url_start,"")
	url = url.replace("www.","")
	url = url_start + "www." + url
	return url

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
	
def wrap_and_print(input_text,no_of_char=80):
	wrp = textwrap.TextWrapper(width=no_of_char)
	for line in wrp.wrap(input_text):
		print(line)

def get_file_name_and_extension(path):
	file_name, file_extension = os.path.splitext(os.path.basename(path))
	return file_name, file_extension

def get_logfile_name():
	# Get the current date
	current_date = datetime.datetime.now()
	# Format the date as desired, e.g., "logfile_YYYY-MM-DD.log"
	logfile_name = current_date.strftime("app_%Y-%b-%d.log")
	return const.OUTPUT_PATH + logfile_name

def clean_scrapped_text(str_input : str):
	#Remove non-printable ASCII characters
	str_input=re.sub(r'[^\x20-\x7E]', '', str_input)
	str_input = str_input.replace("\\xa0","")
	#Use for if there are multiple space are present then convert in single space 
	str_input=re.sub(r'\s+', ' ', str_input)
	return str_input.strip().lower()

def clean_contact_data(str_input : str):
	#Remove non-printable ASCII characters
	cleaned_name = remove_accents(str_input)
	str_input=re.sub(r'[^\x20-\x7E]', '', cleaned_name)
	str_input = str_input.replace("\\xa0","")
	#Use for if there are multiple space are present then convert in single space 
	str_input=re.sub(r'\s+', ' ', str_input)
	str_input = str_input.strip().lower()
	str_input = str_input.replace("null",'') 
	return str_input

def get_processing_time_in_seconds(start_time: datetime):
	return "Total Processing Time : " + "%.2f" % (time.time()-start_time) + " Seconds"

def get_temp_file_name():
	return "tmp_"+ time.strftime("%Y%m%d_%H%M%S")

def get_file_name(file_name:str):
	# Replace Punctuation mark
	file_name = re.sub(r'[^\w\s]', '', file_name)
	# Remove Multiple space with single space
	file_name = re.sub(r"\s+", " ", file_name)
	# Replace space with _
	file_name = file_name.replace(" ","_").strip()
	return file_name

def read_text_file_as_string(file_path : str):
	ret_text = ""
	with open(file_path,"r", encoding="utf8") as f:
		ret_text = f.read().strip()
	return ret_text

def search_using_regex_pattern(search_text: str,pattern : str):
	match = re.search(pattern, search_text)
	if match:
		return str(match.group(0))
	else:
		return const.NOT_FOUND
	
def get_domain_from_email(email : str):
	email_domain = email.split("@")[1]
	email_domain = re.sub(const.FIND_EMAIL_DOMAIN,".",email_domain)
	return email_domain

