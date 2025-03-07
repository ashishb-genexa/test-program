import requests
import src.settings.constants as const
import src.util.log_helper as log_helper
import src.util.helper as helper

# This is Create the Logger Instance 
logger = log_helper.set_get_logger("ProxyCurl",helper.get_logfile_name())

def get_linkedin_profile(linked_in_profile_url : str) -> dict:
  rsp = {}
  rsp["is_success"] = True
  headers = {'Authorization': 'Bearer ' + const.PROXY_CURL_API_KEY}
  para = {
  'linkedin_profile_url': linked_in_profile_url,
  'extra': 'exclude',
  'github_profile_id': 'exclude',
  'facebook_profile_id': 'exclude',
  'twitter_profile_id': 'exclude',
  'personal_contact_number': 'exclude',
  'personal_email': 'exclude',
  'inferred_salary': 'exclude',
  'skills': 'exclude',
  'use_cache': 'if-recent',
  'fallback_to_cache': 'on-error',		
  }
  try:
    with requests.get(const.PROXY_CURL_LINKED_IN_API_ENDPOINT, params=para, headers=headers) as r:
      r.raise_for_status()
      rsp["data"] = r.json()
  except Exception as e:
    logger.exception(e)
    rsp["data"] = "ERROR"
    rsp['is_success'] = False
  return rsp

