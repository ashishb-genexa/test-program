#Common Settings
OUTPUT_PATH = "./output/"
DB_FILE_PATH = "./db/"

# Duplicate Finder Settings
COL_ID = "RowNo"
COL_DUP_GROUP_ID = "dup_group_id"
COL_ERROR_RATE = "error_rate"
COL_FUZZ_SIMILARITY = "fuzzy_similarity"
COL_LEVENSHTEIN_SIMILARITY = "levenshtein_similarity"
COL_JARO_SIMILARITY = "jaro_winkler_similarity"
COL_DUP_ROW_GROUP = "row_group"
COL_SOURCE_ID = "src_row_no"
COL_TARGET_ID = "trg_row_no"
GROUP_BY_COL_NAMES = ["FirstName","LastName","part_email"]
COMPARE_COLUMNS = [ { "name" : "FFN", "compare_cols":["FirstName","LastName","part_email"]} ]
MAX_ERROR_RATE = 25.0

# Web Scrapper Settings
REQUEST_TIME_OUT = 60
BASE_URL_GOOGLE_API = 
#BASE_URL_GOOGLE_API = '
SLEEP_TIME_BETWEEN_REQUEST = 1  
PAGE_CHILD_URLS = ["about","team","leader","bio","contact","company","people","meet-our","management","board-of-directors","our-firm","meet-our-professionals"]
FUZZY_RATIO_THRESHOLD = 70

# NER Settings
CHUNK_MAX_WORDS = 384  # 512 Token. Multiply * 2 to Increase token size like 1024, 2048 etc
KEYWORDS_FOR_COMPANY_INFO = ['global headquarters','corporate hq','contact us','get in touch','contact info','reach out','connect with us','offices','contacts & locations','locations','our locations','customer service','contact']
KEYWORDS_FOR_TEAM = ['industry leaders','executive leadership','leasing team contacts','new associates','executive bios','officers','board of directors','leadership team','leadership','management team', 'executive team', 'chairman managing member', 'trusted advisors','our people','our team','meet our team','our leadership']
SPLIT_LARGE_TEXT_CHUNK_SENTENCE_MAX_WORD = 30