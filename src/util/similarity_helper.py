from rapidfuzz import fuzz
import Levenshtein
import jellyfish as jfish
from difflib import SequenceMatcher
from Levenshtein import ratio

def get_fuzzy_similarity(str_src,str_trg):
	if  (len(str_src) != 0 and len(str_trg) != 0):
		return (fuzz.token_set_ratio(str_src, str_trg)/100)
	else:
		return 0.0	

def get_levenshtein_similarity(str_src,str_trg):
	"""
	Levenshtein distance measures the minimum number of single-character edits required to change one string into another.
	"""
	if  (len(str_src) != 0 and len(str_trg) != 0):
		return Levenshtein.ratio(str_src, str_trg)
	else:
		return 0.0	

def get_jaro_winkler_similarity(str_src,str_trg):
	if  (len(str_src) != 0 and len(str_trg) != 0):
		return jfish.jaro_winkler_similarity(str_src, str_trg)
	else:
		return 0.0	

def sound_index(str_src,str_trg):
	soundex1 = jfish.soundex(str_src)
	soundex2 = jfish.soundex(str_trg)
	soundex_score = 1.0 if soundex1 == soundex2 else 0.0
	return soundex_score

def ngram_similarity(str_src,str_trg):
    return SequenceMatcher(None,str_src ,str_trg).ratio()



def is_similar(name1, name2, threshold=0.55):
    return ratio(name1, name2) >= threshold