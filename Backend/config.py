import re
from typing import Dict, List, Tuple, Optional, Sequence

# IMPORTANT PATHS
DOCX_RETAIL_PATH = r"C:\Users\HarryKember\Desktop\V6\Docx Retail"
CHART_PACKS_PATH = r"C:\Users\HarryKember\Desktop\V6\Chart_Packs"
DB_PATH_MAIN = r"C:\Users\HarryKember\Desktop\V6\Backend\pdfint.db"
HOME_DIR = r"C:\Users\HarryKember\Desktop\V6"

# known companies 
ASX_COMPANIES: Dict[str, str] = {
    "ADH": "Adairs Limited",
    "ALD": "Ampol Limited",
    "AX1": "Accent Group Limited",
    "BAP": "Bapcor Limited",
    "BRG": "Breville Group Limited",
    "CCX": "City Chic Collective Limited",
    "COL": "Coles Group Limited",
    "EDV": "Endeavour Group Limited",
    "DSK": "Dusk Group Limited",
    "HVN": "Harvey Norman Holdings Limited",
    "JBH": "JB Hi-Fi Limited",
    "LOV": "Lovisa Holdings Limited",
    "MTS": "Metcash Limited",
    "MYR": "Myer Holdings Limited",
    "NCK": "Nick Scali Limited",
    "PMV": "Premier Investments Limited",
    "SIG": "Sigma Healthcare Limited",
    "SUL": "Super Retail Group Limited",
    "TPW": "Temple & Webster Group Ltd",
    "VEA": "Viva Energy Group Limited",
    "WES": "Wesfarmers Limited",
    "WOW": "Woolworths Group Limited",
}
ALIASES: Dict[str, List[str]] = {
    "ALD": ["Ampol"],
    "ADH": ["Adairs"],
    "AX1": ["Accent Group", "Accent"],
    "BAP": ["Bapcor"],
    "BRG": ["Breville"],
    "CCX": ["City Chic", "CityChic"],
    "COL": ["Coles"],
    "EDV": ["Endeavour", "Endeavour Group", "Dan Murphy's", "BWS"],
    "DSK": ["Dusk"],
    "HVN": ["Harvey Norman", "HarveyNorman"],
    "JBH": ["JB Hi-Fi", "JBHIFI", "JB HiFi", "JBHiFi", "JB Hifi", "JB"],
    "LOV": ["Lovisa"],
    "MTS": ["Metcash", "IGA"],
    "MYR": ["Myer"],
    "NCK": ["Nick Scali", "NickScali"],
    "PMV": ["Premier Investments", "Premier"],
    "SIG": ["Sigma"],
    "SUL": ["Super Retail", "SuperRetail", "SRG"],
    "TPW": ["Temple & Webster", "Temple&Webster", "Temple and Webster", "T+W"],
    "VEA": ["Viva", "Viva Energy", "VivaEnergy"],
    "WES": ["Wesfarmers", "Wes"],
    "WOW": ["Woolworths", "Woolies", "Woolworths Group"],
}
COMPANY_TERMS = {
    "woolworths", "wow", "asx:wow",
    "coles", "col", "asx:col",
    "jb hi-fi", "jbh", "jb hifi", "jbhifi", "asx:jbh",
    "bunnings",
    "wesfarmers", "wes", "asx:wes",
    "harvey norman", "hvn", "asx:hvn",
    "super retail", "sul", "asx:sul",
    "myer", "myr", "asx:myr",
    "premier investments", "pmv", "asx:pmv",
    "supercheap", "rebel", "bcf",  # brand sublines
    "aldi", "kmart", "target", "officeworks", "big w", "dan murphy", "liquorland",
}

# debug output: q/a logging
OUTPUT_FILE = "question_answer_output.txt"





# non essentials stuff
ORDER_DEFAULT = "newest"

YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
TICKER_RE = re.compile(r"\b[A-Z]{3,4}\b")

LLM_MODEL   = "gpt-4o"
CLASSIFY_MODEL = "gpt-4o-mini"
REWRITER_MODEL  = "gpt-4o-mini"
EMBED_MODEL = "text-embedding-3-small"

REWRITE_ON     = True 
REWRITE_HYDE   = True
REWRITE_PPAR   = 3

OVERVIEW_TOP_K = 16 # unused 

_CIT_MARK = re.compile(r'\[S(?P<S>\d+)\s+p(?P<page>\d+)\s+"(?P<quote>[^"]+)"\]')
DATE_RE = re.compile(r"(\d{6})")  # matches yymmdd

WORD_BOUNDARY = r"(?<![A-Za-z0-9]){term}(?![A-Za-z0-9])"



DATE_LINE_RE = re.compile(
    r"^\s*(\d{1,2}\s+\w+\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})\s*$",
    re.IGNORECASE,
)