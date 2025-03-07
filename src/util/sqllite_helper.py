import sqlite3
import src.settings.constants as const
import src.util.log_helper as log_helper
import src.util.helper as helper

logger = log_helper.set_get_logger("sqllite_helper",helper.get_logfile_name())

def init_db():
  str_sql1 = """ 
  CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    page_url TEXT NOT NULL,
    url_type TEXT NOT NULL,
    page_text TEXT NOT NULL,
    page_text_words INTEGER DEFAULT 0,
    company_info TEXT NOT NULL DEFAULT 'NONE',
    company_info_words INTEGER DEFAULT 0,
    team_info TEXT NOT NULL DEFAULT 'NONE',
    team_info_words INTEGER  DEFAULT 0,
    company_info_json TEXT NOT NULL DEFAULT 'NONE',
    team_info_json TEXT NOT NULL DEFAULT 'NONE',
    created_on TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_on TEXT NULL
  );
  """
  with sqlite3.connect(const.DB_FILE_PATH) as conn:
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS company")
    cur.execute(str_sql1)


def execute_sql(str_sql : str, data : tuple) -> bool:
  with sqlite3.connect(const.DB_FILE_PATH) as conn:
    cur = conn.cursor()
    cur.execute(str_sql,data)

def select_sql(str_sql : str, data : tuple):
  with sqlite3.connect(const.DB_FILE_PATH) as conn:
    cur = conn.cursor()
    cur.execute(str_sql,data)
    rows = cur.fetchall()
    return rows

def select_scaler(str_sql : str, data : tuple) -> str:
  with sqlite3.connect(const.DB_FILE_PATH) as conn:
    cur = conn.cursor()
    cur.execute(str_sql,data)
    rows = cur.fetchone()
    if rows is None:
      return const.NOT_EXISTS
    else:
      return list(rows)[0]





