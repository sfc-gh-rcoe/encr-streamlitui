import pandas as pd
import streamlit as st
from streamlit.logger import get_logger
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

st.set_page_config(page_title="Write Encrypted Table Demo")

def format_values():
	t_reformatted = "{:,}".the_value
	

load_dotenv()
sec_auth_info = {
	"account": os.environ["account_name"],
	"user": os.environ["sec_account_user"],
	"password": os.environ["account_password"],
	"role": os.environ["sec_account_role"],
	"schema": os.environ["sec_account_schema"],
	"database": os.environ["sec_account_database"],
	"warehouse": os.environ["account_warehouse"]
}

auth_info = {
	"account": os.environ["account_name"],
	"user": os.environ["account_user"],
	"password": os.environ["account_password"],
	"role": os.environ["account_role"],
	"schema": os.environ["account_schema"],
	"database": os.environ["account_database"],
	"warehouse": os.environ["account_warehouse"]
}

logger = get_logger(__name__)
the_time = datetime.now()


m_session1 = Session.builder.configs(sec_auth_info).create()

m_session2 = Session.builder.configs(auth_info).create()
record_count = 0

t_matches_df = m_session1.table(st.session_state["matched_table_name"]).limit(15)

st.dataframe(t_matches_df.to_pandas())