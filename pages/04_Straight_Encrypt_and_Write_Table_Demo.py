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

plain_text_table = os.environ["plain_text_table"]
# Display sample set of the matched table in the clear
t_matches_df = m_session1.table(plain_text_table).limit(15)

st.dataframe(t_matches_df.to_pandas())

m_session1.sql("{}".format(os.environ["userkeys"])).collect()

# Create a dataframe representing the entire matched data set
matches_df = m_session1.table(plain_text_table)
# Create a new dataframe representing the encrypted version of the matched data set

enc_matches_df = m_session1.sql("SELECT 'KEY678901' as keyname,\
								ff3_testing_db.ff3_testing_schema.encrypt_ff3_string_pass3('KEY678901', {}, $userkeys) as {}, \
								ff3_testing_db.ff3_testing_schema.encrypt_ff3_string_pass3('KEY678901', {}, $userkeys) as {}, \
								ff3_testing_db.ff3_testing_schema.encrypt_ff3_string_pass3('KEY678901', {}, $userkeys) as {} \
								FROM {}".format("fname", "fname", "displayname", "displayname", "emailaddress", "emailaddress", plain_text_table))
# Count the records in the encrypted matches table.  Diaplay the time it took and the record count
t_start = datetime.now()
n_records = enc_matches_df.count()

# st.write("Overlap matches: {:,}".format(n_records))

# Display sample set of the encrypted matched table
st.dataframe(enc_matches_df.to_pandas())
e_table_name = plain_text_table + "_ENCRYPTED"
enc_matches_df.write.mode("overwrite").save_as_table(table_name=e_table_name, table_type='transient')
t_end = datetime.now()
the_delta =  t_end.strptime(t_end.strftime("%H:%M:%S"), "%H:%M:%S") - t_start.strptime(t_start.strftime("%H:%M:%S"), "%H:%M:%S")
t_timing_statement = "Start Time: {} / End Time: {}\n | Total Query Time: {}\n".format(t_start.strftime("%H:%M:%S"), t_end.strftime("%H:%M:%S"), the_delta)
c1, c2, c3 = st.columns(3)
c1.metric("Start Time", "{}".format(t_start.strftime("%H:%M:%S")))
c2.metric("End Time", "{}".format(t_end.strftime("%H:%M:%S")))
c3.metric("Run Time", "{}".format(the_delta))
st.write(t_timing_statement)
