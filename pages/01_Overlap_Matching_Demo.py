import pandas as pd
from io import StringIO
import streamlit as st
from streamlit.logger import get_logger
from dotenv import load_dotenv
import os
import logging
from datetime import datetime, timedelta
from snowflake.snowpark import Session

st.set_page_config(page_title="Overlap Matching Demo")

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

uploaded_files = st.file_uploader(label = "Select the file(s) against which to match", accept_multiple_files=True, type = ['csv'])

if ((uploaded_files)):
	t_df = pd.DataFrame()
	for t_file in uploaded_files:
		if t_file is not None:
			amt_of_data = t_file.getvalue()
			st.write(amt_of_data)

			str_io = StringIO(t_file.getvalue().decode("utf-8"))
			# st.write(str_io)

			str_data = str_io.read()
			st.write(str_data)

			df = pd.read_csv(t_file)
			t_df = t_df.append(df)

	st.dataframe(t_df)
	snow_df = m_session1.write_pandas(t_df, "TMP_MATCH_DEMO", auto_create_table=True)

	c1, c2 = st.columns(2)

	with c1:
		record_count = st.radio("Max Source Records for Match", (0, 1000000, 10000000, 100000000, 200000000, 350000000), format_func=lambda the_value: "{:,}".format(the_value))
	with c2:
		# snow_df.write.mode("overwrite").save_as_table("TMP_MATCH_DEMO")
		if snow_df:
			st.write("Records in the uploaded file, now resident in a Snowflake Table")
			st.dataframe(snow_df.to_pandas())

# l_df1.head(10)
# l_df1.info()
if record_count > 10:
	c3, c4 = st.columns(2)
	with c3:
		st.write(snow_df.to_pandas())
	with c4:
		wh_size = st.radio("Warehouse Size?", ["XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE", "XXLARGE"], disabled = False, horizontal = True)
	t_message = "Snowpark Session One: Call to to_pandas() method: with specified columns: {}".format("DECRYPTED")
	# print(t_message)
	# logging.info(t_message)
	logger.info(t_message)
	# m_df1 = m_session1.sql("SELECT email, phone, snowssn_eudf(ssn) FROM {}".format(os.environ["source_table"].upper()))
	m_session1.sql("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 2200").collect()
	wh_info = m_session1.get_current_warehouse()
	m_session1.sql("ALTER WAREHOUSE {} SET WAREHOUSE_SIZE = {} WAIT_FOR_COMPLETION = TRUE".format(wh_info, wh_size)).collect()
	t_start = datetime.now()

	m_session1.sql("{}".format(os.environ["userkeys"])).collect()
	# m_df1 = m_session1.sql("SELECT EMAIL, PHONE, SNOWSSN_EUDF(ssn), SNOWSSN_EUDF(ssn2), SNOWSSN_EUDF(ssn3) FROm {} LIMIT {}".format(os.environ["source_table"].upper(), record_count))
	m_df1 = m_session1.sql("SELECT ff3_testing_db.ff3_testing_schema.decrypt_ff3_string_pass3('KEY678901', EMAIL, $userkeys) as email,\
					ff3_testing_db.ff3_testing_schema.decrypt_ff3_string_pass3('KEY678901', NAME, $userkeys) as name,\
					ff3_testing_db.ff3_testing_schema.decrypt_ff3_string_pass3('KEY678901', PHONE, $userkeys) as phone\
					FROm {} LIMIT {}".format(os.environ["ff3_target_table"].upper(), record_count))
	l_df1 = m_df1.limit(200).to_pandas()
	t_end = datetime.now()
	d_table_name = "CUST_DETOKENIZED_" + t_end.strftime("%H_%M_%S")
	m_table_name = "OVERLAP_MATCHES_" + t_end.strftime("%H_%M_%S")
	matched_df = snow_df.join(m_df1, (snow_df["emailaddress"] == m_df1["EMAIL"]))
	m_df1.write.mode("overwrite").save_as_table(d_table_name)
	matched_df.write.mode("overwrite").save_as_table(m_table_name)
	t_end = datetime.now()
	the_delta =  t_end.strptime(t_end.strftime("%H:%M:%S"), "%H:%M:%S") - t_start.strptime(t_start.strftime("%H:%M:%S"), "%H:%M:%S")
	t_timing_statement = "Start Time: {} / End Time: {}\n | Total Query Time: {}\n: Total Record Count: {:,} | {}".format(t_start.strftime("%H:%M:%S"), t_end.strftime("%H:%M:%S"), the_delta, m_df1.count(), wh_info)
	# st.dataframe(next(l_df1))
	if wh_size != "XSMALL":
		m_session1.sql("ALTER WAREHOUSE {} SET WAREHOUSE_SIZE = {}".format(wh_info, "XSMALL")).collect()

	st.dataframe(matched_df.to_pandas())
	st.write(t_timing_statement)

