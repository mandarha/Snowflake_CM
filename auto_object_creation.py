import streamlit as st 
import configparser
#from io import StringIO
import pandas as pd 
import os
import snowflake.connector
#from snowflake.connector.pandas_tools import write_pandas
#import pathlib


#provide header for the app
st.header('Auto Object Creation Utility in Snowflake')

#Set up config parser to parse config.ini file
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

#Set up Snowflake login credential variables

sfAccount = config['SnowflakePOC']['sfAccount']
sfUser = config['SnowflakePOC']['sfUser']
sfPass = config['SnowflakePOC']['sfPass']
sfDB = config['SnowflakePOC']['sfDB']
sfSchema = config['SnowflakePOC']['sfSchema']
sfWarehouse = config['SnowflakePOC']['sfWarehouse']

#Print SF user name to test login credential variables are setup properly
print(sfUser)
print(sfAccount)

#Set up connection with Snowflake
ctx = snowflake.connector.connect(user = sfUser,
                                    password = sfPass,
                                    account = sfAccount,
                                    warehouse = sfWarehouse,
                                    database = sfDB,
                                    schema = sfSchema
                                    )

#cs = conn.cursor()

#Execute basic query


def execute_the_query(db,sch,tt):
 ctx=snowflake.connector.connect(database=db,schema=sch,table_type=tt,user=sfUser,password = sfPass,
                                    account = sfAccount,
                                    warehouse = sfWarehouse,)
 cs=ctx.cursor()
 cs.execute("USE ROLE ACCOUNTADMIN")
 cs.execute(create_tbl_sql )
 cs.execute('TRUNCATE TABLE IF EXISTS ' + SF_table )
 st.session_state['snow_conn'] = cs
 st.session_state['is_ready'] = True
 return cs



#cs.execute("USE ROLE ACCOUNTADMIN")
schema = pd.read_sql("select SCHEMA_NAME from SNOWFLAKE.account_usage.SCHEMATA ;",ctx)
database = pd.read_sql("select DATABASE_NAME from SNOWFLAKE.account_usage.DATABASES ;",ctx)

sidebar = st.sidebar

with st.sidebar:
    tt=st.selectbox(
    'table_type',
    ('PERMANENT','TRANSIENT'))
    db = st.selectbox(
    'database',
    (database))
    sch = st.selectbox(
    'Schema',
    (schema))
    Run= st.button("EXECUTE", on_click=execute_the_query,args=[db,sch,tt])


uploaded_file = st.file_uploader("Choose a file")

      
if uploaded_file:
     st.write("Filename: ", uploaded_file.name)



filename = os.path.splitext(os.path.basename(uploaded_file.name))[0].upper()







SF_table = 'SF_' + filename
    
    # Create a CREATE TABLE SQL-statement
if tt == 'TRANSIENT':
 create_tbl_sql = "CREATE TRANSIENT TABLE " + db + "." + sch +"."+ SF_table  +" (\n"
#st.write(create_tbl_sql)
delimiter = ","
if tt == 'PERMANENT':
  create_tbl_sql = "CREATE  TABLE IF  NOT EXISTS " + db + "." + sch +"."+ SF_table  +" (\n"
  delimiter = ","
else:
     st.write('')

dfF1 = None
if uploaded_file is not None:
  dfF1 = pd.read_csv(uploaded_file, sep=delimiter)
dfF1.rename(columns=str.upper, inplace=True)
dfF1.columns



    
    # Iterating trough the columns
for col in dfF1.columns:
        column_name = col.upper()
        
        if (dfF1[col].dtype.name == "INT" or dfF1[col].dtype.name == "int64"):
            create_tbl_sql = create_tbl_sql + column_name 
        elif dfF1[col].dtype.name == "object":
            create_tbl_sql = create_tbl_sql + column_name 
        elif dfF1[col].dtype.name == "datetime64[ns]":
            create_tbl_sql = create_tbl_sql + column_name 
        elif dfF1[col].dtype.name == "float64":
            create_tbl_sql = create_tbl_sql + column_name 
        elif dfF1[col].dtype.name == "bool":
            create_tbl_sql = create_tbl_sql + column_name 
        else:
            create_tbl_sql = create_tbl_sql + column_name 
        
        # Deciding next steps. Either column is not the last column (add comma) else end create_tbl_statement
        if dfF1[col].name != dfF1.columns[-1]:
            create_tbl_sql = create_tbl_sql + ",\n"
        else:
            create_tbl_sql = create_tbl_sql + ")"
