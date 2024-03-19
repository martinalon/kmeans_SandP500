import streamlit as st
from datetime import date, timedelta, datetime
import os
import requests

st.set_page_config(
    page_title="Home",
    page_icon="🚀",
)

st.write("# Welcome to SP-500 predictions! 👋")
st.sidebar.success("Select a page above.")

st.markdown(
    """
   Hello, here unsupervised models are applied to group the trends of the companies that belong to the SP-500 index.

   Market information is taken every minute, and the following information is obtained for each company:

        Open: The stock price at the beginning of the time interval.

        Close: The stock price at the end of the time interval.

        Low: The lowest price in the time interval.

        High: The highest price in the time interval.
"""
)

##############################################
#############################################
### This part of the code helps to control the choice of the feature of the dbs, en then shows the las day in the base

feature = st.selectbox(
        'Please choose the feature you are interested in ',
        ["Open", "Close", "Low", "High"]
)


url_ld= "http://127.0.0.1:8000/" + feature
payload_ld= {}
headers_ld = {
  'accept': 'application/json'
}
response_ld = requests.request("GET", url_ld, headers=headers_ld, data=payload_ld)
st.write(' The last day in the database is as follows (yyyy-mm-dd):', str(response_ld.text[2:12]))

##################################################
##################################################


##################################################
##################################################
# This part of the code generates a form with a submit button to introduce the information needed to create or update the dbs

st.write(""" 
         You can create a new database, which would delete the existing one 
         and create a new one (you can only go back 30 days from the present day). 
         You can also update the database, which does not delete the existing one 
         and only adds the new days that you select.""")


with st.form("creation_form", clear_on_submit=True):
    create = st.selectbox(
        'What would you like to do? ',
        ["Update the database", "Create a new database"]
        )

    start_day_creation = st.text_input('Enter the FIRST day to be considered with the following format yyyy-mm-dd', )
    end_day_creation = st.text_input('Enter the LAST day to be considered with the following format yyyy-mm-dd', )  
    creation_submited = st.form_submit_button("Submit")

##################################################
##################################################
    
##################################################
##################################################
# This part of the code helps to create a confirmation form and then triger the process to create or update the dbs.  

confirmation_submited = False
if len(start_day_creation)>0 and len(end_day_creation)>0:
  with st.form("Confirmation", clear_on_submit=True):
    if create == "Update the database" :
      creation_bool = False
      creation_message = ("The database has been updated successfully")
      st.markdown(''' :red[¡Warning!]  ''')
      st.write('''
                You are about to update the existing database, where all the company information will be added,
                taking into account from the FIRST day, and taking into account until the LAST day. 
                If you want to continue, press Submit.''')
      
    elif create == "Create a new database":
      creation_bool = True
      creation_message = ("The database has been created successfully")
      st.markdown(''' :red[¡Warning!]  ''')
      st.write('''
                You are about to delete the current database and replace it with a new database that takes 
                into account from the FIRST day, and takes into account until the LAST day. 
                If you want to continue, press Submit.''')
    
    confirmation_submited = st.form_submit_button("I am agree")

##################################################
##################################################
    
##################################################
##################################################
# this part of the code makes the request to the REST api to create or update de dbs.
    
if confirmation_submited:
  url_create_db = "http://127.0.0.1:8000/" + str(creation_bool) + "/"+ start_day_creation + "/" + end_day_creation
  payload_create_db= {}
  headers_create_db = {
    'accept': 'application/json'
    }
      
  response_create_db = requests.request("GET", url_create_db, headers = headers_create_db, data=payload_create_db)
  st.write(creation_message)

##################################################
##################################################
