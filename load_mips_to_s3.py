#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 22:24:21 2023

@author: anita
"""



#Imports 
import json
import requests
import pandas as pd
import io
import boto3



from selenium import webdriver
from bs4 import BeautifulSoup
import time 
from selenium.webdriver.chrome.options import Options

options = Options()
options.add_argument("--headless=new")



#Number of records of review data 
N_ROWS = 10


FILENAME = "aws_secrets.json"
S3_BUCKET = "team11-datalake"

#uuid_2021 = '8c2ab3e4-597e-43c9-9d94-7223c66d7f25'
#uuid_2020 = '48a484e6-ff02-4284-8666-6045b3cac17f'

year_list =['2020', '2021']


# Function to fetch uuid
def get_mips_uuid():
    driver = webdriver.Chrome(options=options)
    driver.get('https://data.cms.gov/quality-of-care/quality-payment-program-experience/api-docs')

    # Wait for the page to fully load
    #driver.implicitly_wait(60)
    time.sleep(10)

    # Step 2: Parse HTML code and grab tables with Beautiful Soup
    soup = BeautifulSoup(driver.page_source, 'html.parser')

    tables = soup.find_all('option')
    df = pd.DataFrame(columns=['year','uuid'])

    for  table in tables:
        #print(table.text, table.attrs['value'])
        df = df.append({'year':table.text, 'uuid':table.attrs['value']}, ignore_index=True)
        

    driver.close()
    
    return(df)



def get_aws_details(file_name):
    with open(file_name, "r") as secrets:
        config = json.load(secrets)
    
    session = boto3.Session(
        aws_access_key_id = config["aws_access_key_id"],
        aws_secret_access_key = config["aws_secret_access_key"]
    )
    return session.resource('s3'), config["aws_access_key_id"], config["aws_secret_access_key"]

def fetch_mips_data(year, nrows, offset, uuid_token):

    
    # if year == '2020':
    #     uuid_token = uuid_2020
    # else :
    #     uuid_token = uuid_2021
    url ='https://data.cms.gov/data-api/v1/dataset/{uuid}/data?size={nrows}&offset={offset}'.format(uuid=uuid_token, nrows=nrows, offset=offset)
    #Fetching MIPS Data From CMS 
    try:
        data_struct =requests.get(url).json()
        df  = pd.DataFrame(data_struct)
        df = df[['practice state or us territory', 'practice size', 'clinician specialty', 'npi', 'final score', 'complex patient bonus', 'quality category score', 'promoting interoperability (pi) category score','ia score','cost score']]
        #print(df)
    except Exception as err:
        print("Failed to Downlod ", year, " Data, Error:", err)
    return(df)
    
def load_review_data(year):
    
    #Code for fetching uuid
    u_df = get_mips_uuid()
    #print(u_df)
    uuid_token = u_df.loc[(u_df['year']==year), 'uuid'].values[0]
    print("Download Review Data for ", year)
    
    filename = 'mips_review_'+year 
    df_list = []
    offset = 0
    
    if(N_ROWS <= 5000) : 
        m_df = fetch_mips_data(year, N_ROWS, offset, uuid_token)
        df_list.append(m_df)
    else:
        n_rows = N_ROWS
        while(n_rows>5000):
            m_df = fetch_mips_data(year, 5000, offset, uuid_token)
            df_list.append(m_df)
            offset=offset+5000
            n_rows=n_rows-5000
            
        #excess of 5000     
        m_df = fetch_mips_data(year, n_rows, offset, uuid_token)
        df_list.append(m_df)
    
    df = pd.concat(df_list)    
   
    #df['yr'] = year
    #print(df.columns)   
    print("Size of df: ", len(df))
        
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    
    ## AWS Connection and Load 
    try:
        
        object_path = "LANDING/ReviewData/CMDMIPS/{year}/{filename}.csv".format(year=year, filename=filename)
        #object_path = "LANDING/ReviewData/CMDMIPS/{filename}.csv".format(filename=filename)
        #object_path = "LANDING/OpenPayments/GeneralPayment/2020/{filename}.csv".format(filename=filename)
   
        _, access_key, secret_key = get_aws_details(FILENAME)
        #print(access_key)
        #print(secret_key)
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=S3_BUCKET, Key=object_path)
        
    except Exception as err:
        print("Failed to Load Data to S3 ", year, " Review Data, Error:", err)
  
for year in year_list:        
    load_review_data(year)