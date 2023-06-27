#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 26 22:24:21 2023

@author: anita
"""


#aws_access_key_id=AKIAIOSFODNN7EXAMPLE
#aws_secret_access_key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY


#Imports 
import json
import requests
import pandas as pd
import io
import boto3


FILENAME = "aws_secrets.json"
S3_BUCKET = "team11-datalake"

uuid_2021 = '8c2ab3e4-597e-43c9-9d94-7223c66d7f25'
uuid_2020 = '48a484e6-ff02-4284-8666-6045b3cac17f'

year_list =['2020']

def get_aws_details(file_name):
    with open(file_name, "r") as secrets:
        config = json.load(secrets)
    
    session = boto3.Session(
        aws_access_key_id = config["aws_access_key_id"],
        aws_secret_access_key = config["aws_secret_access_key"]
    )
    return session.resource('s3'), config["aws_access_key_id"], config["aws_secret_access_key"]


def load_review_data(year):
    print("Download Review Data for ", year)
    if year == '2020':
        uuid_token = uuid_2020
    else :
        uuid_token = uuid_2021
    filename = 'mips_review_'+year    
    url ='https://data.cms.gov/data-api/v1/dataset/{uuid}/data?size=10&offset=0'.format(uuid=uuid_token)
    
    #Fetching MIPS Data From CMS 
    try:
        data_struct =requests.get(url).json()
        df  = pd.DataFrame(data_struct)
        df = df[['practice state or us territory', 'practice size', 'clinician specialty', 'npi', 'final score', 'complex patient bonus', 'quality category score', 'promoting interoperability (pi) category score','ia score','cost score']]
        print(df)
    except Exception as err:
        print("Failed to Downlod ", year, " Data, Error:", err)
     
        
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    
    ## AWS Connection and Load 
    try:
        
        object_path = "LANDING/ReviewData/CMDMIPS/{year}/{filename}.csv".format(year=year, filename=filename)
        #object_path = "LANDING/ReviewData/CMDMIPS/{filename}.csv".format(filename=filename)
        #object_path = "LANDING/OpenPayments/GeneralPayment/2020/{filename}.csv".format(filename=filename)
   
        _, access_key, secret_key = get_aws_details(FILENAME)
        print(access_key)
        print(secret_key)
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        s3.put_object(Body=csv_buffer.getvalue(), Bucket=S3_BUCKET, Key=object_path)
        
    except Exception as err:
        print("Failed to Load Data to S3 ", year, " Review Data, Error:", err)
  
for year in year_list:        
    load_review_data(year)