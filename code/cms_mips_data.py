#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 24 22:25:19 2023

@author: anita
"""

import json
import requests
import pandas as pd



#Reference Page https://data.cms.gov/quality-of-care/quality-payment-program-experience/api-docs

uuid_2021 = '8c2ab3e4-597e-43c9-9d94-7223c66d7f25'
uuid_2020 = '48a484e6-ff02-4284-8666-6045b3cac17f'

def load_review_data(year):
    print("Download Review Data for ", year)
    if year == '2020':
        uuid_token = uuid_2020
    else :
        uuid_token = uuid_2021
        
    url ='https://data.cms.gov/data-api/v1/dataset/{uuid}/data?size=10&offset=0'.format(uuid=uuid_token)
    data_struct =requests.get(url).json()
    df  = pd.DataFrame(data_struct)
    df = df[['practice state or us territory', 'practice size', 'clinician specialty', 'npi', 'final score', 'complex patient bonus', 'quality category score', 'promoting interoperability (pi) category score','ia score','cost score']]
    print(df)
    
#url_ec_file = 'https://data.cms.gov/provider-data/sites/default/files/resources/a0f235e13d54670824f07977299e80e3_1676693125/ec_score_file.csv'
#df3= pd.read_csv(url_ec_file, nrows=10)

#url_csv = 'https://data.cms.gov/data-api/v1/dataset/{uuid}/data.csv'.format(uuid=uuid_2021)
#df2 = pd.read_csv(url_csv)

#url ='https://data.cms.gov/data-api/v1/dataset/{uuid}/data?size=10&offset=0'.format(uuid=uuid_2020)
#data_struct =requests.get(url).json()
#df  = pd.DataFrame(data_struct)
#df = df[['practice state or us territory', 'practice size', 'clinician specialty', 'npi', 'final score', 'complex patient bonus', 'quality category score', 'promoting interoperability (pi) category score','ia score','cost score']]

load_review_data('2021')
