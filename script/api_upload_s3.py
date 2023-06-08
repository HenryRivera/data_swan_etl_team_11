#import necessary packages
import json
import requests
import pandas as pd
import boto3
import logging
from pathlib import Path
from botocore.exceptions import ClientError


def get_ids(start, end,url):
    years = [str(x) for x in range(start, end+1)]
    ids = {"General": [], "Research": [], "Ownership": []}
    # url = 'https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items?show-reference-ids'
    data = requests.get(url).json()
    for d in data:
        split_title = d['title'].split(" ")
        identifier = d['identifier']
        if split_title[0] in years:
            if split_title[1] in ids:
                ids[split_title[1]].append(identifier)
    return ids 

def get_datasets(id_list):
    for category, category_ids in id_list.items():
        for category_id in category_ids:
            url = 'https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items/{}'.format(category_id)
            data = requests.get(url).json()
            download = data["distribution"][0]["downloadURL"]
            try:
                df = pd.read_csv(download, nrows=1)
            except Exception as e:
                print("Failed to download:", download, "Error:", e)

def create_S3_client():
    pass 

def upload_data_to_s3():
    create_S3_client()
    

def main():
    extract_api_main()
    upload_data_to_s3()

def extract_api_main():
    get_ids()
    get_datasets()

if __name__ == '__main__':
    main()







