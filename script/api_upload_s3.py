#import necessary packages
import json
import requests
import pandas as pd
import boto3
import logging
from pathlib import Path
from botocore.exceptions import ClientError


def get_ids(start:int, end:int, url:str) -> dict:
    years = [str(x) for x in range(start, end+1)]
    ids = {"General": [], "Research": [], "Ownership": []}
    data = requests.get(url).json()
    for d in data:
        split_title = d['title'].split(" ")
        identifier = d['identifier']
        if split_title[0] in years:
            if split_title[1] in ids:
                ids[split_title[1]].append(identifier)
    return ids 

def get_datasets(id_list:dict,url:str,dataset:str) -> pd.DataFrame:
    for category, category_ids in id_list.items():
        for category_id in category_ids:
            url = url.format(category_id)
            data = requests.get(''.join[url,category_id]).json()
            download = data["distribution"][0]["downloadURL"]
            try:
                df = pd.read_csv(download, nrows=1)
            except Exception as e:
                print("Failed to download:", download, "Error:", e)

def get_datasets(distribution_ids:dict,request_url:str,dataset:str,limit:int,offset:int) -> pd.DataFrame:
    if dataset == 'General':
        general_distro_ids =  [id for id in distribution_ids['General']]
        try:
            for id in general_distro_ids:
                requests.get(request_url.format(id,limit,offset))
                
        except Exception as e:
            print (e)
        pass
    if dataset == 'Research':
        pass 
    if dataset == 'Ownership':
        pass

def create_S3_client():
    pass 

def upload_data_to_s3():
    create_S3_client()
    
def main():
    get_ids()
    get_datasets()
    create_S3_client()
    upload_data_to_s3()

if __name__ == '__main__':
    main()







