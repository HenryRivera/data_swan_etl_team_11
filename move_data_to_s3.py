# Importing libraries
import json
import requests
import pandas as pd
import io
import boto3


# Setting global vars
FILENAME = "aws_secrets.json"
S3_BUCKET = "team11-datalake"


# Takes a start and end year and returns the ids for datasets in that timeframe
def get_ids(start, end):
    years = [str(x) for x in range(start, end+1)]
    # For the scope of this project, we are only interested in these 3 kinds of datasets
    ids = {"General": [], "Research": [], "Ownership": []}
    url = 'https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items?show-reference-ids'
    data = requests.get(url).json()
    for d in data:
        split_title = d['title'].split(" ")
        identifier = d['identifier']
        if split_title[0] in years:
            if split_title[1] in ids:
                ids[split_title[1]].append(identifier)
    # returns dictionary
    return ids


def get_aws_details(file_name):
    with open(file_name, "r") as secrets:
        config = json.load(secrets)
    
    session = boto3.Session(
        aws_access_key_id = config["aws_access_key_id"],
        aws_secret_access_key = config["aws_secret_access_key"]
    )
    return session.resource('s3'), config["aws_access_key_id"], config["aws_secret_access_key"]


# Uses the ids collected from the get_ids function to download the datasets and move them to S3
def move_data_to_s3(id_list):
    for category, category_ids in id_list.items():
        for category_id in category_ids:
            url = 'https://openpaymentsdata.cms.gov/api/1/metastore/schemas/dataset/items/{}'.format(category_id)
            data = requests.get(url).json()
            
            # download link
            download = data["distribution"][0]["downloadURL"]
            # removing spaces from file title to be able to use it as S3 filename
            file_name = data["title"].replace(" ", "")
            file_name_split = data["title"].split()
            year = file_name_split[0]
            category = file_name_split[1]
            try:
                object_path = "LANDING/OpenPayments/{category}Payment/{year}/{filename}.csv".format(category=category, year=year, filename=file_name)
                
                response = requests.get(download)
                
                df = pd.read_csv(io.BytesIO(response.content), nrows=10)
                
                csv_buffer = io.BytesIO()
                df.to_csv(csv_buffer, index=False)
                
                # Set AWS credentials and bucket details
                _, access_key, secret_key = get_aws_details(FILENAME)
                s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
                s3.put_object(Body=csv_buffer.getvalue(), Bucket=S3_BUCKET, Key=object_path)
            except Exception as e:
                # will need to add logging
                print("Failed to download:", download, "Error:", e)


def main():
    ids = get_ids(2020, 2021)
    move_data_to_s3(ids)