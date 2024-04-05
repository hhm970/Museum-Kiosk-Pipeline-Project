"""
Script that extracts all relevant files from a given S3 bucket (details of
which are specified in .env), downloads them onto our local environment, and
merges all .csv files into a singular combined .csv file, ready to be cleaned.
"""
import logging
from os import environ as ENV, listdir, path, remove

import pandas as pd
from dotenv import load_dotenv
from boto3 import client


def get_bucket_names(s3_client) -> list[str]:
    """Returns a list of available bucket names."""

    response = s3_client.list_buckets()

    buckets = response["Buckets"]

    return [b["Name"] for b in buckets]


def get_bucket_objects(s3_client, bucket_name: str) -> list[str]:
    """Returns a list of available object names in a given bucket."""

    response = s3_client.list_objects(Bucket=bucket_name)

    objects = response['Contents']

    return [o["Key"] for o in objects]


def filter_json_csv_files(resource_list: list[str]) -> list[str]:
    """Filters out relevant .json and .csv files in a given list of filenames."""

    result = []

    for key in resource_list:
        if key.startswith("lmnh_exhibition_") and key.endswith(".json"):
            result.append(key)
        if key.startswith("lmnh_hist_data_") and key.endswith(".csv") and key[15: -4].isnumeric():
            result.append(key)

    return result


def download_bucket_cs_resources(s3_client, bucket_name: str,
                                 folder_name: str = "bucket_data") -> None:
    """Downloads all files in the S3 bucket that are relevant to the case study."""

    if bucket_name not in get_bucket_names(s3_client):
        logging.error(f"Bucket {bucket_name} not found")
        raise ValueError(f"Bucket {bucket_name} not found")

    museum_resources_list = get_bucket_objects(
        s3_client, bucket_name)

    cs_resources = filter_json_csv_files(museum_resources_list)

    folder_path = f"./{folder_name}"

    for o in cs_resources:
        s3_client.download_file(Bucket=bucket_name,
                                Key=o,
                                Filename=f"{folder_path}/{o}")


def combine_csv_files(folder_path: str = 'bucket_data') -> None:
    """
    Combines multiple .csv files in a folder into one, and then deletes all 
    initial .csv files from the folder.
    """
    csv_files = [filename
                 for filename in listdir(folder_path)
                 if filename.endswith(".csv")]

    df_list = []

    for csv in csv_files:
        file_path = path.join(folder_path, csv)
        try:
            df = pd.read_csv(file_path)
            df_list.append(df)
        except Exception as e:
            print(f"Could not read file {csv} because of error: {e}")
        finally:
            remove(file_path)

    big_df = pd.concat(df_list, ignore_index=True)

    big_df.to_csv(path.join(folder_path, 'combined_file.csv'), index=False)


if __name__ == "__main__":

    load_dotenv()

    s3 = client("s3",
                aws_access_key_id=ENV["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=ENV["AWS_SECRET_ACCESS_KEY"])

    download_bucket_cs_resources(s3, "resources-museum",
                                 "bucket_data")

    combine_csv_files("bucket_data")
