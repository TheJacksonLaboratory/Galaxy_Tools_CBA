
import boto3
import botocore

import configparser  

import my_logger

# Connect to a AWS cloud server and list a directory (bucket)
bucket_name=None
aws_access_key_id=None
aws_secret_access_key=None
prefix=None


    
def copy_and_delete_object(old_key_name, new_key_name):
    """
    Copies the old object to new then deletes the old. 
    Essential a rename.

    Args:
        old_key_name (str): The key of the object to copy and delete.
        new_key_namenew_key_name (str): The key of the new object.

    Returns:
        str:  A message of success or failure.
    """

    s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    
    my_logger.info(f"Attempting to copy {old_key_name} to {new_key_name} and deleted the old object.")

    # NB: Need s3:PutObject and s3:DeleteObject permissions to run the following code
    try:
        response = s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': old_key_name}, Key=new_key_name) 
        response = s3_client.delete_object(Bucket=bucket_name, Key=old_key_name)
    except botocore.exceptions.ClientError as e:
        return(f"Error accessing bucket: {e}")   

    s3_client.close()
    
    
    my_logger.info(f"Successfully copied {old_key_name} to {new_key_name} and deleted the old object.")
    return(f"Successfully copied {old_key_name} to {new_key_name} and deleted the old object.")


def delete_file(key_name):
    """
    Deletes an object from the S3 bucket.

    Args:
        key_name (str): The key of the object to delete.
    Returns:
        str: A message of success or failure.
    """
    s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    try:
        response = s3_client.delete_object(Bucket=bucket_name, Key=key_name)
    except botocore.exceptions.ClientError as e:
        return(f"Error accessing bucket: {e}")   

    s3_client.close()
    
    return(f"Successfully deleted {new_key_name}.")


def get_all_s3_objects(prefix=None):
    """
    Retrieves all objects from an S3 bucket, optionally filtered by a prefix.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str, optional): The prefix to filter objects by. e.g. 'jacktsta240/home/filedrop_error_files/cage_by_section/'

    Returns:
        list: A list of dictionaries, where each dictionary represents an S3 object.
    """
    s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    paginator = s3.get_paginator('list_objects_v2')

    # Prepare pagination arguments
    pagination_args = {'Bucket': bucket_name}
    if prefix:
        pagination_args['Prefix'] = prefix

    all_objects = []
    for page in paginator.paginate(**pagination_args):
        if 'Contents' in page:
            for obj in page['Contents']:
                all_objects.append(obj)
    return all_objects


def main():

    my_logger.info('Logger has been created')
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    
    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")

    global bucket_name, aws_access_key_id, aws_secret_access_key, prefix
    bucket_name = public_config["AWS S3"]["bucket_name"]
    prefix = public_config["AWS S3"]["prefix"]

    aws_access_key_id = private_config["AWS S3"]["aws_access_key_id"]   
    aws_secret_access_key= private_config["AWS S3"]["aws_secret_access_key"] 

    #test_delete_file()
    ls = get_all_s3_objects(prefix)
    if len(ls) > 1:
        my_logger.info(f"Found {len(ls)} files. Should be exactly one.")
        for obj in ls:
            my_logger.info(f" - {obj['Key']}")  
        exit()
    
    my_logger.info(f"Found {ls[0]['Key']}. Attempting to rename.")
    copy_and_delete_object(old_key_name=ls[0]['Key'], new_key_name='FILE.txt')

def test_list_files():
    ls = get_all_s3_objects(prefix)
    if len(ls) > 1:
        my_logger.info(f"Found {len(ls)} files. Should be exactly one")

    for obj in ls:
        print(obj)

def test_delete_file():
    key_name='jacktsta240/home/filedrop_error_files/cage_by_section/FILE.txt'
    msg = delete_file(key_name)
    print(msg)  


if __name__ == "__main__":
    main()
