
import boto3
import botocore
import time
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
    
    # If old_key_name is the prefix, skip it. We do not want to rename and delete the "directory".
    if old_key_name == prefix:
        return(f"File {old_key_name} is the prefix. No action taken.")
    
    if old_key_name == new_key_name:
        return(f"Old and new key names are the same: {old_key_name}. No action taken.")
    
    # If the object is not a txt file we do not process it.
    if not old_key_name.lower().endswith('.txt'):
        return(f"File {old_key_name} is not a .txt file. No action taken.")
    
    s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    my_logger.info(f"Attempting to copy {old_key_name} to {new_key_name} and deleted the old object.")

    # NB: Need s3:PutObject and s3:DeleteObject permissions to run the following code
    try:
        print(f"Copying {old_key_name} to {new_key_name}...")
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
    
    return(f"Successfully deleted {key_name}.")


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
    my_logger.info('Version 2025-11-21 14:45')
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    
    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")

    global bucket_name, aws_access_key_id, aws_secret_access_key, prefix
    bucket_name = public_config["AWS S3"]["bucket_name"]
    prefix = public_config["AWS S3"]["prefix"]  #ex jacktsta240/home/filedrop/cage_by_section/

    aws_access_key_id = private_config["AWS S3"]["aws_access_key_id"]   
    aws_secret_access_key= private_config["AWS S3"]["aws_secret_access_key"] 

    ls = get_all_s3_objects(prefix)
    #print(ls)
    #exit()

    if len(ls) > 2:
        my_logger.info(f"Found {len(ls)} files. Should be exactly two.") # TBD - How should I handle this?
        for obj in ls:
            my_logger.info(f" - {obj['Key']}") 
    elif len(ls) == 0:
        my_logger.info("No files found. Waiting.")
    else:  # Exactly one file and one folder name is present
        for old_key_name in ls:
                my_logger.info("Found " + old_key_name['Key'])
                if old_key_name['Key'].lower().endswith('.txt'):
                        my_logger.info(f"Found {old_key_name['Key']}. Attempting to rename.")
                        #my_logger.info(copy_and_delete_object(old_key_name['Key'], new_key_name=prefix + 'FILE.txt'))

"""
TEST : What follows are some test functions.
"""
def test_list_files():
    ls = get_all_s3_objects(prefix)
    if len(ls) > 1:
        my_logger.info(f"Found {len(ls)} files. Should be exactly one")

    for obj in ls:
        print(obj["Key"])

def test_delete_file():
    key_name='jacktsta240/home/filedrop_error_files/cage_by_section/FILE.txt'
    msg = delete_file(key_name)
    print(msg)  

def test_create_folder(bucket_name):
    s3 = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    try:    
        directory_name = "jacksonproda6/home/filedrop/cage_by_section/" # Name of the folder i.e. object key
        s3.put_object(Bucket=bucket_name, Key=(directory_name))
    except botocore.exceptions.ClientError as e:
        print(f"Error creating bucket: {e}")   
    s3.close()
    print(f"Bucket {directory_name} created.")

    ls = get_all_s3_objects(prefix)

def test_copy_file():

    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    
    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")

    global bucket_name, aws_access_key_id, aws_secret_access_key, prefix
    bucket_name = public_config["AWS S3"]["bucket_name"]
    prefix = public_config["AWS S3"]["prefix"]  #ex jacktsta240/home/filedrop/cage_by_section/

    aws_access_key_id = private_config["AWS S3"]["aws_access_key_id"]   
    aws_secret_access_key= private_config["AWS S3"]["aws_secret_access_key"] 

    src_key_name='jacksonproda6/home/filedrop_error_files/cage_by_section/cag_fa924181_2025-11-20-17-20-08.txt'
    dest_key_name='jacksonproda6/home/filedrop/cage_by_section/cag_fa924181_2025-11-20-17-20-08.txt'
    msg = print(f"Copying {src_key_name} to {dest_key_name}...")
    s3_client = boto3.client('s3',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    response = s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': src_key_name}, Key=dest_key_name) 
    print(msg)


"""
End of test functions
"""

if __name__ == "__main__":
    #main()
    test_copy_file()
