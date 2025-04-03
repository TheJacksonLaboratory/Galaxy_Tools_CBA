import sys
import configparser  
import json
from datetime import datetime
import pandas as pd
import requests
import os
import logging
import json
from requests.auth import HTTPBasicAuth

def filter(df_list,column_name,filter_list):
    # Take the already winnowed list and keep only the rows that match the filter
    filtered_list = pd.DataFrame()
    for filter in filter_list:
        tmp_df = df_list[df_list[column_name] == filter]
        filtered_list = pd.concat([filtered_list,tmp_df])
    return filtered_list



def test():

    # KOMP BWT LOVs
    keep_columns = [
        "ExperimentName",
		"Sample",
		"Customer_Mouse_ID",
        "Body_Weight_(g)",
        "Age",
		"Experiment_Date",
		"Sex",
		"Genotype",
		"Strain_Name",
		"Strain",
		"Experiment_Barcode",
		"Experiment_Status",
		"Pen",
		"Bedding",
		"Diet",
		"Additional_Notes",
		"Primary_ID",
		"Primary_ID_Value",
		"Date_of_Birth",
		"Exit Reason",
		"Whole_Mouse_Fail",
		"Whole_Mouse_Fail_Reason",
		"Experiment",
		"Protocol_Name",
		"Tester_Name",
        ]
    df = pd.read_csv('/projects/galaxy/tools/cba/data/KOMP_BWT_raw_data.csv')
    # Re-order the columns
    
    # Subtract column Date_of_Birth  from Experiment_Date  and add it to the dataframe
    df['Experiment_Date'] = pd.to_datetime(df['Experiment_Date'])
    df['Date_of_Birth'] = pd.to_datetime(df['Date_of_Birth'])
    df['Age'] = df['Experiment_Date'] - df['Date_of_Birth']
    df['Age'] = df['Age'].dt.days / 7
    
    df = df[keep_columns]
    df = df.sort_values(by=['Sample','Age'],ascending=True)
    f = open("/projects/galaxy/tools/cba/data/re_ordered.csv", 'w', encoding='utf-8')
    df.to_csv(f,encoding='utf-8', errors='replace', index=False, header=True)
    


def main():
    url = 'https://jacksonlabs.platformforscience.com/PROD/odata/CBA_BATCH?$expand=REV_EXPERIMENT_BATCH_CBA_MAGNETIC_RESONANCE_IMAGING_EXPERIMENT&$select=Barcode&$count=true'
    my_auth = HTTPBasicAuth('svc-corePFS@jax.org', 'hRbP&6K&(Qvw')
    # JSESSIONID 879B981C4B84001D149470AF43DAD753
    my_session = requests.Session()
    response = my_session.get(url)
    
    session_id = response.cookies.get('JSESSIONID')
    print(response.status_code)
    print(session_id)
    

if __name__ == "__main__":
    test()

#cookies_dict = {'JSESSIONID': 'ABCDEF012346789'}
#response = requests.get('http://httpbin.org/cookies', cookies=cookies_dict)
#import requests
#session = requests.Session()
# Set a cookie
#session.cookies.set('cookie_name', 'cookie_value', domain='httpbin.org', path='/')
# Send a request with the cookie
#response = session.get('http://httpbin.org/cookies')
#print(response.text)
