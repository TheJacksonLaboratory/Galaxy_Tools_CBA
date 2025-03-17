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
# Read a csv file into a dataframe
    df1 = pd.read_csv('data/BIG_ASS_raw_data.csv')        

    # Filter dataframe by column CBA_Batch
    column_name= 'CBA_Batch'
    filter_list = ["CBB615","CBB63","CBB75"]
    df2 = filter(df1,column_name,filter_list)
    
    filter_list.clear()
    filter_list = ["CBA_NMR_BODY_COMPOSITION_EXPERIMENT"]
    df2 = filter(df2,"ExperimentName",filter_list)
    
    filter_list.clear()
    filter_list = ["HOM"]
    df2 = filter(df2,"Genotype",filter_list)
    
    df2.to_csv("data/test-results.csv")


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
    main()

#cookies_dict = {'JSESSIONID': 'ABCDEF012346789'}
#response = requests.get('http://httpbin.org/cookies', cookies=cookies_dict)
#import requests
#session = requests.Session()
# Set a cookie
#session.cookies.set('cookie_name', 'cookie_value', domain='httpbin.org', path='/')
# Send a request with the cookie
#response = session.get('http://httpbin.org/cookies')
#print(response.text)
