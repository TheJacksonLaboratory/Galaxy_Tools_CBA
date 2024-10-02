#### This script pulls in progress strains from Jax Lims and Core PFS and calculates a combined count

# Import all the required libraries
import mysql.connector as sql
import pandas as pd
import requests
import json
from datetime import datetime


# Connect to the Lims database
rslims_conn = sql.connect(
  host="rslims.jax.org",
  user="dba",
  password="rsdba",
  database="rslims"
)

# Query the database (sql)
rslims_data_table = pd.read_sql("""
SELECT t3.StockNumber, t4.LineName, t5.Sex, t7.GenotypeSymbol, t8.OrganismStatus, t9.ExitReason
FROM organismstudy t1 
    INNER JOIN organism t2 USING (_Organism_key)
    INNER JOIN cv_organismstatus t8 USING (_OrganismStatus_key)
    LEFT OUTER JOIN cv_exitreason t9 USING (_ExitReason_key)
    INNER JOIN line t3 USING (_Line_key)
    INNER JOIN linename t4 USING (_Line_key)
    INNER JOIN cv_sex t5 USING (_Sex_key)
    INNER JOIN genotype t6 USING (_Organism_key)
    INNER JOIN cv_genotypesymbol t7 USING (_GenotypeSymbol_key)
WHERE
    t1._Study_key = '27'
    AND t3._LineStatus_key != '13'
    AND t4.IsPrimaryName = '1'
    AND t8._OrganismStatus_key != 14;
                          """,
                        rslims_conn)


# Close connection to the database
rslims_conn.close()

# Clean up the data table for Lims to match with Core PFS
# Clean the stock numbers to remove 0
rslims_data_table['StockNumber'] = rslims_data_table['StockNumber'].str.lstrip('0')

# Remove whitespaces
rslims_data_table = rslims_data_table.map(lambda x: x.strip() if isinstance(x, str) else x)


# Connect to the PFS via OData API
# Set your username and password for basic authentication
username = "svc-limsdb@jax.org"
password = "vA&ce3(ROzAL"

# Query using the credentials
auth = (username, password)
queryString = "https://jacksonlabs.platformforscience.com/PROD/odata/KOMP_REQUEST?$expand=REV_MOUSESAMPLELOT_KOMPREQUEST($expand=SAMPLE/pfs.MOUSE_SAMPLE)&$count=true"
result = requests.get(queryString, auth=auth,headers = {"Prefer": "odata.maxpagesize=5000"})

# The query outputs in JSON format, clean and parse the JSON
# Parse the string data into a dictionary
content = json.loads(result.content)

# Get list of experiment entities
exp_ls = content["value"]
#with open("mice_details_0924.json", "w") as outfile:
#    json.dump(exp_ls, outfile)

# Initialize a variable to store all the print results
output = ""
 
# For each experiment in the list, get the list EXPERIMENT_SAMPLES
for exp in exp_ls:
    sample_lots = exp["REV_MOUSESAMPLELOT_KOMPREQUEST"]  # Could be empty
    # for each experiment_sample,
    for sample_lot in sample_lots:
        mouse_sample = sample_lot["SAMPLE"]
        # Check if the mouse sample is active
        if not mouse_sample.get("Active", False):
            continue
        output += 'Mouse Name:' + mouse_sample['JAX_SAMPLE_EXTERNALID'] + "\n"
        output += 'Mouse Sex:' + mouse_sample['JAX_MOUSESAMPLE_SEX'] + "\n"
        idx = mouse_sample['JAX_MOUSESAMPLE_ALLELE'].index(' ')
        output += 'Mouse Line:' + mouse_sample['JAX_MOUSESAMPLE_ALLELE'][0:idx] + "\n"
        output += 'Mouse StockNumber:' + mouse_sample['JAX_MOUSESAMPLE_ALLELE'][idx+4:-1] + "\n" # 4 lets us skip the "JR"
        output += 'Mouse Genotype:' + mouse_sample['JAX_MOUSESAMPLE_GENOTYPE'] + "\n"
        #output += 'Mouse life status:' + mouse_sample['JAX_SAMPLELOT_STATUS'] + "\n" # Note this a sample_lot!
        if sample_lot['JAX_SAMPLELOT_STATUS'] is None:
            output += 'Mouse life status:' + "null" + "\n"
        else:
            output += 'Mouse life status:' + sample_lot['JAX_SAMPLELOT_STATUS'] + "\n" 
        
        if mouse_sample['JAX_MOUSESAMPLE_EXITREASON'] is None:
            output += 'Mouse exit reason:' + "null" + "\n"
        else:
            output += 'Mouse exit reason:' + mouse_sample['JAX_MOUSESAMPLE_EXITREASON'] + "\n"
        if mouse_sample['JAX_MOUSESAMPLE_ISFILLER'] is None:
            output += 'Mouse filler:' + "null" + "\n"
        else:
            output += 'Mouse filler:' + mouse_sample['JAX_MOUSESAMPLE_ISFILLER'] + "\n"
        output += "\n"
#print(output)

# Split the data into individual entries
entries = output.strip().split('\n\n')

# Initialize a list to store parsed entries
mouse_data = []

# Parse each entry and extract information
for entry in entries:
    lines = entry.split('\n')
    mouse_info = {}
    for line in lines:
        key, value = line.split(':')
        print("key:", key)
        print("value:", value)
        mouse_info[key.strip()] = value.strip()
    mouse_data.append(mouse_info)

# Store in a dataframe
pfs_data_parsed = pd.DataFrame(mouse_data)

# Check for duplicates and remove those
#if pfs_data_parsed['Mouse Name'].duplicated().any():
#    pfs_data_parsed.drop_duplicates(subset=['Mouse Name'], inplace=True)
#    print("Duplicates have been dropped from the DataFrame.")
#else:
#    print("No duplicates found in the DataFrame.")

# Clean the data table from PFS to match with Lims
# Drop the column 'Number of experiments' and 'Mouse Name' if it exists
if 'Number of experiments' in pfs_data_parsed.columns:
    pfs_data_parsed.drop(columns=['Number of experiments'], inplace=True)
if 'Mouse Name' in pfs_data_parsed.columns:
    pfs_data_parsed.drop(columns=['Mouse Name'], inplace=True)

# Drop rows with 'Yes' in the Mouse filler column - we do not need to count filler mice
pfs_data_parsed = pfs_data_parsed[pfs_data_parsed["Mouse filler"].str.contains("Yes") == False]

# Drop the Mouse filler column
pfs_data_parsed.drop(['Mouse filler'], axis=1, inplace=True)

##### Write to csv
#type(pfs_data_parsed)
#pfs_data_parsed.to_csv('pfs_data_parsed.csv', index=False)



# Replace 'M' with 'Male' and 'F' with 'Female' in Mouse Sex column
pfs_data_parsed['Mouse Sex'] = pfs_data_parsed['Mouse Sex'].replace({'M': 'Male', 'F': 'Female'})

# Replace 'Discarded' with 'Euthanized' and 'Received' with 'Alive' in Mouse life status column
pfs_data_parsed['Mouse life status'] = pfs_data_parsed['Mouse life status'].replace({'Discarded': 'Euthanized', 'Received': 'Alive'})

# Rename the columns
pfs_data_parsed.rename(columns={
                   'Mouse Sex': 'Sex',
                   'Mouse Line': 'LineName',
                   'Mouse StockNumber': 'StockNumber',
                   'Mouse Genotype': 'GenotypeSymbol',
                   'Mouse life status': 'OrganismStatus',
                   'Mouse exit reason': 'ExitReason'}, inplace=True)

# Clean the stock numbers to remove 0
pfs_data_parsed['StockNumber'] = pfs_data_parsed['StockNumber'].str.lstrip('0')

# Remove whitespaces
pfs_data_parsed = pfs_data_parsed.map(lambda x: x.strip() if isinstance(x, str) else x)

# Modify the 'Exit Reason' column from 'null' to 'None'
pfs_data_parsed['ExitReason'] = pfs_data_parsed['ExitReason'].replace({'null': 'None'})

# Print the data tables from Lims and PFS
#print(rslims_data_table)
#print(pfs_data_parsed)

# Export the PFS data to check with the excel export directly from PFS
#pfs_data_parsed.to_csv("pfs_data_python_04_22.csv", index=False)

# Concatenate the two tables
# Merge the two tables
lims_pfs_merge = pd.concat([rslims_data_table, pfs_data_parsed], ignore_index=True)

# Empty rows break the code while grouping, replace with 'None' strings
# Convert blank values to None
lims_pfs_merge['ExitReason'] = lims_pfs_merge['ExitReason'].fillna('None').astype(str)

# Group by and count
grouped_lims_pfs_df = lims_pfs_merge.groupby(['StockNumber', 'LineName', 'Sex', 'GenotypeSymbol', 'OrganismStatus', 'ExitReason'], as_index=False).size()

# Drop rows for Stock Number 5304
grouped_lims_pfs_df = grouped_lims_pfs_df[grouped_lims_pfs_df["StockNumber"].str.contains("5304") == False]

# Get the current date and time
current_datetime = datetime.now()

# Format the date and time
date_string = current_datetime.strftime("%Y-%m-%d")

# Define the filename with the current date
filename = f'lims_pfs_strain_inprogress_{date_string}.csv'

# Export as a csv file with date
grouped_lims_pfs_df.to_csv(filename, index=False)

print(f"CSV file '{filename}' has been created.")
