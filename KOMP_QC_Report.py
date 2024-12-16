#!/usr/bin/env python
######## 
# Author: Shalmali Kulkarni
# This script pulls in Core PFS data via OData query and performs certain checks for validating mice, metadata (assay data attributes) and assay data. This is version v0 which for now just
# checks for presence of values for mandatory parameters, experimenter id to integer match on JAXLIMS and a valid test date.
# Next version might include controlled vocabularies for values and some case-by-case basis additions.
# It converts IMPC codes to column headers for easy understanding
# This script should generate 1 csv file, mouse, metadata and assay data which should include validation output from 17 experiments.


########## Import all required libraries #########################################################################################################################################
import mysql.connector as sql
import pandas as pd
import read_config as cfg
import requests
import re
from requests.auth import HTTPBasicAuth
import os
import json
import csv
from datetime import datetime, timedelta
import sys

###################################################################################################################################################################################
##################################################### List of experiments to validate from Core PFS ###############################################################################

# Experiments to validate
kompExperimentNames = [
"AUDITORY_BRAINSTEM_RESPONSE",
"BODY_COMPOSITION",
"BODY_WEIGHT",
"CLINICAL_BLOOD_CHEMISTRY",
"ELECTROCARDIOGRAM",
"ELECTRORETINOGRAPHY",
"EYE_MORPHOLOGY",
"FUNDUS_IMAGING",
"GLUCOSE_TOLERANCE_TEST",
"GRIP_STRENGTH",
"HEART_WEIGHT",
"HEMATOLOGY",
"HOLEBOARD",
"LIGHT_DARK_BOX",
"OPEN_FIELD",
"SHIRPA_DYSMORPHOLOGY",
"STARTLE_PPI"
]

#######################################################################################################################################################################################
################################################################## Get mandatory IMPC codes from JAXLIMS ##############################################################################

# Connect to the SQLite database
parameterdetails_conn = sql.connect(
  host="rslims.jax.org",
  user="dba",
  password="rsdba",
  database="komp"
)

## Query the database (sql)
parameterdetails_data_table = pd.read_sql("""
SELECT * FROM komp.dccparameterdetails;
                          """,
                        parameterdetails_conn)


# Close connection to the database
parameterdetails_conn.close()

impc_codes = parameterdetails_data_table['ImpcCode'].tolist()

###################################################################################################################################################################
###################################################### Mouse validation ###########################################################################################

# Get all mice  
mycfg = cfg.parse_config(path="/projects/galaxy/tools/cba/config_sk.yml")
# Setup credentials for database
baseURL = mycfg['corepfs_database']['baseURL']
mouseEndpoint = mycfg['corepfs_database']['mouseEndpoint']
username = mycfg['corepfs_database']['username']
password = mycfg['corepfs_database']['password']
      

# Mouse validation
my_auth = HTTPBasicAuth(username, password)
query = baseURL + mouseEndpoint
result = requests.get(query, auth=my_auth, headers={"Prefer": "odata.maxpagesize=5000"})
mice_content = json.loads(result.content)
exp_ls = mice_content["value"]
fields_to_check = ['JAX_SAMPLE_EXTERNALID', 'JAX_MOUSESAMPLE_SEX', 'JAX_MOUSESAMPLE_ALLELE', 'JAX_MOUSESAMPLE_GENOTYPE', 'JAX_MOUSESAMPLE_DATEOFBIRTH']
mouse_table_rows = []
for komp_request in exp_ls:
    for mouse_sample_lot in komp_request['REV_MOUSESAMPLELOT_KOMPREQUEST']:
        mouse_sample_dict = mouse_sample_lot['SAMPLE']
        if mouse_sample_dict['JAX_MOUSESAMPLE_ISFILLER'] is None:
            for field in fields_to_check:
                if not mouse_sample_dict.get(field):
                    mouse_barcode = mouse_sample_dict.get('Barcode', 'Unknown')
                    comment = f"The field '{field}' is missing."
                    mouse_table_rows.append([mouse_barcode, comment])
mouse_df = pd.DataFrame(mouse_table_rows, columns=["Mouse_ID", "Comments"])


#####################################################################################################################################################################################################
####################################### Metadata validation #########################################################################################################################################

# Parse the configuration file
mycfg = cfg.parse_config(path="/projects/galaxy/tools/cba/config_sk.yml")
baseURL = mycfg['corepfs_database']['baseURL']
username = mycfg['corepfs_database']['username']
password = mycfg['corepfs_database']['password']
experimentEndpointTemplate = mycfg['corepfs_database']['experimentEndpointTemplate']

# Initialize an empty list to store the table rows
metadata_table_rows = []

# Iterate over all experiment names
for experimentName in kompExperimentNames:
    experimentEndpoint = experimentEndpointTemplate.format(exp=experimentName)
    my_auth = HTTPBasicAuth(username, password)
    query = baseURL + experimentEndpoint
    result = requests.get(query, auth=my_auth, headers={"Prefer": "odata.maxpagesize=5000"})
    
    # Check if the request was successful
    if result.status_code == 200:
        # Check if the response content is not empty
        if result.content:
            # Parse the string data into a dictionary
            exp_content = json.loads(result.content)
            # Get list of experiment entities
            exp_ls = exp_content["value"]
            
            # Find the entity with EntityTypeName "KOMP_XXX_XXX_EXPERIMENT"
            for entity in exp_ls:
                if entity["EntityTypeName"].startswith("KOMP_") and entity["EntityTypeName"].endswith("_EXPERIMENT"):
                    # Check if JAX_EXPERIMENT_STATUS is "Review Completed"
                    if entity.get("JAX_EXPERIMENT_STATUS") == "Review Completed":
                        # Iterate over the fields in the list
                        for field in impc_codes:
                            # Check if the field exists in the entity
                            if field in entity:
                                field_value = entity[field]
                                if not field_value:
                                    experiment_barcode = entity.get('Barcode', 'Unknown')
                                    comment = f"Metadata QC Fail: Missing value for {field}"
                                    metadata_table_rows.append([experiment_barcode, comment])
                    break  # Only check the first matching entity
        else:
            print(f"No content returned for {experimentName}.")
    else:
        print(f"Failed to retrieve data for {experimentName}. Status code: {result.status_code}")

metadata_df = pd.DataFrame(metadata_table_rows, columns=["Experiment_Barcode", "Comments"])

#######################################################################################################################################################################################################
#################################### Assay data validation ############################################################################################################################################

# Check experimenter ID - pull experimenter id and barcode from lims

experimentid_conn = sql.connect(
  host="rslims.jax.org",
  user="dba",
  password="rsdba",
  database="komp"
)

experimenterid_data_table = pd.read_sql("""
SELECT * FROM komp.experimenterid;
                          """,
                        experimentid_conn)


# Close connection to the database
experimentid_conn.close()

#print(experimenterid_data_table)
exp_id = experimenterid_data_table["FirstName"].tolist()

# Fields to ignore
ignore_fields = ['JAX_ASSAY_ASSAYFAILCOMMENTS', 'JAX_ASSAY_COMMENTS', 'JAX_ASSAY_ASSAYFAILREASON']


###### Construct a List for time series - hard coded
GTT_series = ['IMPC_IPG_002_001_T0', 'IMPC_IPG_002_001_T15', 'IMPC_IPG_002_001_T30', 'IMPC_IPG_002_001_T60', 'IMPC_IPG_002_001_T120']
OFD_series = ['JAX_OFD_005_001_1st5', 'JAX_OFD_005_001_2nd5', 'JAX_OFD_005_001_3rd5', 'JAX_OFD_005_001_4th5']
GRS_series = ['IMPC_GRS_001_001_T1', 'IMPC_GRS_001_001_T2', 'IMPC_GRS_001_001_T3', 'IMPC_GRS_002_001_T1', 'IMPC_GRS_002_001_T2', 'IMPC_GRS_002_001_T3']
HBD_series = ['JAX_HBD_002_001']



# Define the experiment types and their corresponding series ---- # Same for media series
experiment_series = {
    "KOMP_GLUCOSE_TOLERANCE_TEST_EXPERIMENT": GTT_series,
    "KOMP_OPEN_FIELD_EXPERIMENT": OFD_series,
    "KOMP_HOLEBOARD_EXPERIMENT": HBD_series,
    "KOMP_GRIP_STRENGTH_EXPERIMENT": GRS_series
}



media_series_impc_codes = ['IMPC_CSD_085_001_01', 'IMPC_CSD_085_001_02', 'IMPC_CSD_085_001_03', 
                           'IMPC_CSD_085_001_04', 'IMPC_CSD_085_001_05', 'IMPC_CSD_085_001_06', 
                           'IMPC_EYE_051_001_1', 'IMPC_EYE_051_001_2', 'IMPC_EYE_051_001_3', 
                           'IMPC_EYE_051_001_4', 'IMPC_EYE_051_001_5',
                           'IMPC_ECG_025_001_f1', 'IMPC_ECG_025_001_f2']


# Parse the configuration file
mycfg = cfg.parse_config(path="/projects/galaxy/tools/cba/config_sk.yml")
baseURL = mycfg['corepfs_database']['baseURL']
username = mycfg['corepfs_database']['username']
password = mycfg['corepfs_database']['password']
experimentEndpointTemplate = mycfg['corepfs_database']['experimentEndpointTemplate']

# Initialize an empty list to store the table rows
table_rows = []

# Iterate over all experiment names
for experimentName in kompExperimentNames:
    experimentEndpoint = experimentEndpointTemplate.format(exp=experimentName)
    my_auth = HTTPBasicAuth(username, password)
    query = baseURL + experimentEndpoint
    result = requests.get(query, auth=my_auth, headers={"Prefer": "odata.maxpagesize=5000"})
    
    # Check if the response is valid JSON
    try:
        exp_content = json.loads(result.content)
    except json.JSONDecodeError:
        print(f"Error decoding JSON for experiment: {experimentName}")
        continue
    
    # Get list of experiment entities
    exp_ls = exp_content.get("value", [])
    
    # Loop through each sample
    for sample in exp_ls:
        # Check the experiment status
        if sample.get("JAX_EXPERIMENT_STATUS") != "Review Completed":
            # Extract the experiment barcode from the start of the JSON dictionary
            entity_type = sample.get("EntityTypeName", "").replace("EXPERIMENT", "ASSAY")
            entity_id = sample.get("Id", "")
            name = sample.get("Name", "")
            baseURL_exp = "https://jacksonlabs.platformforscience.com/PROD/corelims?cmd=enter-validation&entityType="
            url = f'{baseURL_exp}{entity_type.replace("_", "%20")}&entityId={entity_id}'
            experiment_barcode = f'=HYPERLINK("{url}", "{name}")'
            comments = "Experiment status is invalid"
            table_rows.append([None, experiment_barcode, None, comments, "Experiment cannot be validated"])
            continue  # Skip this sample if the status is not "Review Completed"
        
        # Extract the experiment barcode from the start of the JSON dictionary
        entity_type = sample.get("EntityTypeName", "").replace("EXPERIMENT", "ASSAY")
        entity_id = sample.get("Id", "")
        name = sample.get("Name", "")
        baseURL_exp = "https://jacksonlabs.platformforscience.com/PROD/corelims?cmd=enter-validation&entityType="
        url = f'{baseURL_exp}{entity_type.replace("_", "%20")}&entityId={entity_id}'
        experiment_barcode = f'=HYPERLINK("{url}", "{name}")'
        
        for komp_sample in sample.get("EXPERIMENT_SAMPLES", []):
            # Check if the field Active is 'true'
            if not komp_sample.get("Active", False):
                continue  # Skip this sample if Active is 'false'
            
            assay_data = komp_sample.get("ASSAY_DATA", {})
            mouse_sample = komp_sample.get("ENTITY", {}).get("SAMPLE", {})
            barcode = komp_sample.get("ENTITY", {}).get("Barcode", "")

            # New validation logic for penbarcode location "LC5" and date of death before test date
            if assay_data.get("JAX_ASSAY_PENBARCODE") == "LC5":
                jax_mouse_sample_date_of_death_str = mouse_sample.get("JAX_MOUSESAMPLE_DATEOFDEATH")
            
                if jax_mouse_sample_date_of_death_str:
                    jax_mouse_sample_date_of_death = datetime.strptime(jax_mouse_sample_date_of_death_str, "%Y-%m-%d")
                
                    # Find the test date from other assays in the same experiment
                    skip_validation = False
                    for other_komp_sample in sample.get("EXPERIMENT_SAMPLES", []):
                        other_assay_data = other_komp_sample.get("ASSAY_DATA", {})
                        other_penbarcode = other_assay_data.get("JAX_ASSAY_PENBARCODE")
                        other_test_date_str = (other_assay_data.get("JAX_ASSAY_TEST_DATE") or
                                               other_assay_data.get("IMPC_CBC_046_001") or
                                               other_assay_data.get("JAX_ASSAY_TESTDATE"))
                        
                        # Ensure other_test_date_str is not None and handle accordingly
                        if other_test_date_str:
                            if isinstance(other_test_date_str, list):
                                other_test_date_str = [date_str.split()[0] if "IMPC_CBC_046_001" in other_assay_data else date_str for date_str in other_test_date_str]
                            elif isinstance(other_test_date_str, str):
                                if "IMPC_CBC_046_001" in other_assay_data:
                                    other_test_date_str = other_test_date_str.split()[0]
                                other_test_date_str = [other_test_date_str]  # Convert to list for uniform processing
                    
                            for date_str in other_test_date_str:
                                # Parse the date based on the format
                                if "IMPC_CBC_046_001" in other_assay_data:
                                    other_test_date = datetime.strptime(date_str, "%m/%d/%Y")
                                elif "JAX_ASSAY_TESTDATE" in other_assay_data:
                                    other_test_date = datetime.strptime(date_str, "%m/%d/%Y")
                                else:
                                    other_test_date = datetime.strptime(date_str, "%Y-%m-%d")
                    
                                if other_test_date > jax_mouse_sample_date_of_death:
                                    skip_validation = True
                                    break
                        else:
                            # Find another assay_data with a valid date
                            for alt_komp_sample in sample.get("EXPERIMENT_SAMPLES", []):
                                alt_assay_data = alt_komp_sample.get("ASSAY_DATA", {})
                                alt_test_date_str = (alt_assay_data.get("JAX_ASSAY_TEST_DATE") or
                                                     alt_assay_data.get("IMPC_CBC_046_001") or
                                                     alt_assay_data.get("JAX_ASSAY_TESTDATE"))
                                if alt_test_date_str:
                                    if isinstance(alt_test_date_str, list):
                                        alt_test_date_str = [date_str.split()[0] if "IMPC_CBC_046_001" in alt_assay_data else date_str for date_str in alt_test_date_str]
                                    elif isinstance(alt_test_date_str, str):
                                        if "IMPC_CBC_046_001" in alt_assay_data:
                                            alt_test_date_str = alt_test_date_str.split()[0]
                                        alt_test_date_str = [alt_test_date_str]  # Convert to list for uniform processing
                                    
                                    for date_str in alt_test_date_str:
                                        # Parse the date based on the format
                                        if "IMPC_CBC_046_001" in alt_assay_data:
                                            other_test_date = datetime.strptime(date_str, "%m/%d/%Y")
                                        elif "JAX_ASSAY_TESTDATE" in alt_assay_data:
                                            other_test_date = datetime.strptime(date_str, "%m/%d/%Y")
                                        else:
                                            other_test_date = datetime.strptime(date_str, "%Y-%m-%d")
                                        
                                        if other_test_date > jax_mouse_sample_date_of_death:
                                            skip_validation = True
                                            break
                                    if skip_validation:
                                        break
                
                    # Skip validation only if both conditions are met
                    if skip_validation:
                        continue  # Skip validation for this assay

            # Check if "JAX_ASSAY_ASSAYFAILREASON" is set to '-'
            if assay_data.get("JAX_ASSAY_ASSAYFAILREASON") != '-':
                continue  # Skip this sample if the condition is not met
            
            # Remove the fields to ignore from assay_data
            filtered_assay_data = {k: v for k, v in assay_data.items() if k not in ignore_fields}
            
            # Check if the mouse is a filler
            is_filler = mouse_sample.get("JAX_MOUSESAMPLE_ISFILLER")
            if is_filler is not None:
                comments = "Filler Mouse"
                table_rows.append([barcode, experiment_barcode, assay_data.get("Barcode", ""), comments, "Procedure QC Failed"])
                continue  # Skip this sample if the mouse is a filler
            
            # Check if each field in filtered_assay_data has a value and its corresponding QC field
            for field in filtered_assay_data.keys():
                field_qc = field + "_QC"
                if field_qc in filtered_assay_data and filtered_assay_data[field_qc] != "-":
                    continue  # Skip this field if the QC field is not set to "-"
                if field != "Barcode" and not filtered_assay_data[field]:
                    comments = f"'{field}' does not have a value"
                    table_rows.append([barcode, experiment_barcode, assay_data.get("Barcode", ""), comments, "Procedure QC Failed"])
            
            # Get the experiment type
            experiment_type = entity.get("EXPERIMENT", {}).get("EntityTypeName", "") if isinstance(entity.get("EXPERIMENT", {}), dict) else ""
            
            # Check if any value from the corresponding series list is present in the filtered_assay_data
            for experiment, series in experiment_series.items():
                if experiment_type == experiment:
                    for field in series:
                        if field not in filtered_assay_data:
                            comments = f"'{field}' does not have a value"
                            table_rows.append([barcode, experiment_barcode, assay_data.get("Barcode", ""), comments, "Procedure QC Failed"])
                    break
            
            # Check for experimenter id present in the filtered_assay_data
            if not any(value in exp_id for value in filtered_assay_data.values()):
                comments = "No/incorrect experimenter ID associated with this experiment"
                table_rows.append([barcode, experiment_barcode, assay_data.get("Barcode", ""), comments, "Procedure QC Failed"])
            
            # For media series, check if the file path exists, if it does, check if there is a file on the server
            for field in media_series_impc_codes:
                if field in filtered_assay_data.keys():
                    # Check if it has a value (essentially a file path)
                    file_path = filtered_assay_data[field]
                    if file_path != '0' and file_path is not None:
                        file_exists = os.path.exists(file_path)
                        if not file_exists:
                            comments = f"Image not found '{file_path}' for '{field}'"
                            table_rows.append([barcode, experiment_barcode, assay_data.get("Barcode", ""), comments, "Procedure QC Failed"])
            
            # Check the date of birth and assay test date
            dob_str = mouse_sample.get("JAX_MOUSESAMPLE_DATEOFBIRTH")
            test_date_str = assay_data.get("JAX_ASSAY_TEST_DATE")
            if dob_str and test_date_str:
                dob = datetime.strptime(dob_str, "%Y-%m-%d")
                test_date = datetime.strptime(test_date_str, "%Y-%m-%d")
                weeks_diff = (test_date - dob).days / 7
                if not (dob + timedelta(weeks=3.5) <= test_date <= dob + timedelta(weeks=18)):
                    comments = f"Test date is {weeks_diff:.2f} weeks after DOB"
                    table_rows.append([barcode, experiment_barcode, assay_data.get("Barcode", ""), comments, "Procedure QC Failed"])

# Create a DataFrame from the table rows
assay_data_df = pd.DataFrame(table_rows, columns=["Mouse_ID", "Experiment_Barcode", "Assay_Data_Barcode", "Comments", "Assay_Fail_Reason"])

# Convert IMPC codes to column headers
impc_columnheader = pd.read_excel('/projects/galaxy/tools/cba/impc_column_header.xlsx')

# Create a dictionary from the lookup DataFrame for quick lookup
lookup_dict = dict(zip(impc_columnheader['Code'], impc_columnheader['Column_header']))

# Function to replace IMPC code with column header in comments
def replace_impc_with_header(comment):
    # Find all IMPC codes in the comment
    impc_codes = re.findall(r'IMPC_\w+', comment)
    for code in impc_codes:
        if code in lookup_dict:
            comment = comment.replace(code, lookup_dict[code])
    return comment

# Apply the function to the Comments column in assay_data_df
updated_assay_data_df = assay_data_df.copy()
updated_assay_data_df['Comments'] = updated_assay_data_df['Comments'].apply(replace_impc_with_header)

# Merge mouse_df with assay_data_df on Mouse_ID
merged_df = pd.merge(updated_assay_data_df, mouse_df, on="Mouse_ID", how="outer")

# Merge the result with metadata_df on Experiment_Barcode
merged_df = pd.merge(merged_df, metadata_df, on="Experiment_Barcode", how="outer")

# Combine the Comments columns into a single column
merged_df['Comments'] = merged_df['Comments_x'].combine_first(merged_df['Comments_y']).combine_first(merged_df['Comments'])

# Drop the extra Comments columns
merged_df.drop(columns=['Comments_x', 'Comments_y'], inplace=True)

# Fill missing values with NaN
merged_df.fillna(value=pd.NA, inplace=True)

# Get the current date
current_date = datetime.now().strftime("%Y%m%d")

# Write the merged table to an Excel file with the current date as the filename
if len(sys.argv) > 1:
	file_name = sys.argv[1]
else:
	file_name = f"QC_validation_header_final0_{current_date}.csv"
	
merged_df.to_csv(file_name, index=False)