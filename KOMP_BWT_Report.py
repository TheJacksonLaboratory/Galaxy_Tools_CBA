import argparse 
import runQuery
import sys
import configparser  
import json
from datetime import datetime, timedelta
import pandas as pd
import csv
from io import BytesIO as IO
import pandas as pd
import sqlite3

"""
    This module is used to generate a report of body weight data for the KOMP Project.
    It also contains the functions that produce the data warehouse.
    
    It gets data from a number of diffent KOMP experiments that have a body weight attribute.
    The data is stored in a data warehouse. The data warehouse is a CSV file that is
    read into a pandas dataframe. The data is then filtered based on the commandline.
    
    The report is generated from the data warehouse and is based on the commandline
    arguments passed to the script. The script is called by Galaxy and the report
    is written to an Excel file.
    
    Galaxy calls main() which parses the commandline arguments and then calls fetch_report()
    The warehouse builder calls body_weight_data_warehouse()
"""

pertinent_experiments = [
    'KOMP_BODY_WEIGHT_EXPERIMENT',
    'KOMP_GRIP_STRENGTH_EXPERIMENT',
    'KOMP_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT',
    'KOMP_BODY_COMPOSITION_EXPERIMENT',
    'KOMP_GLUCOSE_TOLERANCE_TEST_EXPERIMENT',
    'KOMP_HEART_WEIGHT_EXPERIMENT']
    
    # The columns that will be available in the data warehouse
keep_columns = [
    "ExperimentName",
    "Sample",
    "Customer_Mouse_ID",
    "Body_Weight_(g)",
    "Pen",
    "Sex",
    "Genotype",
    "Strain_Name",
    "Strain",
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
    "Experiment_Date",
    "Experiment_Status",
    "Protocol_Name",
    "Assay_Tester_Name",
    "Experiment_Barcode"]
        

# Turn a comma separated list on the command into a python list
def returnList(pList):
    if len(pList) == 0:
        return []
    elif ',' in pList:
        return pList.split(',')
    elif pList == 'None':
        return []
    else: return [pList]


# Remove the rows fom the dataframe that do not match the filter
def filter(df_list,column_name,filter_list):
    # Take the already winnowed list and keep only the rows that match the filter
    if len(filter_list) == 0:  # No filter
        return df_list
    
    filtered_list = pd.DataFrame()
    for filter in filter_list:
        tmp_df = df_list[df_list[column_name] == filter]
        filtered_list = pd.concat([filtered_list,tmp_df])
    return filtered_list

def fetch_report(komp_customer_id_ls,
                 komp_sample_ls, 
                 templateList, 
                 from_test_date, 
                 to_test_date, 
                 publishedBool, 
                 unpublishedBool, 
                 inactiveBool, 
                 summaryBool, 
                 jaxstrain_ls,
                 experiment_barcode_ls
                 ):
    # Generate the report from the so-called data warehouse based on the commandline args.
    
    # Get the whole shebang then start removing rows that do not match the filter
    dw_df = pd.read_csv('/projects/galaxy/tools/cba/data/KOMP_BWT_raw_data.csv')

    #Start with 
    dw_df = filter(dw_df,"Customer_Mouse_ID",komp_customer_id_ls)
    # Next MUS name
    dw_df = filter(dw_df,"Sample",komp_sample_ls)
    # Next experiment name
    dw_df = filter(dw_df,"ExperimentName",templateList)
    
    dw_df = filter(dw_df,"Experiment_Barcode",experiment_barcode_ls)
    
    # Next JAX Strain
    dw_df = filter(dw_df,"Strain",jaxstrain_ls)
    
    # Next Date Range
    if from_test_date:
        dw_df = dw_df[dw_df['Experiment_Date'] >= from_test_date]
    if to_test_date:    
        dw_df = dw_df[dw_df['Experiment_Date'] <= to_test_date] 
    
    # Write the data to a file
    dw_df.to_csv(sys.stdout,index=False)
    #dw_df.to_csv("/projects/galaxy/tools/cba/data/KOMP_BWT.csv",index=False)
    #write_to_excel(dw_df)
    return


# Dump out data as an Excel file
def write_to_excel(df):
    
    excel_file = IO()
    xlwriter = pd.ExcelWriter(excel_file, 
                              date_format="YYYY-MM-DD",
                              datetime_format="YYYY-MM-DD HH:MM:SS",
                              engine='xlsxwriter')

    df.to_excel(xlwriter, sheet_name="BodyWeights", index=False)

    workbook = xlwriter.book
    worksheet = xlwriter.sheets["BodyWeights"]

    # set experiment sample numeric columns with PFS precision settings
    df_format = pd.DataFrame()

    # Do I need to do anything for formatting?
    
    xlwriter.close()
    excel_file.seek(0)  #reset to beginning
    sys.stdout.buffer.write(excel_file.getbuffer())
    return

    
# User has specified the "w" option. Build the data warehouse
def build_data_warehouse(cbbList, 
                          requestList, 
                          templateList, 
                          from_test_date, 
                          to_test_date, 
                          publishedBool, 
                          unpublishedBool, 
                          inactiveBool, 
                          summaryBool, 
                          jaxstrain, 
                          m_filter,
                          SERVICE_USERNAME, 
                          SERVICE_PASSWORD
                          ):   
    try:    
        newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
            from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,m_filter,'KOMP') 
            
        tupleList = (newObj.controller())
        return tupleList
    except Exception as e:
        print(e)
        return []

def body_weight_data_warehouse(SERVICE_USERNAME, SERVICE_PASSWORD):
    # Initialize the filter variables
    requestList = []        
    cbbList = '' 
    from_test_date = ''
    to_test_date = ''
    publishedBool = False   # Unused
    unpublishedBool = False # Unused
    inactiveBool = False    # Unused
    summaryBool = True      # Unused
    jaxstrain = ''          # Unused
    
    try:
        # Open the file once
        f = open("/projects/galaxy/tools/cba/data/KOMP_BWT_raw_data.csv", 'w', encoding='utf-8') # The data warehouse is currently a CSV file
        # Write keep_columns as CSV header line
        csvwriter = csv.writer(f)
        age = ["Age"]
        # Add Age to the header row
        csvwriter.writerow(keep_columns[0:4] + age + keep_columns[4:]) # Add the age column to the header
        
        # Dates are experiment START DATEs
        epoch_date =  datetime(2024, 3, 1) # The KOMP epoch  
        current_date = datetime.now()
        
        
    #EXPERIMENT/pfs.{experiment}/JAX_EXPERIMENT_STATUS eq 'Data Sent to DCC' and 
    # Created ge  2024-04-01T00:00:00Z and Created le  2024-05-01T00:00:00Z 

        # Start at the KOMP epoch and loop to the curent date 4 months at a time
        for experiment in pertinent_experiments:
            
            templateList = [experiment] # Consider just passing the whole list instead of one at a time
            complete_response_ls = []
            create_from_test_date = epoch_date
            create_to_test_date = epoch_date + timedelta(days=120) # 4 months later

            while create_to_test_date <=  current_date:
                my_filter = f" Created ge {datetime.strftime(create_from_test_date, '%Y-%m-%dT%H:%M:%SZ')} and Created le {datetime.strftime(create_to_test_date, '%Y-%m-%dT%H:%M:%SZ')}"
                
                tuple_ls = build_data_warehouse(cbbList, 
                                requestList, 
                                templateList, 
                                from_test_date,
                                to_test_date,
                                publishedBool, 
                                unpublishedBool, 
                                inactiveBool, 
                                summaryBool, 
                                jaxstrain, 
                                my_filter,
                                SERVICE_USERNAME, 
                                SERVICE_PASSWORD
                                )
                
                create_from_test_date = create_to_test_date + timedelta(days=1) # Start the next batch at the day after the last one
                create_to_test_date = create_to_test_date + timedelta(days=120) # ~4 months later
                complete_response_ls.extend(tuple_ls)
            
            # Get the last batch
            # pd.set_option('display.max_columns', None)
            for my_tuple in complete_response_ls:
                _,df = my_tuple  
                df.insert(loc=0,column="ExperimentName",value=templateList[0])
                # Remove unwanted columns and ensure we have the ones we need
                df = relevantColumnsOnly(keep_columns,df)
                df.fillna('', inplace = True)
                # Re-order the columns
                df = df[keep_columns]
                # Some special formating
                df['Experiment_Date'] = pd.to_datetime(df['Experiment_Date'])
                df['Date_of_Birth'] = pd.to_datetime(df['Date_of_Birth'])
                # Compute the age
                df.insert(loc=4,column="Age",value=df['Experiment_Date'] - df['Date_of_Birth'])
                df['Age'] = df['Age'].dt.days / 7
                # Organize them
                df = df.sort_values(by=['Sample','Age'],ascending=True)
                df.to_csv(f,encoding='utf-8', errors='replace', index=False, header=False)

    except Exception as e:
        print(e)    
    finally:
        f.close()    
    return 


def body_weight_data_warehouse_from_dw(SERVICE_USERNAME, SERVICE_PASSWORD):
    # For each pertinent experiment 1) get the batches and then 2) get the body weight data.
    
    try:
        # Open the file once
        f = open("/projects/galaxy/tools/cba/data/KOMP_BWT_raw_data.csv", 'w', encoding='utf-8') # The data warehouse is currently a CSV file
        # Write keep_columns as CSV header line
        csvwriter = csv.writer(f)
        age = ["Age"]
        # Add Age to the header row
        csvwriter.writerow(keep_columns[0:4] + age + keep_columns[4:]) # Add the age column to the header

        for experiment in pertinent_experiments:
            templateList = [experiment] # Consider just passing the whole list instead of one at a time
            
            # Open the SQLite db
            # Get * FROM the table in a dataframe 
            connection = sqlite3.connect('/projects/galaxy/tools/cba/data/KOMP-warehouse.db')
            # Get the data from the database
            query = f"SELECT * FROM {experiment}"
            df = pd.read_sql_query(query, connection)
            connection.close()
            
            # Remove unwanted columns and ensure we have the ones we need
            df = relevantColumnsOnly(keep_columns,df)
            df.fillna('', inplace = True)
            # Re-order the columns
            df = df[keep_columns]
            # Some special formating
            df['Experiment_Date'] = pd.to_datetime(df['Experiment_Date'])
            df['Date_of_Birth'] = pd.to_datetime(df['Date_of_Birth'])
            # Compute the age
            df.insert(loc=4,column="Age",value=df['Experiment_Date'] - df['Date_of_Birth'])
            df['Age'] = df['Age'].dt.days / 7
            # Organize them
            df = df.sort_values(by=['Sample','Age'],ascending=True)
            df.to_csv(f,encoding='utf-8', errors='replace', index=False, header=False)

    except Exception as e:
        print(e)    
    finally:
        f.close()    
    return 


# Clean up the dataframe by removing columns that are not in the keep_columns list, 
# add the ones that need to be there, and change any name that is non-standard.
def relevantColumnsOnly(keep_columns,df): 
    # 1. Change the column names that don't match keep_columns but are to be kept,eg JAX_ASSAY_PIEZO_PREWEIGHT
    change_names = {"Total_Tissue_Mass_(g)": "Body_Weight_(g)", "Pre-weight_(g)": "Body_Weight_(g)" }
    for key in change_names:
        df.rename(columns={key: change_names[key]}, inplace=True)
    
    # 2. Drop the slop
    exclude_cols = [col for col in df.columns if col not in keep_columns]
    df.drop(exclude_cols, axis=1, inplace=True)
    
    # 3. Make sure the required columns are in the dataframe
    idx = 0
    for col in keep_columns:
        if col not in df.columns:
            df.insert(idx,col,'')
        idx += 1
    return df

def main():
    # Called by Galaxy. 
    # Parse the args,
    # Either build the data warehouse or produce a report
    # If the 'w' option is set the other args are irrelevant.
    parser = argparse.ArgumentParser() 
    parser.add_argument("-r", "--komp_sample", help = "Show Output", nargs='?', const='')
    parser.add_argument("-b", "--komp_customer_id", help = "Show Output",  nargs='?', const='')
    parser.add_argument("-e", "--experiment", help = "Show Output", nargs='?', const='')
    parser.add_argument("-f", "--from_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-x", "--experiment_barcode", help = "Show Output", nargs='?', const='')
    parser.add_argument("-t", "--to_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-o", "--options", help = "Show Output", nargs='?', const='')
    parser.add_argument("-u", "--user", help = "Show Output")
    parser.add_argument("-j", "--jaxstrain", help = "Show Output", nargs='?', const='')
    parser.add_argument("-w", "--build_data_warehouse", help = "Show Output", nargs='?', const='')
    args = parser.parse_args() 
   
    # Get credentials from the config file
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]
    
    # Initialize the variables
    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = True
    f_from_test_date = ''
    f_to_test_date = ''
    
    # If true then simply build the data warehouse
    build_data_warehouse = str(args.build_data_warehouse).lower() == 'true'
    
    # Do these make sense in the body weight reports?
    if args.options != None:
        for opt in args.options.split(","):
            publishedBool = True if opt == 'p' else publishedBool
            inactiveBool = True if opt == 'i' else inactiveBool
            unpublishedBool = True if opt == 'u' else unpublishedBool
    
    komp_customer_id_ls = returnList(args.komp_customer_id) if args.komp_customer_id else []
    komp_sample_ls = returnList(args.komp_sample) if args.komp_sample else []
    templateList = returnList(args.experiment) if args.experiment else []
    jaxstrain_ls = returnList(args.jaxstrain) if args.jaxstrain else [] 
    experiment_barcode_ls = returnList(args.experiment_barcode) if args.experiment_barcode else []  
    
    # Format the dates
    if args.from_test_date:
        f_from_test_date = datetime.strftime(datetime.strptime(args.from_test_date, '%m-%d-%Y'), '%Y-%m-%d')
    else:
        f_from_test_date = None
    if args.to_test_date:
        f_to_test_date = datetime.strftime(datetime.strptime(args.to_test_date, '%m-%d-%Y'), '%Y-%m-%d') 
    else:
        f_to_test_date = None
 
        
    if build_data_warehouse == True:
        # Only body weight for now
        body_weight_data_warehouse_from_dw(SERVICE_USERNAME, SERVICE_PASSWORD)
    else:
        report_data = fetch_report(komp_customer_id_ls,komp_sample_ls, 
                 templateList, 
                 f_from_test_date, 
                 f_to_test_date, 
                 publishedBool, 
                 unpublishedBool, 
                 inactiveBool, 
                 summaryBool, 
                 jaxstrain_ls,
                 experiment_barcode_ls)
                
    return
   
if __name__ == "__main__":
    main()
