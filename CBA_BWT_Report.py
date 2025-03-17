import argparse 
import runQuery
import sys
import configparser  
import json
from datetime import datetime
import pandas as pd
import csv
from io import BytesIO as IO
"""
    This module is used to generate a report of body weight data for the CBA.
    It also contains the functions that produce the data warehouse.
    The report is generated from the data warehouse and is based on the commandline
    arguments passed to the script. The script is called by Galaxy and the report
    is written to an Excel file.
    
    Galaxy calls main() which parses the commandline arguments and then calls fetch_report()
    The warehouse builder calls body_weight_data_warehouse()
"""
def returnList(pList):
    if len(pList) == 0:
        return []
    elif ',' in pList:
        return pList.split(',')
    elif pList == 'None':
        return []
    else: return [pList]

def main():
    # Called by Galaxy
    parser = argparse.ArgumentParser() 
    parser.add_argument("-r", "--request", help = "Show Output", nargs='?', const='')
    parser.add_argument("-b", "--batch", help = "Show Output",  nargs='?', const='')
    parser.add_argument("-e", "--experiment", help = "Show Output", nargs='?', const='')
    parser.add_argument("-f", "--from_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-t", "--to_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-o", "--options", help = "Show Output", nargs='?', const='')
    parser.add_argument("-u", "--user", help = "Show Output")
    parser.add_argument("-j", "--jaxstrain", help = "Show Output", nargs='?', const='')
    parser.add_argument("-w", "--build_data_warehouse", help = "Show Output", nargs='?', const='')
    args = parser.parse_args() 
   
    # Get credentials from the config file
    public_config = configparser.ConfigParser()
    public_config.read("./config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("./config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

    # Check if the user has access to CBA
    if not(has_komp_access(args.user, SERVICE_USERNAME, SERVICE_PASSWORD)):
        raise Exception("User %s does not have access to CBA" % args.user) 

    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = False
    f_from_test_date = ''
    f_to_test_date = ''
    build_data_warehouse = str(args.build_data_warehouse).lower() == 'true'
    
    for opt in args.options.split(","):
        publishedBool = True if opt == 'p' else publishedBool
        inactiveBool = True if opt == 'i' else inactiveBool
        summaryBool = True if opt == 's' else summaryBool
        unpublishedBool = True if opt == 'u' else unpublishedBool
    
    cbbList = returnList(args.batch) if args.batch else []

    requestList = returnList(args.request) if args.request else []
    templateList = returnList(args.experiment) if args.experiment else []
    
    # Format the dates
    if args.from_test_date:
        f_from_test_date = datetime.strftime(datetime.strptime(args.from_test_date, '%m-%d-%Y'), '%Y-%m-%d')
    else:
        f_from_test_date = None
    if args.to_test_date:
        f_to_test_date = datetime.strftime(datetime.strptime(args.to_test_date, '%m-%d-%Y'), '%Y-%m-%d') 
    else:
        f_to_test_date = None
 
    jaxstrainLs = returnList(args.jaxstrain) if args.jaxstrain else [] 
        
    if build_data_warehouse == True:
        # Only body weight for now
        body_weight_data_warehouse(SERVICE_USERNAME, SERVICE_PASSWORD)
    else:
        report_data = fetch_report(cbbList,requestList, 
                 templateList, 
                 f_from_test_date, 
                 f_to_test_date, 
                 publishedBool, 
                 unpublishedBool, 
                 inactiveBool, 
                 summaryBool, 
                 jaxstrainLs)
                
    return

def fetch_report(cbbList, 
                 requestList, 
                 templateList, 
                 from_test_date, 
                 to_test_date, 
                 publishedBool, 
                 unpublishedBool, 
                 inactiveBool, 
                 summaryBool, 
                 jaxstrainLs
                 ):
    # Generate the report from the so-called data warehouse based on the commandline args.
    dw_df = pd.read_csv('data/CBA_BWT_raw_data.csv')
    
    # Filter the data
    #Start with CBA_Request
    dw_df = filter(dw_df,"CBA_Request",requestList)
    print(dw_df)
    # Next Experiment
    dw_df = filter(dw_df,"ExperimentName",templateList)
    print(dw_df)
    # Next Batch
    dw_df = filter(dw_df,"CBA_Batch",cbbList)
    print(dw_df)
    
    # Next JAX Strain
    jaxstrainLs = [element for element in jaxstrainLs if len(element) > 0]
    dw_df = filter(dw_df,"Strain",jaxstrainLs)
    print(dw_df)
    
    # Next Date Range
    if from_test_date:
        dw_df = dw_df[dw_df['Experiment_Date'] >= from_test_date]
    if to_test_date:    
        dw_df = dw_df[dw_df['Experiment_Date'] <= to_test_date] 
    # Write the data to a file
    dw_df.to_csv("data/CBA_BWT.csv",index=False)
    write_to_excel(dw_df)
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
                          SERVICE_USERNAME, 
                          SERVICE_PASSWORD
                          ):   
    try:    
        newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
            from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,'CBA') 
            
        tupleList = (newObj.controller())
        return tupleList
    except Exception as e:
        print(e)
        return None
    
def body_weight_data_warehouse(SERVICE_USERNAME, SERVICE_PASSWORD):
    
    pertinent_experiments = [
    'CBA_BODY_WEIGHT_EXPERIMENT',
    'CBA_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT',
    'CBA_BASELINE_GLUCOSE_EXPERIMENT',
    'CBA_DEXA_EXPERIMENT',
        # 'CBA_BASIC_ECHOCARDIOGRAPHY_EXPERIMENT', -- No BWTs In Mouse Details tab?
        # 'CBA_FEAR_CONDITIONING_EXPERIMENT',  No data -- but it looks like attribute exists. REV navigation issue exists
    'CBA_FRAILTY_EXPERIMENT',
        # 'CBA_GLUCOSE_CLAMPS_EXPERIMENT',  No data
        # 'CBA_GLUCOSE_TOLERANCE_EXPERIMENT',  No REV_EXPERIMENT_BATCH_CBA_GLUCOSE_TOLERANCE_EXPERIMEN
    'CBA_GRIP_STRENGTH_EXPERIMENT',
    'CBA_GTT_PLUS_INSULIN_EXPERIMENT',
    'CBA_HEART_WEIGHT_EXPERIMENT',  # No data! Why?
    'CBA_INDIRECT_CALORIMETRY_24H_FAST_REFEED_EXPERIMENT',
    'CBA_INSULIN_TOLERANCE_TEST_EXPERIMENT',
    'CBA_INTRAOCULAR_PRESSURE_EXPERIMENT',
        # 'CBA_MAGNETIC_RESONANCE_IMAGING_EXPERIMENT',  No Data
        # 'CBA_MICRO_CT_EXPERIMENT',  Data but No BWTs
    'CBA_MMTT_PLUS_HORMONE_EXPERIMENT',
    'CBA_NMR_BODY_COMPOSITION_EXPERIMENT',
        # 'CBA_NON-INVASIVE_BLOOD_PRESSURE_EXPERIMENT', Dash in name may be an issue!
        # 'CBA_PIEZO_5_DAY EXPERIMENT',  No REV navigation
        # 'CBA_PIEZOELECTRIC_SLEEP_MONITOR_SYSTEM_EXPERIMENT', No data
        # 'CBA_PYRUVATE_TOLERANCE_TEST_EXPERIMENT', No data
    'CBA_UNCONSCIOUS_ELECTROCARDIOGRAM_EXPERIMENT'
        # 'CBA_VOLUNTARY_RUNNING_WHEELS_EXPERIMENT' No data
        ]
    
    keep_columns = [
        'ExperimentName',
        'CBA_Request',
        'CBA_Batch',
        'Sample',
        'Sex',
        'Genotype',
        'Strain',
        'User_Defined_Strain_Name',
        'Primary_ID_Value',
        'Whole_Mouse_Fail',
        'Whole_Mouse_Fail_Reason',
        'Experiment',
        'Experiment_Date',
        'Tester_Name',
        'Age_(wks)',
        'Experiment_Barcode',
        'Body_Weight_(g)',
        'Body_Weight_QC',
        'Secondary_ID_Value',
        'Entire_Assay_Fail_Reason',
        'Entire_Assay_Fail_Comments']
        
    # Initialize the variables
    requestList = []        
    cbbList = '' 
    from_test_date = ''
    to_test_date = ''
    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = True
    jaxstrain = ''
    
    try:
        # Open the file once
        f = open("./data/BIG_ASS_raw_data.csv", 'w', encoding='utf-8')
        # Write keep_columns as CSV header line
        csvwriter = csv.writer(f)
        csvwriter.writerow(keep_columns)
                
        # Get the experiments that have body weights
        for experiment in pertinent_experiments:
            templateList =  [experiment]
            # For each experiment get the batches 
            # 1. Setup the query
            newObj = runQuery.CBABatchBarcodeRequestHandler(cbbList, requestList, templateList, \
                from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,'CBA') 
            # 2. get the batches   
            tupleList = (newObj.controller())
            
            batch_ls = []
            for my_tuple in tupleList:
                barcode_ls = my_tuple['Barcode'] # Just want the barcode
                for val in barcode_ls:
                    batch_ls.append(val)   
            
            # batch_ls = ['CBB462','CBB534','CBB535','CBB536','CBB538','CBB539','CBB537'] #  DEBUGGING
            # Pass in 5 batches at a time for the current experiment
            lower = 0
            upper = 5
            complete_response_ls = []
            while lower < len(batch_ls):
                cbbList = batch_ls[lower:upper]
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
                                SERVICE_USERNAME, 
                                SERVICE_PASSWORD
                                )
                lower = upper
                upper += 5
                complete_response_ls.extend(tuple_ls)
                
            # Get the last batch
            
            for my_tuple in complete_response_ls:
                _,df = my_tuple 
                df.insert(loc=0,column="ExperimentName",value=templateList[0])  
                df = relevantColumnsOnly(keep_columns,df)
                df.to_csv(f,encoding='utf-8', errors='replace', index=False, header=False)
            #f.close()
    except Exception as e:
        print(e)    
    finally:
        f.close()    
    return 

def relevantColumnsOnly(keep_columns,df): 
    # 1. Change the column names
    change_names = {"odd_body_weight_name": "Body_Weight_(g)"}
    for key in change_names:
        df.rename(columns={key: change_names[key]}, inplace=True)
    
    # 2. Drop the slop
    for col in df.columns:
        if col not in keep_columns:
            df.drop(col, axis=1, inplace=True)
    # 3. Make sure the columns are in the dataframe
    for col in keep_columns:
        if col not in df.columns:
            df[col] = ''
            print("Added column:" + col)
    return df


def filter(df_list,column_name,filter_list):
    # Take the already winnowed list and keep only the rows that match the filter
    if len(filter_list) == 0:
        return df_list
    
    filtered_list = pd.DataFrame()
    for filter in filter_list:
        tmp_df = df_list[df_list[column_name] == filter]
        filtered_list = pd.concat([filtered_list,tmp_df])
    return filtered_list


def has_komp_access(user, service_username, service_password):
    has_komp_access = False
    check_access_query = runQuery.QueryHandler(service_username, service_password)
    employee_string = f"EMPLOYEE?&expand=PROJECT&$filter=contains(CI_USERNAME, '{user.lower()}') and PROJECT/any(a:a/Name eq 'Center for Biometric Analysis')"
    result_data = check_access_query.runQuery(check_access_query.queryBase + employee_string, 'xml')
    json_data = json.loads(result_data)
    if len(json_data['value']) > 0:
        has_komp_access = True
    return has_komp_access
   
# For each experiment and for each batch get the body weight 
# data as well as other info wrt experiment, batch, animal, etc.


if __name__ == "__main__":
    main()


# To get batches for the body weight experiments
"""
    https://jacksonlabs.platformforscience.com/PROD/odata/CBA_BATCH?$expand=REV_EXPERIMENT_BATCH_CBA_BODY_WEIGHT_EXPERIMENT&$select=Barcode&$count=true
"""