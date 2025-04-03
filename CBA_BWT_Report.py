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
    
    It gets data from a number of diffent CBA experiments that have a body weight attribute.
    The data is stored in a data warehouse. The data warehouse is a CSV file that is
    read into a pandas dataframe. The data is then filtered based on the commandline.
    
    The report is generated from the data warehouse and is based on the commandline
    arguments passed to the script. The script is called by Galaxy and the report
    is written to an Excel file.
    
    Galaxy calls main() which parses the commandline arguments and then calls fetch_report()
    The warehouse builder calls body_weight_data_warehouse()
"""
ROOT_DIR = '.'


# Stanard function to check if a user has access to the CBA
def has_core_access(user, service_username, service_password):
    has_komp_access = False
    check_access_query = runQuery.QueryHandler(service_username, service_password)
    employee_string = f"EMPLOYEE?&expand=PROJECT&$filter=contains(CI_USERNAME, '{user.lower()}') and PROJECT/any(a:a/Name eq 'Center for Biometric Analysis')"
    result_data = check_access_query.runQuery(check_access_query.queryBase + employee_string, 'xml')
    json_data = json.loads(result_data)
    if len(json_data['value']) > 0:
        has_komp_access = True
    return has_komp_access


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

def fetch_report(cbbList, 
                 requestList, 
                 templateList, 
                 from_test_date, 
                 to_test_date, 
                 publishedBool, 
                 unpublishedBool, 
                 inactiveBool, 
                 summaryBool, 
                 jaxstrainLs,
                 miceLs
                 ):
    # Generate the report from the so-called data warehouse based on the commandline args.
    
    # Get the whole shebang then start removing rows that do not match the filter
    dw_df = pd.read_csv('/projects/galaxy/tools/cba/data/CBA_BWT_raw_data.csv')

    #Start with CBA_Request
    dw_df = filter(dw_df,"CBA_Request",requestList)
    #print(dw_df)
    # Next Experiment
    dw_df = filter(dw_df,"ExperimentName",templateList)
    #print(dw_df)
    # Next Batch
    dw_df = filter(dw_df,"CBA_Batch",cbbList)
    #print(dw_df)
    
    # Next JAX Strain
    jaxstrainLs = [element for element in jaxstrainLs if len(element) > 0]
    dw_df = filter(dw_df,"Strain",jaxstrainLs)
    #print(dw_df)
    
    # Next Date Range
    if from_test_date:
        dw_df = dw_df[dw_df['Experiment_Date'] >= from_test_date]
    if to_test_date:    
        dw_df = dw_df[dw_df['Experiment_Date'] <= to_test_date] 
    
    dw_df = filter(dw_df,"Sample",cbbList)
    
    # Write the data to a file
    dw_df.to_csv(sys.stdout,index=False)
    dw_df.to_csv("/projects/galaxy/tools/cba/data/CBA_BWT.csv",index=False)
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
    # For each pertinent experiment 1) get the batches and then 2) get the body weight data.
    # pertinent_experiments = ['CBA_FEAR_CONDITIONING_EXPERIMENT']  # DEBUG
    pertinent_experiments = [
        'CBA_BODY_WEIGHT_EXPERIMENT',
        'CBA_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT',
        'CBA_BASELINE_GLUCOSE_EXPERIMENT',
        'CBA_DEXA_EXPERIMENT',
        'CBA_BASIC_ECHOCARDIOGRAPHY_EXPERIMENT',
        # 'CBA_FEAR_CONDITIONING_EXPERIMENT',  No data -- but it looks like attribute exists. REV navigation issue exists
        'CBA_FRAILTY_EXPERIMENT',
        # 'CBA_GLUCOSE_CLAMPS_EXPERIMENT',  No data
    #'CBA_GLUCOSE_TOLERANCE_EXPERIMENT', Waiting for edits
        'CBA_GRIP_STRENGTH_EXPERIMENT',  # No data
        'CBA_GTT_PLUS_INSULIN_EXPERIMENT',
    'CBA_HEART_WEIGHT_EXPERIMENT',  # No data! Why?
        'CBA_INDIRECT_CALORIMETRY_24H_FAST_REFEED_EXPERIMENT',
        'CBA_INSULIN_TOLERANCE_TEST_EXPERIMENT',
        'CBA_INTRAOCULAR_PRESSURE_EXPERIMENT',
        # 'CBA_MAGNETIC_RESONANCE_IMAGING_EXPERIMENT',  No Data
        # 'CBA_MICRO_CT_EXPERIMENT',  Data but No BWTs i.e "JAX_ASSAY_BODYWEIGHT": null,
        'CBA_MMTT_PLUS_HORMONE_EXPERIMENT',
        'CBA_NMR_BODY_COMPOSITION_EXPERIMENT',
        # 'CBA_NON-INVASIVE_BLOOD_PRESSURE_EXPERIMENT', Dash in name may be an issue!
        'CBA_PIEZO_5_DAY_EXPERIMENT',  # No BODYWEIGHT but JAX_ASSAY_PIEZO_PREWEIGHT and JAX_ASSAY_PIEZO_POSTWEIGHT. How do we get batch lists?
    'CBA_PIEZOELECTRIC_SLEEP_MONITOR_SYSTEM_EXPERIMENT', # JAX_ASSAY_PIEZO_PREWEIGHT, JAX_ASSAY_PIEZO_POSTWEIGHT. How do we get batch lists?
    'CBA_PYRUVATE_TOLERANCE_TEST_EXPERIMENT',
        'CBA_UNCONSCIOUS_ELECTROCARDIOGRAM_EXPERIMENT'
        # 'CBA_VOLUNTARY_RUNNING_WHEELS_EXPERIMENT' "JAX_ASSAY_BODYWEIGHT_STARTDAY": null, "JAX_ASSAY_BODYWEIGHT_ENDDAY": null,
    ]
    
    # The columns that will be available in the data warehouse
    keep_columns = [
        'ExperimentName',
        'CBA_Request',
        'CBA_Batch',
        'Sample',
        'Pen',
        'Sex',
        'Genotype',
        'Strain',
        'User_Defined_Strain_Name',
        "Primary_ID",
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
        'Entire_Assay_Fail_Reason',
        'Entire_Assay_Fail_Comments']
        
    # Initialize the variables
    requestList =   []    
    cbbList = '' 
    from_test_date = ''
    to_test_date = ''
    publishedBool = False   # Unused
    unpublishedBool = False # Unused
    inactiveBool = False    # Unused
    summaryBool = True      # Unused
    jaxstrain = ''          # Unused
    templateList =  ['CBA_BODY_WEIGHT_EXPERIMENT'] 
    try:
        # Open the file once
        f = open('/projects/galaxy/tools/cba/data/CBA_BWT_raw_data.csv', 'w', encoding='utf-8') # The data warehouse is currently a CSV file
        # Write keep_columns as CSV header line
        csvwriter = csv.writer(f)
        csvwriter.writerow(keep_columns)
                
        batch_ls = []
        newObj = runQuery.BatchBarcodeRequestHandler(cbbList, requestList, templateList, \
                    from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,'CBA') 
                
                # 2. get the batches   
        tupleList = (newObj.controller())
        for my_tuple in tupleList:
                    barcode_ls = my_tuple['Barcode'] # Just want the barcode
                    for val in barcode_ls:
                        batch_ls.append(val)   
                
                
        # Get the batch of experiments that have body weights - just once
        for experiment in pertinent_experiments:
            templateList =  [experiment]

            # For each experiment Pass in 5 batches at a time for the current experiment
            lower = 0
            upper = 20
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
                upper += 20
                complete_response_ls.extend(tuple_ls)
                
            # Get the last batch
            pd.set_option('display.max_columns', None)
            for my_tuple in complete_response_ls:
                _,df = my_tuple  
                df.insert(loc=0,column="ExperimentName",value=templateList[0])
                #print(df)
                df = relevantColumnsOnly(keep_columns,df)
                df.fillna('', inplace = True)
                #print(df)
                df = df[keep_columns]
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
    change_names = {"Pre-weight_(g)": "Body_Weight_(g)", "Pre-weight_(g)": "Body_Weight_(g)" }
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
    parser.add_argument("-r", "--request", help = "Show Output", nargs='?', const='')
    parser.add_argument("-b", "--batch", help = "Show Output",  nargs='?', const='')
    parser.add_argument("-e", "--experiment", help = "Show Output", nargs='?', const='')
    parser.add_argument("-f", "--from_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-t", "--to_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-o", "--options", help = "Show Output", nargs='?', const='')
    parser.add_argument("-u", "--user", help = "Show Output")
    parser.add_argument("-j", "--jaxstrain", help = "Show Output", nargs='?', const='')
    parser.add_argument("-m", "--mice", help = "Show Output", nargs='?', const='')
    parser.add_argument("-w", "--build_data_warehouse", help = "Show Output", nargs='?', const='')
    args = parser.parse_args() 
   
    # Get credentials from the config file
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]
    #ROOT_DIR = public_config["CORE LIMS"]["root_dir"]   
    
    # Check if the user has access to CBA
    if not(has_core_access(args.user, SERVICE_USERNAME, SERVICE_PASSWORD)):
        raise Exception("User %s does not have access to CBA" % args.user) 

    # Initialize the variables
    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = False
    f_from_test_date = ''
    f_to_test_date = ''
    
    # If true then simply build the data warehouse
    build_data_warehouse = str(args.build_data_warehouse).lower() == 'true'
    
    # Do these make sense in the body weight reports?
    if args.options != None:
        for opt in args.options.split(","):
            publishedBool = True if opt == 'p' else publishedBool
            inactiveBool = True if opt == 'i' else inactiveBool
            summaryBool = True if opt == 's' else summaryBool
            unpublishedBool = True if opt == 'u' else unpublishedBool
    
    cbbList = returnList(args.batch) if args.batch else []
    requestList = returnList(args.request) if args.request else []
    templateList = returnList(args.experiment) if args.experiment else []
    jaxstrainLs = returnList(args.jaxstrain) if args.jaxstrain else [] 
    miceList = returnList(args.mice) if args.mice else [] 
    
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
                 jaxstrainLs,
                 miceList)
                
    return
   
if __name__ == "__main__":
    main()
