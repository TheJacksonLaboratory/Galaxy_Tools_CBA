import argparse 
import runQuery
import sys
import configparser  
import json
from datetime import datetime

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
    if ',' in pList:
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
    jaxstrain = ''
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
 
    if args.jaxstrain:
        jaxstrain = args.jaxstrain

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
                 jaxstrain)
                
        sys.stdout.buffer.write(report_data) # Excel data that Galaxy will redirect to an Excel file
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
                 jaxstrain, 
                 SERVICE_USERNAME, 
                 SERVICE_PASSWORD
                 ):
    # Generate the report from the so-called data warehouse based on the commandline args.
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
    
    newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
        from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,'CBA') 
        
    tupleList = (newObj.controller())
    # Write the Dataframes to a CSV file
    # Open the CSV for adding data
    f = open('./data/KOMP_BWT_Report.csv', 'a', encoding='utf-8')
    for my_tuple in tupleList:
        exp_name,df = my_tuple
        # Remove blank lines
        df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df.dropna(how='all', inplace=True)
        df.to_csv(f,encoding='utf-8', errors='replace', index=False)
    f.close()
    
def body_weight_data_warehouse(SERVICE_USERNAME, SERVICE_PASSWORD):
    
    # Get the experiments that have body weights
    # For each experiment get the batches 
    requestList = []
    templateList = ['CBA_BODY_WEIGHT_EXPERIMENT']  # TESTING
    cbbList = ['CBB1774']  # Batches  # TESTING
    from_test_date = ''
    to_test_date = ''
    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = True
    jaxstrain = ''

    build_data_warehouse(cbbList, 
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
    return 

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