import argparse 
import runQuery
import sys
import io
import os
import configparser  
import json
from datetime import datetime

def returnList(pList):
    if ',' in pList:
        return pList.split(',')
    elif pList == 'None':
        return []
    else: return [pList]

def main():
    parser = argparse.ArgumentParser() 
    parser.add_argument("-r", "--request", help = "Show Output", nargs='?', const='')
    parser.add_argument("-b", "--batch", help = "Show Output",  nargs='?', const='')
    parser.add_argument("-e", "--experiment", help = "Show Output", nargs='?', const='')
    parser.add_argument("-f", "--from_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-t", "--to_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-o", "--options", help = "Show Output", nargs='?', const='')
    parser.add_argument("-u", "--user", help = "Show Output")
    parser.add_argument("-j", "--jaxstrain", help = "Show Output", nargs='?', const='')
    args = parser.parse_args() 
   
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

         
    if not(has_komp_access(args.user, SERVICE_USERNAME, SERVICE_PASSWORD)):
        raise Exception("User %s does not have access to KOMP" % args.user) 

    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = False
    jaxstrain = ''
    
    for opt in args.options.split(","):
        publishedBool = True if opt == 'p' else publishedBool
        inactiveBool = True if opt == 'i' else inactiveBool
        summaryBool = True if opt == 's' else summaryBool
        unpublishedBool = True if opt == 'u' else unpublishedBool
    
    cbbList = returnList(args.batch) if args.batch else []

    requestList = returnList(args.request) if args.request else []
    #raise Exception(str(requestList))
    templateList = returnList(args.experiment) if args.experiment else []
    
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
    
    newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
        f_from_test_date, f_to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, \
		jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,'','KOMP') 
        
    dfList = (newObj.controller())
    data = newObj.writeFile(dfList)

    sys.stdout.buffer.write(data.getbuffer())

def has_komp_access(user, service_username, service_password):
    has_komp_access = False
    check_access_query = runQuery.QueryHandler(service_username, service_password)
    employee_string = f"EMPLOYEE_LIMITED?&expand=PROJECT&$filter=contains(CI_USERNAME, '{user.lower()}') and PROJECT/any(a:a/Name eq 'Center for Biometric Analysis')"
    result_data = check_access_query.runQuery(check_access_query.queryBase + employee_string, 'xml')
    json_data = json.loads(result_data)
    if len(json_data['value']) > 0:
        has_komp_access = True
    return has_komp_access
   



if __name__ == "__main__":
    main()


