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
    args = parser.parse_args() 
   
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

    publishedBool = False
    inactiveBool = False
    summaryBool = False

    cbbList = returnList(args.batch) if args.batch else [] 

    requestList = returnList(args.request) if args.request else ["CBA193"]
    # print(requestList)
    
    templateList = returnList(args.experiment) if args.experiment else []
    
    from_test_date = None

    if from_test_date:
        f_from_test_date = datetime.strftime(datetime.strptime(args.from_test_date, '%m-%d-%Y'), '%Y-%m-%d')
    else:
        f_from_test_date = None

    to_test_date = None

    if to_test_date:
        f_to_test_date = datetime.strftime(datetime.strptime(args.to_test_date, '%m-%d-%Y'), '%Y-%m-%d') 
    else:
        f_to_test_date = None
 
    
    newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
        f_from_test_date, f_to_test_date, publishedBool, inactiveBool, summaryBool, SERVICE_USERNAME, SERVICE_PASSWORD)
        
    dfList = (newObj.controller())
    print(dfList[0][0], dfList[0][1].size)

    data = newObj.writeFile(dfList)

    data.seek(0)
    with open("output.txt", "wb") as f:  #!works
       f.write(data.getbuffer())
    # sys.stdout.buffer.write(data.getbuffer())

def has_cba_access(user, service_username, service_password):
    has_cba_access = False
    check_access_query = runQuery.QueryHandler(service_username, service_password)
    employee_string = f"EMPLOYEE?&expand=PROJECT&$filter=contains(CI_USERNAME, '{user.lower()}') and PROJECT/any(a:a/Name eq 'Center for Biometric Analysis')"
    result_data = check_access_query.runQuery(check_access_query.queryBase + employee_string, 'xml')
    json_data = json.loads(result_data)
    if len(json_data['value']) > 0:
        has_cba_access = True
    return has_cba_access
   



if __name__ == "__main__":
    main()


