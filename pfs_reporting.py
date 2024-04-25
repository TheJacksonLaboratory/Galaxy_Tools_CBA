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

def main_debug():
    """
    parser.add_argument("-r", "--request", help = "Show Output", nargs='?', const='')
    parser.add_argument("-b", "--batch", help = "Show Output",  nargs='?', const='')
    parser.add_argument("-e", "--experiment", help = "Show Output", nargs='?', const='')
    parser.add_argument("-f", "--from_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-t", "--to_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-o", "--options", help = "Show Output", nargs='?', const='')
    parser.add_argument("-u", "--user", help = "Show Output")
    parser.add_argument("-j", "--jaxstrain", help = "Show Output", nargs='?', const='')
    """

    if not(has_cba_access('michael.mcfarland@jax.org', 'svc-corePFS@jax.org', 'hRbP&6K&(Qvw')):
        raise Exception("User %s does not have access to CBA" % 'michael.mcfarland@jax.org') 

    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = False
    jaxstrain = ''
    
    opt = 's'
    publishedBool = True if opt == 'p' else publishedBool
    inactiveBool = True if opt == 'i' else inactiveBool
    summaryBool = True if opt == 's' else summaryBool
    unpublishedBool = True if opt == 'u' else unpublishedBool
    
    batch = 'CBB110'
    #batch = 'KOMPB1'
    cbbList = returnList(batch) if batch else []
    #request = 'CBA110'
    request = None
    requestList = returnList(request) if request else []
    experiment = ''
    templateList = returnList(experiment) if experiment else []
    
    from_test_date=None
    to_test_date=None
    if from_test_date:
        f_from_test_date = datetime.strftime(datetime.strptime(from_test_date, '%m-%d-%Y'), '%Y-%m-%d')
    else:
        f_from_test_date = None
    if to_test_date:
        f_to_test_date = datetime.strftime(datetime.strptime(to_test_date, '%m-%d-%Y'), '%Y-%m-%d') 
    else:
        f_to_test_date = None
 
    jaxstrain = None
    if jaxstrain:
        jaxstrain = jaxstrain
        
    newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
        f_from_test_date, f_to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, 'svc-corePFS@jax.org', 'hRbP&6K&(Qvw','KOMP') # Need to add unpublishedBool
        
    dfList = (newObj.controller())
    data = newObj.writeFile(dfList)
    sys.stdout.buffer.write(data.getbuffer())


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
    public_config.read("./config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("./config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

         

    if not(has_cba_access(args.user, SERVICE_USERNAME, SERVICE_PASSWORD)):
        raise Exception("User %s does not have access to CBA" % args.user) 

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
        f_from_test_date, f_to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD) # Need to add unpublishedBool
        
    dfList = (newObj.controller())
    data = newObj.writeFile(dfList)
    sys.stdout.buffer.write(data.getbuffer())

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
    main_debug()


