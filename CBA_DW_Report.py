import argparse 
import runQuery
import sys
import io
import os
import configparser  
import json
from datetime import datetime
import pandas as pd
import sqlite3
import xlsxwriter
import os
import create_sqlite_dw

def create_database():
    # Create a database file with the given name
    connection = sqlite3.connect("/projects/galaxy/tools/cba/data/CBA-warehouse.db")
    return connection

def close_connection(conn):
    conn.close()


def returnList(pList):
    if ',' in pList:
        return pList.split(',')
    elif pList == 'None':
        return []
    else: return [pList]


def build_in_clause(value_ls):
    # Build an IN clause for the SQL query
    in_clause = ""
    for value in value_ls:
        in_clause += f"'{value}',"
    if len(in_clause) > 0:
        in_clause = " IN (" + in_clause[:-1] + ")"
    return in_clause    


def fetch_report(
					cbbList,	# CBA Batch
					from_test_date,
					to_test_date,
					inactiveBool,
					jaxstrain_ls,
					publishedBool,
					requestList, 	# REQUESTs
					summaryBool,
					template_ls, # EXPERIMENTs
					unpublishedBool
				):
    
    # Generate the report from the data warehouse based on the commandline args.
    
    try:
        # Open the data source
        conn = create_database()  # TBD - Get from config file
        
        # Right now we bring back all columns
        select_query = "SELECT * FROM "
        where_clause =""
        if len(template_ls) == 0: # What if the user did not specify an experiment? QUery all experiments
            template_ls = create_sqlite_dw.cba_pertinent_experiments # Build an individual query for each experiment (template)
        
        df_ls = [] # List of dataframes to be returned
        # WE OR together the criteria within an area and AND the areas together
        for experiment in template_ls:
            select_query = "SELECT * FROM "
            where_clause =""
            
            select_query += experiment
            
            req_in_clause = ""  
            batch_in_clause = ""
            jaxstrain_in_clause = "" 
            experiment_in_clause = ""
            
            req_in_clause = build_in_clause(requestList)
            if(len(req_in_clause) > 0):
                where_clause += f"CBA_Request {req_in_clause} AND "
            
            batch_in_clause = build_in_clause(cbbList)
            
            if(len(batch_in_clause) > 0):
                where_clause += f"CBA_Batch {batch_in_clause} AND "
                
            jaxstrain_in_clause = build_in_clause(jaxstrain_ls)
            if(len(jaxstrain_in_clause) > 0):
                where_clause += f"Strain {jaxstrain_in_clause} AND "
                
            experiment_in_clause = build_in_clause(template_ls)
            if(len(experiment_in_clause) > 0):
                where_clause += f"ExperimentName {experiment_in_clause} AND "
               
            # If the user specified a date range then add it to the where clause
            
            if from_test_date != None:
                where_clause += f"Experiment_Date >= '{from_test_date}' AND "
            if to_test_date != None:
                where_clause += f"Experiment_Date <= '{to_test_date}' AND "

            # Remove the last occurance of AND
            if where_clause.endswith(" AND "):
                where_clause = where_clause[:-4]    
                where_clause = " WHERE " + where_clause
                
            select_query += f" {where_clause} "
            try:
                df = pd.read_sql_query(select_query, conn)
                df_ls.append((experiment,df)) # A tuple of the experiment name and the dataframe
            except Exception as e:
                continue
            
        #final_df.to_csv(sys.stdout,index=False)  # Reaaly need excel output
    except sqlite3.Error as e:
        print("Error opening database: ", e)
        print(repr(e))
    except Exception as e:
        print(repr(e))
    finally:
        close_connection(conn)
	
    return df_ls


def main():
    
    parser = argparse.ArgumentParser() 
    parser.add_argument("-r", "--request", help = "Show Output", nargs='?', const='')
    parser.add_argument("-b", "--batch", help = "Show Output",  nargs='?', const='')
    parser.add_argument("-e", "--experiment", help = "Show Output", nargs='?', const='')
    parser.add_argument("-f", "--from_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-t", "--to_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-o", "--options", help = "Show Output", nargs='?', const='')
    parser.add_argument("-u", "--user", help = "Show Output")
    parser.add_argument("-j", "--jaxstrain", help = "Show Output", nargs='?', const='') # List
    args = parser.parse_args() 
    
    public_config = configparser.ConfigParser()
    public_config.read("./config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("./config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]
   

    if not(has_cba_access(args.user, SERVICE_USERNAME, SERVICE_PASSWORD)):
        raise Exception("User %s does not have access to CBA" % args.user) 

    cbbList = []    # CBA Batch
    f_from_test_date = ''
    f_to_test_date = ''
    inactiveBool = False
    jaxstrainList = []
    publishedBool = False
    requestList = []    # REQUESTs
    summaryBool = False
    templateList = []   # EXPERIMENTs
    unpublishedBool = False

    
    for opt in args.options.split(","):
        publishedBool = True if opt == 'p' else publishedBool
        inactiveBool = True if opt == 'i' else inactiveBool
        summaryBool = True if opt == 's' else summaryBool
        unpublishedBool = True if opt == 'u' else unpublishedBool
    
    cbbList = returnList(args.batch) if args.batch else []
    requestList = returnList(args.request) if args.request else []
    templateList = returnList(args.experiment) if args.experiment else []
    jaxstrainList = returnList(args.jaxstrain) if args.jaxstrain else []
	
    if args.from_test_date:
        f_from_test_date = datetime.strftime(datetime.strptime(args.from_test_date, '%m-%d-%Y'), '%Y-%m-%d')
    else:
        f_from_test_date = None
    if args.to_test_date:
        f_to_test_date = datetime.strftime(datetime.strptime(args.to_test_date, '%m-%d-%Y'), '%Y-%m-%d') 
    else:
        f_to_test_date = None
 
    dfList = fetch_report(
					cbbList,	# CBA Batch
					f_from_test_date,
					f_to_test_date,
					inactiveBool,
					jaxstrainList,
					publishedBool,
					requestList, 	# REQUESTs
					summaryBool,
					templateList, # EXPERIMENTs
					unpublishedBool)
    
    newObj = runQuery.CBAAssayHandler([], [], templateList, \
                f_from_test_date, f_to_test_date, publishedBool, unpublishedBool, inactiveBool, \
                summaryBool, jaxstrainList, 'svc-corePFS@jax.org', 'hRbP&6K&(Qvw','CBA')   # TODO - Get from config file
    		
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
    main()


