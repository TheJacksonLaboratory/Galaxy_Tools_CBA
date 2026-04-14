import argparse 
import runQuery
import sys
import configparser  
import json
from datetime import datetime
import pandas as pd
from io import BytesIO as IO
import sqlite3
import xlsxwriter
import os
import create_sqlite_dw
"""
    This module is used to generate a general report for the KOMP Project.
    It also contains the functions that produce the data warehouse.
    
    It gets data from a number of diffent KOMP experiments that have a body weight attribute.
    The data is stored in a data warehouse. The data warehouse is a CSV file that is
    read into a pandas dataframe. The data is then filtered based on the commandline.
    
    The report is generated from the data warehouse and is based on the commandline
    arguments passed to the script. The script is called by Galaxy and the report
    is written to an Excel file.
    
    Galaxy calls main() which parses the commandline arguments and then calls fetch_report()
"""

def create_database():
    # Create a database file with the given name
    connection = sqlite3.connect("/projects/galaxy/tools/cba/data/JAXLIMS-warehouse.db")
    return connection

def close_connection(conn):
    conn.close()

# Turn a comma separated list on the command into a python list
def returnList(pList):
    if len(pList) == 0:
        return []
    elif ',' in pList:
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
                 jaxlims_sample_ls, 
                 mouse_name_substring,
                 study_ls,
                 experiment_ls, 
                 from_test_date, 
                 to_test_date, 
                 publishedBool, 
                 unpublishedBool, 
                 inactiveBool, 
                 summaryBool, 
                 jaxstrain_ls, 
                 experiment_status_ls
                 ):
    # Generate the report from the data warehouse based on the commandline args.
    
    try:
        # Open the data source
        conn = create_database() 
        df_ls=[]
        # Right now we bring back all columns
        select_query = "SELECT * FROM "
        # What if the user did not specify an experiment? QUery all experiments
        where_clause =""
        if len(experiment_ls) == 0:
            experiment_ls = create_sqlite_dw.jaxlims_pertinent_experiments # Build an individual query for each experiment (template)
        final_df = pd.DataFrame()
        
        # WE OR together the criteria within an area and AND the areas together
        for experiment in experiment_ls:
            experiment = experiment.replace(" ", "_").replace('&', '_').replace('/', '_') 
            select_query = "SELECT * FROM `" + experiment + "`"
            where_clause =""
            
            sample_in_clause = ""
            jaxstrain_in_clause = ""
            experiment_status_in_clause = ""
            # For the other filters or them together with a context then and them all together
            
            sample_in_clause = build_in_clause(jaxlims_sample_ls)
            
            if(len(sample_in_clause) > 0):
                where_clause += f"OrganismID {sample_in_clause} AND "
            elif len(mouse_name_substring) > 0:
                where_clause += f"OrganismID LIKE '{mouse_name_substring}%' AND "   
            
            study_in_clause = build_in_clause(study_ls)
            if(len(study_in_clause) > 0):
                where_clause += f"StudyName {study_in_clause} AND "

            jaxstrain_in_clause = build_in_clause(jaxstrain_ls)
            if(len(jaxstrain_in_clause) > 0):
                where_clause += f"StockNumber {jaxstrain_in_clause} AND "
            
            experiment_status_in_clause = build_in_clause(experiment_status_ls)
            if(len(experiment_status_in_clause) > 0):
                where_clause += f"ProcedureStatus {experiment_status_in_clause.replace('_',' ')} AND "
               
            # If the user specified a date range then add it to the where clause
            
            if from_test_date != None:
                where_clause += f"DateComplete >= '{from_test_date}' AND "
            if to_test_date != None:
                where_clause += f"DateComplete <= '{to_test_date}' AND "

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
    except sqlite3.Error as e:
        print("Error opening database: ", e)
        print(repr(e))
    except Exception as e:
        print(repr(e))
    finally:
        close_connection(conn)
	
    
    
    # Create Excel file
    
    newObj = runQuery.CBAAssayHandler([], [], experiment_ls, \
            from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, \
                summaryBool, jaxstrain_ls, 'svc-corePFS@jax.org', 'hRbP&6K&(Qvw','','KOMP')   # TODO - Get from config file
    
    data = newObj.writeFile(df_ls)
    sys.stdout.buffer.write(data.getbuffer())
    
    return


def main():
    # Called by Galaxy. 
    # Parse the args,
    # Either build the data warehouse or produce a report
    # If the 'w' option is set the other args are irrelevant.
    parser = argparse.ArgumentParser() 
    parser.add_argument("-r", "--jaxlims_sample", help = "Show Output", nargs='?', const='')
    parser.add_argument("-e", "--experiment", help = "Show Output", nargs='?', const='')
    parser.add_argument("-s", "--experiment_status", help = "Show Output", nargs='?', const='')
    parser.add_argument("-j", "--jaxstrain", help = "Show Output", nargs='?', const='')
    parser.add_argument("-f", "--from_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-t", "--to_test_date", help = "Show Output", nargs='?', const='')
    parser.add_argument("-c", "--mouse_name_substring", help = "Show Output", nargs='?', const='')
    parser.add_argument("-p", "--study", help = "Show Output", nargs='?', const='')
    #parser.add_argument("-o", "--options", help = "Show Output", nargs='?', const='')
    parser.add_argument("-u", "--user", help = "Show Output")
    args = parser.parse_args() 
   
    # Get credentials from the config file
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]
    
    # Initialize the non-list variables
    publishedBool = False
    unpublishedBool = False
    inactiveBool = False
    summaryBool = True
    f_from_test_date = ''
    f_to_test_date = ''
    # Do these make sense in the body weight reports?
    # if args.options != None:
    #     for opt in args.options.split(","):
    #         publishedBool = True if opt == 'p' else publishedBool
    #         inactiveBool = True if opt == 'i' else inactiveBool
    #         unpublishedBool = True if opt == 'u' else unpublishedBool
    
    # Get the lists of filter values from the command line
    jaxlims_sample_ls = returnList(args.jaxlims_sample) if args.jaxlims_sample else []
    experiment_ls = returnList(args.experiment) if args.experiment else []
    jaxstrain_ls = returnList(args.jaxstrain) if args.jaxstrain else [] 
    # Remove underscore from any items in the list. Replace with a space
    args.experiment_status = args.experiment_status.replace('_',' ') if args.experiment_status else []  
    experiment_status_ls = returnList(args.experiment_status) if args.experiment_status else []  
    mouse_name_substring = args.mouse_name_substring if args.mouse_name_substring else ''
    study_ls = returnList(args.study) if args.study else []
    
    # Format the dates
    if args.from_test_date:
        f_from_test_date = datetime.strftime(datetime.strptime(args.from_test_date, '%Y-%m-%d'), '%Y-%m-%d')
    else:
        f_from_test_date = None
    if args.to_test_date:
        f_to_test_date = datetime.strftime(datetime.strptime(args.to_test_date, '%Y-%m-%d'), '%Y-%m-%d') 
    else:
        f_to_test_date = None
 
    #test_query()
    
    
    report_data = fetch_report(jaxlims_sample_ls,
                mouse_name_substring,
                study_ls,
                experiment_ls, 
                f_from_test_date, 
                f_to_test_date, 
                publishedBool, 
                unpublishedBool, 
                inactiveBool, 
                summaryBool, 
                jaxstrain_ls,
                experiment_status_ls)
             
    return

if __name__ == "__main__":
    main()
