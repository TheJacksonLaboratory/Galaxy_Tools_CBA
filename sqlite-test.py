import sqlite3
import pandas as pd
import getpass   
import runQuery
import datetime


connection = None

def create_table():
    # Create a connection to the SQLite database
    connection = sqlite3.connect('example.db')
    cursor = connection.cursor()

    # Create a table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER NOT NULL
        )
    ''')

    # Commit the changes and close the connection
    connection.commit()
    connection.close()          

def create_database(name:str):
    # Create a database file with the given name
    connection = sqlite3.connect(name)
    return connection
     
def close_db(connection):
    # Close the database connection
    connection.close()      

def insert_data(table_name:str, value_ls:list):
    # Insert data into the specified table
    connection = sqlite3.connect('/projects/galaxy/tools/cba/data/example.db')
    cursor = connection.cursor()
    # Start a transaction
    connection.execute('BEGIN TRANSACTION')
    
    # Prepare the SQL statement with placeholders for the values
    placeholders = ', '.join(['?'] * len(value_ls))
    sql = f'INSERT INTO {table_name} VALUES ({placeholders})'

    # Execute the SQL statement with the provided values
    cursor.execute(sql, value_ls)
    
    # End transaction
    connection.execute('COMMIT TRANSACTION')
    
    # Commit the changes and close the connection
    connection.commit()
    connection.close()   
    
    
    
# User has specified the "w" option. Build the data warehouse
def get_experiments(cbbList, 
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
   
    
    
def build_data_warehouse(SERVICE_USERNAME, SERVICE_PASSWORD):
    
    connection = create_database('/projects/galaxy/tools/cba/data/warehouse.db')
    
    pertinent_experiments = [
    'CBA_BODY_WEIGHT_EXPERIMENT',
    'CBA_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT',
    'CBA_BASELINE_GLUCOSE_EXPERIMENT',
    'CBA_DEXA_EXPERIMENT',
    'CBA_BASIC_ECHOCARDIOGRAPHY_EXPERIMENT',
    'CBA_FRAILTY_EXPERIMENT',
    #'CBA_GLUCOSE_TOLERANCE_EXPERIMENT',
    'CBA_GRIP_STRENGTH_EXPERIMENT',  # No data
    'CBA_GTT_PLUS_INSULIN_EXPERIMENT',
    'CBA_HEART_WEIGHT_EXPERIMENT',  # No data! Why?
    'CBA_INDIRECT_CALORIMETRY_24H_FAST_REFEED_EXPERIMENT',
    'CBA_INSULIN_TOLERANCE_TEST_EXPERIMENT',
    'CBA_INTRAOCULAR_PRESSURE_EXPERIMENT',
    'CBA_MMTT_PLUS_HORMONE_EXPERIMENT',
    'CBA_NMR_BODY_COMPOSITION_EXPERIMENT',
    'CBA_PIEZO_5_DAY_EXPERIMENT', 
    'CBA_PIEZOELECTRIC_SLEEP_MONITOR_SYSTEM_EXPERIMENT', 
    'CBA_PYRUVATE_TOLERANCE_TEST_EXPERIMENT',
    'CBA_UNCONSCIOUS_ELECTROCARDIOGRAM_EXPERIMENT'
    ]
    
    # Filters
    requestList = []        
    cbbList = []
    from_test_date = ''
    to_test_date = ''
    publishedBool = False   # Unused
    unpublishedBool = False # Unused
    inactiveBool = False    # Unused
    summaryBool = True      # Unused
    jaxstrain = '' # Unused
    templateList = ['CBA_BODY_WEIGHT_EXPERIMENT'] # Just need some value to get the batches. Doesn't matter what it is.
    try:
        batch_ls = []
        queryObj = runQuery.BatchBarcodeRequestHandler(cbbList, requestList, templateList, \
                    from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,'CBA') 
                
        # Get the batches   
        tupleList = (queryObj.controller())
        for my_tuple in tupleList:
                    barcode_ls = my_tuple['Barcode'] # Just want the barcode
                    for val in barcode_ls:
                        batch_ls.append(val)   
                
                
        for experiment in pertinent_experiments:
            formatted_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(experiment + ": " + formatted_time)
            
            # Trying at the experiment level    
            templateList = [experiment]  # Can't handle multiple templates in this version of the code  
            lower = 0
            upper = 15
            complete_response_ls = []
            while lower < len(batch_ls):
                cbbList = batch_ls[lower:upper]  
                tuple_ls = get_experiments(cbbList, 
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
                upper += 15
                print("      Number of responses from this request: " + str(len(tuple_ls)))
                if len(tuple_ls) > 0:
                    complete_response_ls.extend(tuple_ls)
                
            #pd.set_option('display.max_columns', None)
            
            print("      Total of responses: " + str(len(complete_response_ls)))
            for my_tuple in complete_response_ls:
                _,df = my_tuple  
                df.insert(loc=0,column="ExperimentName",value=templateList[0])
                df.fillna('', inplace = True)
                
                try:
                    print("          Number of rows in dataframe: " + str(df.shape[0]))
                    df.to_sql(templateList[0], connection, if_exists='append', index=False)
                except Exception as e:
                    print(e)
                
    except Exception as e:
        print(e)     
    finally:
        close_db(connection)
    return 


    
def main():
    usernme = getpass.getuser()
    print("User: " + usernme)
    
    build_data_warehouse('svc-corePFS@jax.org', 'hRbP&6K&(Qvw')



if __name__ == "__main__":
    main()