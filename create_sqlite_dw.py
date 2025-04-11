import sqlite3
import pandas as pd
import getpass   
import runQuery
import datetime


connection = None
cba_pertinent_experiments = [
    'CBA_BODY_WEIGHT_EXPERIMENT',
        'CBA_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT',
        'CBA_BASELINE_GLUCOSE_EXPERIMENT',
        'CBA_DEXA_EXPERIMENT',
        'CBA_BASIC_ECHOCARDIOGRAPHY_EXPERIMENT',
        'CBA_FEAR_CONDITIONING_EXPERIMENT',
        'CBA_FRAILTY_EXPERIMENT',
        'CBA_GLUCOSE_CLAMPS_EXPERIMENT',
        'CBA_GLUCOSE_TOLERANCE_TEST_EXPERIMENT',
        'CBA_GRIP_STRENGTH_EXPERIMENT', 
        'CBA_GTT_PLUS_INSULIN_EXPERIMENT',
        'CBA_HEART_WEIGHT_EXPERIMENT',
        'CBA_INDIRECT_CALORIMETRY_24H_FAST_REFEED_EXPERIMENT',
        'CBA_INSULIN_TOLERANCE_TEST_EXPERIMENT',
        'CBA_INTRAOCULAR_PRESSURE_EXPERIMENT',
        'CBA_MAGNETIC_RESONANCE_IMAGING_EXPERIMENT',
        'CBA_MICRO_CT_EXPERIMENT',
        'CBA_MMTT_PLUS_HORMONE_EXPERIMENT',
        'CBA_NMR_BODY_COMPOSITION_EXPERIMENT',
        'CBA_NON-INVASIVE_BLOOD_PRESSURE_EXPERIMENT',
        'CBA_PIEZO_5_DAY_EXPERIMENT',  
        'CBA_PIEZOELECTRIC_SLEEP_MONITOR_SYSTEM_EXPERIMENT',
        'CBA_PYRUVATE_TOLERANCE_TEST_EXPERIMENT',
        'CBA_UNCONSCIOUS_ELECTROCARDIOGRAM_EXPERIMENT',
        'CBA_VOLUNTARY_RUNNING_WHEELS_EXPERIMENT'
    ]
    
komp_pertinent_experiments = [
	'KOMP_BODY_WEIGHT_EXPERIMENT',
	'KOMP_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT',
	'KOMP_BODY_COMPOSITION_EXPERIMENT',
	'KOMP_CLINICAL_BLOOD_CHEMISTRY_EXPERIMENT',
	'KOMP_ELECTROCARDIOGRAM_EXPERIMENT',
	'KOMP_ELECTRORETINOGRAPHY_EXPERIMENT',
	'KOMP_EYE_MORPHOLOGY_EXPERIMENT',
	'KOMP_FUNDUS_IMAGING_EXPERIMENT',
	'KOMP_GLUCOSE_TOLERANCE_TEST_EXPERIMENT',
	'KOMP_GRIP_STRENGTH_EXPERIMENT',
	'KOMP_HEART_WEIGHT_EXPERIMENT',
	'KOMP_HEMATOLOGY_EXPERIMENT',
	'KOMP_HOLEBOARD_EXPERIMENT',
	'KOMP_LIGHT_DARK_BOX_EXPERIMENT',
	'KOMP_OPEN_FIELD_EXPERIMENT',
	'KOMP_SHIRPA_DYSMORPHOLOGY_EXPERIMENT',
	'KOMP_STARTLE_PPI_EXPERIMENT']
    
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
                          source,
                          SERVICE_USERNAME, 
                          SERVICE_PASSWORD
                          ):   
    try:    
        newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
            from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,source) 
            
        tupleList = (newObj.controller())
        return tupleList
    except Exception as e:
        print(e)
        return None
   
    
    
def build_data_warehouse(source,pertinent_experiments,SERVICE_USERNAME, SERVICE_PASSWORD):
    
    connection = create_database(f"/projects/galaxy/tools/cba/data/{source}-warehouse-test.db")
    impertinent_experiments = ['KOMP_STARTLE_PPI_EXPERIMENT']
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
    templateList = []
    templateList.append(impertinent_experiments[0]) # Just need some value to get the batches. Let's go with body weight
    try:
        batch_ls = []
        queryObj = runQuery.BatchBarcodeRequestHandler(cbbList, requestList, templateList, \
                    from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,source) 
                
        # Get the batches   
        tupleList = (queryObj.controller())
        for my_tuple in tupleList:
                    barcode_ls = my_tuple['Barcode'] # Just want the barcode
                    for val in barcode_ls:
                        batch_ls.append(val)   
                
                
        for experiment in impertinent_experiments:
            formatted_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            #print(experiment + ": " + formatted_time)
            
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
                                source,
                                SERVICE_USERNAME, 
                                SERVICE_PASSWORD
                                )
                lower = upper
                upper += 15
                #print("      Number of responses from this request: " + str(len(tuple_ls)))
                complete_response_ls.extend(tuple_ls)
                
                formatted_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                #print(experiment + ": " + formatted_time)
            #pd.set_option('display.max_columns', None)
            
            #print("      Total of responses: " + str(len(complete_response_ls)))
            for my_tuple in complete_response_ls:
                _,df = my_tuple  
                df.insert(loc=0,column="ExperimentName",value=templateList[0])
                df.fillna('', inplace = True)
                
                try:
                    print(templateList[0] + ": Number of rows in dataframe: " + str(df.shape[0]))
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
    source = 'KOMP'
    
    
    if source == 'CBA':
        build_data_warehouse(source,cba_pertinent_experiments,'svc-corePFS@jax.org', 'hRbP&6K&(Qvw')
    elif source == 'KOMP':
        build_data_warehouse(source, komp_pertinent_experiments,'svc-corePFS@jax.org', 'hRbP&6K&(Qvw')
    else:
        print("Please specify a source: CBA or KOMP")
        return
    # Create the SQLite database and table



if __name__ == "__main__":
    main()