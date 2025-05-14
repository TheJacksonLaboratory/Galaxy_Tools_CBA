import sqlite3
import pandas as pd
import sys
import os       
import runQuery
from datetime import datetime, timedelta
import configparser

connection = None


cba_pertinent_experiments = [
"CBA_2D_X_RAY_CRANIAL_FACIAL_EXPERIMENT",
"CBA_2D_X_RAY_SKELETAL_EXPERIMENT",
"CBA_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT",
"CBA_BASELINE_GLUCOSE_EXPERIMENT",
"CBA_BASIC_ECHOCARDIOGRAPHY_EXPERIMENT",
"CBA_BODY_WEIGHT_EXPERIMENT",
"CBA_DEXA_EXPERIMENT",
"CBA_ELECTRORETINOGRAPHY_EXPERIMENT",
"CBA_FEAR_CONDITIONING_EXPERIMENT",
"CBA_FRAILTY_EXPERIMENT",
"CBA_GLUCOSE_TOLERANCE_TEST_EXPERIMENT",
"CBA_GRIP_STRENGTH_EXPERIMENT",
"CBA_GROOMING_EXPERIMENT",
"CBA_GROSS_MORPHOLOGY_EXPERIMENT",
"CBA_GTT_PLUS_INSULIN_EXPERIMENT",
"CBA_INDIRECT_CALORIMETRY_EXPERIMENT",
"CBA_INDIRECT_CALORIMETRY_24H_FAST_REFEED_EXPERIMENT",
"CBA_INSULIN_TOLERANCE_TEST_EXPERIMENT",
"CBA_INTRAOCULAR_PRESSURE_EXPERIMENT",
"CBA_LIGHT_DARK_BOX_EXPERIMENT",
"CBA_MICRO_CT_EXPERIMENT",
"CBA_NMR_BODY_COMPOSITION_EXPERIMENT",
"CBA_NON_INVASIVE_BLOOD_PRESSURE_EXPERIMENT",
"CBA_NOVEL_OBJECT_RECOGNITION_EXPERIMENT",
"CBA_OPEN_FIELD_EXPERIMENT",
"CBA_PIEZO_5_DAY_EXPERIMENT",
"CBA_PYRUVATE_TOLERANCE_TEST_EXPERIMENT",
"CBA_ROTAROD_EXPERIMENT",
"CBA_SUBCUTANEOUS_TEMPERATURE_EXPERIMENT",
"CBA_TAIL_SUSPENSION_TEST_EXPERIMENT",
"CBA_TREADMILL_MEP_EXPERIMENT",
"CBA_UNCONSCIOUS_ELECTROCARDIOGRAM_EXPERIMENT",
"CBA_VISUAL_EVOKED_POTENTIAL_EXPERIMENT",
"CBA_Y_MAZE_DELAYED_SPATIAL_RECOGNITION_EXPERIMENT",
"CBA_Y_MAZE_SPONTANEOUS_ALTERNATION_EXPERIMENT"
]

cba_pertinent_experiments_test = [
"CBA_PIEZO_5_DAY_EXPERIMENT",
]

cba_pertinent_experiments_old = [
    'CBA_BODY_WEIGHT_EXPERIMENT',  # Has EXPERIMENT_INSTRUMENT 
    'CBA_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT', # Has EXPERIMENT_INSTRUMENT
        'CBA_BASELINE_GLUCOSE_EXPERIMENT',
        'CBA_DEXA_EXPERIMENT',
    'CBA_BASIC_ECHOCARDIOGRAPHY_EXPERIMENT',  # Has EXPERIMENT_INSTRUMENT
        'CBA_FEAR_CONDITIONING_EXPERIMENT',
        'CBA_FRAILTY_EXPERIMENT',
            'CBA_GLUCOSE_CLAMPS_EXPERIMENT', # No records
        'CBA_GLUCOSE_TOLERANCE_TEST_EXPERIMENT',
        'CBA_GRIP_STRENGTH_EXPERIMENT', 
        'CBA_GTT_PLUS_INSULIN_EXPERIMENT',
            'CBA_HEART_WEIGHT_EXPERIMENT', # No records
    'CBA_INDIRECT_CALORIMETRY_24H_FAST_REFEED_EXPERIMENT', # Has EXPERIMENT_INSTRUMENT
        'CBA_INSULIN_TOLERANCE_TEST_EXPERIMENT',
        'CBA_INTRAOCULAR_PRESSURE_EXPERIMENT',
            'CBA_MAGNETIC_RESONANCE_IMAGING_EXPERIMENT', # No records
    'CBA_MICRO_CT_EXPERIMENT',  # Has EXPERIMENT_INSTRUMENT
            'CBA_MRI_BRAIN_EXPERIMENT', # No records
            'CBA_MMTT_PLUS_HORMONE_EXPERIMENT', # No records
        'CBA_NMR_BODY_COMPOSITION_EXPERIMENT',
    'CBA_NON_INVASIVE_BLOOD_PRESSURE_EXPERIMENT',# Has EXPERIMENT_INSTRUMENT
    'CBA_PIEZO_5_DAY_EXPERIMENT',  # Has EXPERIMENT_INSTRUMENT
            'CBA_PIEZOELECTRIC_SLEEP_MONITOR_SYSTEM_EXPERIMENT', # No records
            'CBA_PYRUVATE_TOLERANCE_TEST_EXPERIMENT', # No records
    'CBA_UNCONSCIOUS_ELECTROCARDIOGRAM_EXPERIMENT',  # Has EXPERIMENT_INSTRUMENT
            'CBA_VOLUNTARY_RUNNING_WHEELS_EXPERIMENT' # No records
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
                          my_filter,
                          source,
                          SERVICE_USERNAME, 
                          SERVICE_PASSWORD
                          ):   
    try:    
        newObj = runQuery.CBAAssayHandler(cbbList, requestList, templateList, \
            from_test_date, to_test_date, publishedBool, unpublishedBool, inactiveBool, summaryBool, jaxstrain, SERVICE_USERNAME, SERVICE_PASSWORD,my_filter,source) 
            
        tupleList = (newObj.controller())
        return tupleList
    except Exception as e:
        print(e)
        return None
   
    
    
def build_data_warehouse(source,pertinent_experiments,SERVICE_USERNAME, SERVICE_PASSWORD):
    
    connection = create_database(f"/projects/galaxy/tools/cba/data/{source}-warehouse.db")
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
    try:
        
        # Dates are experiment CREATE DATEs
        epoch_date = None
        if source == 'CBA':
            epoch_date =  datetime(2019, 1, 9)  # CBA epoch 1/9/2019
        else:
            epoch_date =  datetime(2024, 1, 1) # The KOMP epoch 
            
            
        current_date = datetime.now()
        create_from_test_date = epoch_date
        create_to_test_date = epoch_date + timedelta(days=120) # 4 months later
                
        for experiment in pertinent_experiments:
            create_from_test_date = epoch_date
            create_to_test_date = epoch_date + timedelta(days=120) # 4 months later
            formatted_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # For debugging...
            
            templateList = [experiment]  # Can't handle multiple templates in this version of the code  
            complete_response_ls = []
            print(experiment + "  " + formatted_time)
            
            while create_to_test_date <=  current_date: 
                my_filter = f" Created ge {datetime.strftime(create_from_test_date, '%Y-%m-%dT%H:%M:%SZ')} and Created le {datetime.strftime(create_to_test_date, '%Y-%m-%dT%H:%M:%SZ')}"
               
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
                                my_filter,
                                source,
                                SERVICE_USERNAME, 
                                SERVICE_PASSWORD
                                )
                
                print("Number of tuples:" + str(len(tuple_ls)) + ", " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                print("From test date:" + str(create_from_test_date) + ", To test date: " + str(create_to_test_date))
                
                complete_response_ls.extend(tuple_ls)
                create_from_test_date = create_to_test_date + timedelta(days=1) # Start the next batch at the day after the last one
                create_to_test_date = create_to_test_date + timedelta(days=120) # ~4 months later
                
            print("      Total of responses: " + str(len(complete_response_ls)))
            if table_exists(connection,templateList[0]):
                drop_sql = f"DROP TABLE IF EXISTS  {templateList[0]}"
                # Delete all rows from the table before inserting new data
                connection.execute(drop_sql)
            
            for my_tuple in complete_response_ls:
                _,df = my_tuple  
                #print(list(df.columns))
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

def table_exists(conn, table_name):
    """
    Checks if a table exists in a SQLite database.

    Args:
        conn: Open connection to the SQLite database file.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,)
        )
        result = cursor.fetchone()

        return result is not None
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False


def create_sqlite_view(connection, source, view_name:str, sql:str):
    # Create a view in the SQLite database
    connection = sqlite3.connect(f"/projects/galaxy/tools/cba/data/{source}-warehouse.db")
    cursor = connection.cursor()
    cursor.execute(f'DROP VIEW IF EXISTS {view_name}')
    cursor.execute(f'CREATE VIEW IF NOT EXISTS {view_name} AS {sql}')
    connection.commit()
    cursor.close()  # Note the close() here
    connection.close()  # Close the connection after creating the view

def create_komp_strain_view():
    komp_strains = """
    SELECT DISTINCT Strain  FROM KOMP_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_BODY_COMPOSITION_EXPERIMENT
    UNION 
    SELECT DISTINCT Strain FROM KOMP_BODY_WEIGHT_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_CLINICAL_BLOOD_CHEMISTRY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_ELECTROCARDIOGRAM_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_ELECTRORETINOGRAPHY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_EYE_MORPHOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_GLUCOSE_TOLERANCE_TEST_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_GRIP_STRENGTH_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_HEART_WEIGHT_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_HEMATOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_HOLEBOARD_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_LIGHT_DARK_BOX_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_OPEN_FIELD_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_SHIRPA_DYSMORPHOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_STARTLE_PPI_EXPERIMENT
    """
    create_sqlite_view(connection,'KOMP','vKompStrain',komp_strains)
       
def create_komp_strain_view():
    komp_strains = """
    SELECT DISTINCT Strain  FROM KOMP_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_BODY_COMPOSITION_EXPERIMENT
    UNION 
    SELECT DISTINCT Strain FROM KOMP_BODY_WEIGHT_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_CLINICAL_BLOOD_CHEMISTRY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_ELECTROCARDIOGRAM_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_ELECTRORETINOGRAPHY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_EYE_MORPHOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_GLUCOSE_TOLERANCE_TEST_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_GRIP_STRENGTH_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_HEART_WEIGHT_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_HEMATOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_HOLEBOARD_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_LIGHT_DARK_BOX_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_OPEN_FIELD_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_SHIRPA_DYSMORPHOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Strain FROM KOMP_STARTLE_PPI_EXPERIMENT
    """
    create_sqlite_view(connection,'KOMP','vKompStrain',komp_strains)
              
def create_komp_experiment_view():
    komp_exps = """
    SELECT DISTINCT Experiment  FROM KOMP_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_BODY_COMPOSITION_EXPERIMENT
    UNION 
    SELECT DISTINCT Experiment FROM KOMP_BODY_WEIGHT_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_CLINICAL_BLOOD_CHEMISTRY_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_ELECTROCARDIOGRAM_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_ELECTRORETINOGRAPHY_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_EYE_MORPHOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_GLUCOSE_TOLERANCE_TEST_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_GRIP_STRENGTH_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_HEART_WEIGHT_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_HEMATOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_HOLEBOARD_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_LIGHT_DARK_BOX_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_OPEN_FIELD_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_SHIRPA_DYSMORPHOLOGY_EXPERIMENT
    UNION
    SELECT DISTINCT Experiment FROM KOMP_STARTLE_PPI_EXPERIMENT
    """
    create_sqlite_view(connection,'KOMP','vKompExperiment',komp_exps)
       
def main():
    # Get arg c from command line 
    
    public_config = configparser.ConfigParser()
    public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
    SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]
    
    DATABASE_DIR = public_config["CORE LIMS"]["database_dir"]

    private_config = configparser.ConfigParser()
    private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
    SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

    source = sys.argv[1]  
    #source = 'CBA' # For testing purposes
    if source == 'CBA':
        build_data_warehouse('CBA',cba_pertinent_experiments,SERVICE_USERNAME, SERVICE_PASSWORD)
        # TODO Create CBA views here
        #e. g. create_cba_strain_view()
    elif source == 'KOMP':
        build_data_warehouse('KOMP',komp_pertinent_experiments,SERVICE_USERNAME, SERVICE_PASSWORD)
        create_komp_strain_view()
        create_komp_experiment_view()
    else:
        print("Please specify either CBA or KOMP")
        return


if __name__ == "__main__":
    main()