import collections
import os
import collections
from pathlib import Path
import pandas as pd
import os
import sys


"""
Algorithm:

Take in a list of files, parse and collect data from the .txt files of VEP raw data, 
transform it, and output to a .csv file.

"""

"""Utility functions"""

#Function to remove space and tab from a line in the .txt file
def strip_lines(lines):
    return [x.strip().split('\t') for x in lines]
    

#Function to convert tuple/pair to a dict
def tuple_to_dict(list_of_tuple, d):
    for (x, y) in list_of_tuple:
        d.setdefault(x, []).append(y)
    return d



def get_first_five_cols(lines, num_rows: int):
    """
    Function to generate information from the file and generate the first five columns(general information of mice),
    the result is like the following:

    Protocol,Steps,Channels,Animal # ,Study Name
    PCP Photopic Adapted Long Protocol06 [14806-D  ||  ECN 1496  ||  8 June 2020],5,4,6137-B,OM-243_C2

    """

    d = collections.defaultdict(list)
    protocol_name = lines[3].strip().split('\t') 
    steps = lines[7].strip().split('\t') 
    channel = lines[8].strip().split('\t') 
    animal_number = lines[9].strip().split('\t') 
    study_name = lines[10].strip().split('\t') 

    # Make n copy of data read above
    d[protocol_name[0]].extend([protocol_name[1]] * num_rows)
    d[steps[0]].extend([steps[1]] * num_rows)
    d[channel[0]].extend([channel[1]] * num_rows)
    d[animal_number[0]].extend([animal_number[1]] * num_rows)
    d[study_name[0]].extend([study_name[1]] * num_rows)
    
    df = pd.DataFrame.from_dict(d)

    return df


# Function to read data from files
def parse_file(filename, workspace):
    os.chdir(workspace)

    with open(filename, "r", encoding='utf-8',
              errors='ignore') as f:
        lines = f.readlines()
        columns = lines[16].strip().split() 
        print(columns)  
        # Reformat the column name of 'Cage #'
        if '#' in columns:
            columns.remove('#')
        columns[1] = 'Mouse Name'
        columns[2] = 'Cage #'

        num_rows = len(lines[17:33])
        first_five_cols = get_first_five_cols(lines, num_rows)
        # get data in marker's table section
        temp = strip_lines(lines[17:33])
        marker_table_data = pd.DataFrame.from_records(temp, columns=columns)
        res = pd.concat([first_five_cols, marker_table_data], axis=1)
        return res

def transform_files(file_list, workspace, outputFileName) -> None:

    if not file_list:
        return []

    result = []
    for file in file_list:
        df = parse_file(file, workspace)
        result.append(df)

    # Write data to the file
    final_data = pd.concat(result, ignore_index=True)
    # delete columns that are not wanted
    final_data.drop('Animal # ', axis=1, inplace=True)
    final_data.drop('Cage #', axis=1, inplace=True)
    final_data.drop('Age', axis=1, inplace=True)
    
    final_data.rename(columns={'ms':'Latency (ms)', 'uV': 'Amplitude (uV)', \
                                'R':'Result', 'S':'Stimulation', 'C': 'Channel', \
                                'Comment':'Comments','Name' : 'Waveform', 'Channels':'Total Channels'}, inplace=True)
    # Change some column names
    """
    final_data.rename(columns={'ms':'Latency (ms)', 'uV': 'Amplitude (uV)', \
                                'Animal # ':'LOT BARCODE', 'R':'Result', 'S':'Stimulation', 'C': 'Channel', \
                                'Comment':'Comments','Name' : 'Waveform', 'Channels':'Total Channels'}, inplace=True)
    """
    
    ## Re-sort. Note that dataframes are case sensitive when it comes to sorting.Uppercase comes before lower.
    #sorted_data = final_data.sort_values(by=['LOT BARCODE','Protocol', 'Stimulation', 'Eye','Waveform'])
    sorted_data = final_data.sort_values(by=['Protocol', 'Stimulation', 'Eye','Waveform'])
    sorted_data = sorted_data[sorted_data['Waveform'].isin(['P1','N1','P2'])]
    
    sorted_data.to_csv(outputFileName,sep=',')



def validateFiles(inputFile1):
    try:
        f1 = Path(inputFile1)
        if f1.exists() == False:
            print("First file in list does not exist.")
            return False
    except Exception as e:
        print(e)
        return False
    
    return True

def main():

    if len(sys.argv) < 3:
        print("Usage: python VEP_Transform.py inputfile1,...inputFileN outputFile")
        exit()

    inputFiles = sys.argv[1].split(',')
    outputFile = sys.argv[2]

    transform_files(inputFiles, '.', outputFile)

if __name__ == "__main__":
    main()

   
    
