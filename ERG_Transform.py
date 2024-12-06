import collections
import os
import collections
from pathlib import Path
import pandas as pd
import os
import optparse
import sys


"""
Algorithm:

Parse and collect data from .txt file of PCP raw data, output a .csv file.

1. Group files based on its name, for example, 1-P.txt should be grouped with 1-S.txt
2. Extract data from a tuple/group of files(use different approach based on S/P file)
3. Merge all extracted data into one big excel/csv file

Usage eg:  python PCP_Transform.py -d path/to/your/work/directory -f outputfilename.csv   
"""

"""Utility functions"""


def get_file_extension(filename: str):
    return Path(filename).suffix.lower()


def get_field_prefix(fileName: str):
    return fileName.split("-")[0]


#Get type of a file, e.g it's a 'S' file or a 'T' file
def get_filed_sub(fileName: str) -> str:
    return fileName[2:3]


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

    # Read data in the file
    d = collections.defaultdict(list)
    protocol_name = lines[14].strip().split('\t')
    steps = lines[18].strip().split('\t')
    channel = lines[19].strip().split('\t')
    animal_number = lines[20].strip().split('\t')
    study_name = lines[21].strip().split('\t')

    # Make n copy of data read above
    d[protocol_name[0]].extend([protocol_name[1]] * num_rows)
    d[steps[0]].extend([steps[1]] * num_rows)
    d[channel[0]].extend([channel[1]] * num_rows)
    d[animal_number[0]].extend([animal_number[1]] * num_rows)
    d[study_name[0]].extend([study_name[1]] * num_rows)
    df = pd.DataFrame.from_dict(d)

    return df


# Function to group files based on their names, e.g 1-P.txt and 1-S.txt will be grouped together because they both
# have '1' as prefix
def organize_files(path: str):
    if not os.path.isdir(path):
        print(f"{path} is not a directory")
        return {}

    files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    file_dict = collections.defaultdict(list)
    for file in files:
        extention = get_file_extension(file)
        if extention != ".txt":
            continue
        file_key = get_field_prefix(file)
        group = file_dict.get(file_key, [])
        group.append(file)
        file_dict[file_key] = group

    return file_dict


# Function to read data from files
def parse_file(filename, file_type, workspace):
    os.chdir(workspace)

    with open(filename, "r", encoding='utf-8',
              errors='ignore') as f:
        lines = f.readlines()
        columns = lines[26].strip().split()

        # Reformat the column name of 'Cage #'
        columns.remove('#')
        columns[1] = 'Mouse Name'
        columns[2] = 'Cage #'

        if file_type == "P":
            num_rows = len(lines[27:47])
            first_five_cols = get_first_five_cols(lines, num_rows)
            # get data in marker's table section
            temp = strip_lines(lines[27:47])
            marker_table_data = pd.DataFrame.from_records(temp, columns=columns)
            res = pd.concat([first_five_cols, marker_table_data], axis=1)
            return res

        elif file_type == "S":
            num_rows = len(lines[27:63])
            first_five_cols = get_first_five_cols(lines, num_rows)
            # get data in marker's table section
            temp = strip_lines(lines[27:63])
            marker_table_data = pd.DataFrame.from_records(temp, columns=columns)
            res = pd.concat([first_five_cols, marker_table_data], axis=1)
            return res



def transform_files(file_list, workspace, outputFileName) -> None:

    if not file_list:
        return []

    result = []
    for file in file_list:
        # Read and aggregate data
        if isPfile(file):
            df = parse_file(file, "P", workspace)
        elif isSfile(file):
            df = parse_file(file, "S", workspace)
        else:
            continue

        result.append(df)

    # Write data to the file
    final_data = pd.concat(result, ignore_index=True)
    
    final_data.drop('Animal # ', axis=1, inplace=True)
    final_data.drop('Cage #', axis=1, inplace=True)
    final_data.drop('Age', axis=1, inplace=True)
    final_data.drop('Comment', axis=1, inplace=True)
    final_data.drop('C', axis=1, inplace=True)
    final_data.drop('Group', axis=1, inplace=True)
      
    # Change some column names
    final_data.rename(columns={'ms':'Latency (ms)', 'uV': 'Amplitude (uV)', \
                             'R':'Result', 'S':'Stimulation', \
                            'Name' : 'Waveform', 'Channels':'Total Channels'}, inplace=True)
    
    ## Re-sort. Note that dataframes are case sensitive when it comes to sorting.Uppercase comes before lower.
    #sorted_data = final_data.sort_values(by=['LOT BARCODE','Protocol', 'Stimulation', 'Waveform','Eye'])
    sorted_data = final_data.sort_values(by=['Mouse Name','Protocol', 'Stimulation','Waveform', 'Eye'])
    sorted_data.to_csv(outputFileName,sep=',')



def transform(file_groups: dict, workspace, outputFileName) -> None:
    if not file_groups:
        return []

    result = []
    for prefix, files in file_groups.items():
        files = sorted(files, key=get_filed_sub)
        p_file, s_file = files[0], files[1]

        # Read and aggregate data
        df_1 = parse_file(p_file, "P", workspace)
        df_2 = parse_file(s_file, "S", workspace)
        df = pd.concat([df_1, df_2], ignore_index=True)
        result.append(df)

    # Write data to the file
    final_data = pd.concat(result, ignore_index=True)
    final_data.to_csv(outputFileName,sep=',')


def isPfile(filename):
    # If line 49 starts with "Summary Table" then it is a P file
    try:
        with open(filename, "r", encoding='utf-8',
                errors='ignore') as f:
            
            lines = f.readlines()
            columns = lines[48].strip().split()
            return columns[0] == "Summary" and columns[1] == "Table"
    except Exception:
            return False


def isSfile(filename):
    # If line 65 starts with "Summary Table" then it is an S file
    try:
        with open(filename, "r", encoding='utf-8',
                errors='ignore') as f:
            
            lines = f.readlines()
            columns = lines[64].strip().split()
            return columns[0] == "Summary" and columns[1] == "Table"
    except Exception:
        return False

def validateFiles(inputFile1, inputFile2):
    try:
        f1 = Path(inputFile1)
        if f1.exists() == False:
            print("First file in list does not exist.")
            return False
        f2 = Path(inputFile2)
        if f2.exists() == False:
            print("Second file in list does not exist.")
            return False
            
        if isSfile(inputFile1) == False and isPfile(inputFile1) == False:
            print("First file has an invalid format")
            return False
        if isSfile(inputFile2) == False and isPfile(inputFile2) == False:
            print("Second file has an invalid format")
            return False
    except Exception as e:
        print(e)
        return False
    
    return True

def main():

    #Parse the coomand line argument
    #print(sys.argv)
    """
    parser = optparse.OptionParser()
    parser.add_option('-d', dest = 'directory',
                      type = 'str', 
                      help = 'path to the directory you want to work with')
    parser.add_option('-f', dest = 'filename',
                      type = 'str', 
                      help = 'desired name of your outputfile, be sure to include .csv at the end')

    options, args = parser.parse_args() 
    if (options.directory == None):
        print("You must provide a work directory.")
        print(parser.usage)
        exit(0)

    if (options.filename == None):
        print("You must provide a output filename.")
        print(parser.usage)
        exit(0)
        
    workspace = options.directory
    outputFileName = options.filename

     # Collect data from files
    file_groups = organize_files(path=workspace)
    transform(file_groups=file_groups, workspace=workspace, outputFileName=outputFileName)
    """
 
    if len(sys.argv) < 3:
        print("Usage: inputfile1,inputFile2 outputFile")
        print(len(sys.argv))
        print(sys.argv)
        exit()


    #print(sys.argv)
    inputFiles = sys.argv[1].split(',')
    outputFile = sys.argv[2]

    transform_files(inputFiles, '.', outputFile)

if __name__ == "__main__":
    main()

   
    
