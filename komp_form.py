import json

def load_file():
    with open('/projects/galaxy/tools/cba/komp_lists.txt', 'r') as f:
        j = json.load(f)
    return j

def get_experiment_fields():
    j = load_file()
    experiment_values = [(value, value, 0) for value in j['KOMP_EXPERIMENTS']]
    experiment_values.insert(0,("","",0)) 
    #Create 'empty' value at beginning of list 
    return experiment_values

def get_batch_fields():
    j = load_file()
    batch_values = [(value, value, 0) for value in j['KOMP_BATCH_LIST']]
    batch_values.insert(0,("","",0))
    return batch_values

def get_line_fields():
    j = load_file()
    line_values = [(value, value, 0) for value in j['KOMP_LINE_LIST']]
    line_values.insert(0,("","",0))
    return line_values

def get_request_fields():
    j = load_file()
    request_values = [(value, value, 0) for value in j['KOMP_REQUEST_LIST']] 
    request_values.insert(0,("",'',0))
    #print (str(request_values))
    return request_values

if __name__ == '__main__':
    print(get_request_fields())
