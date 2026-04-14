import json

def load_file():
    with open('/projects/galaxy/tools/cba/jaxlims_lists.txt', 'r') as f:
        j = json.load(f)
    return j

def get_jaxlims_line_fields():
    j = load_file()
    line_values = [(value, value, 0) for value in j['JAXLIMS_LINE_LIST']]
    line_values.insert(0,("","",0))
    return line_values

def get_jaxlims_sample_fields():
    j = load_file()
    request_values = [(value, value, 0) for value in j['JAXLIMS_SAMPLES']] 
    request_values.insert(0,("",'',0))
    return request_values

def get_all_jaxlims_experiments():	
    j = load_file()
    request_values = [(value, value, 0) for value in j['JAXLIMS_ALL_EXPERIMENTS']] 
    request_values.insert(0,("",'',0))
    return request_values


def get_jaxlims_exp_statuses():
    j = load_file()
    request_values = [(value, value, 0) for value in j['JAXLIMS_EXP_STATUS']] 
    request_values.insert(0,("",'',0))
    return request_values

def get_jaxlims_study_fields():
    j = load_file()
    request_values = [(value, value, 0) for value in j['JAXLIMS_STUDY_LIST']]
    request_values.insert(0,("",'',0))
    return request_values

if __name__ == '__main__':
    print(get_jaxlims_sample_fields())
