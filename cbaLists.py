import configparser
import os
import sys
import json
from runQuery import QueryHandler

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(BASE_DIR)

public_config = configparser.ConfigParser()
public_config.read("./config/setup.cfg")
SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

private_config = configparser.ConfigParser()
private_config.read("./config/secret.cfg")
SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

query = QueryHandler(SERVICE_USERNAME, SERVICE_PASSWORD)

CBA_LINE_LIST = query.runLineQuery(query.queryBase + "MOUSE_SAMPLE?$select =Barcode&$expand=MOUSESAMPLE_STRAIN($select=Barcode)&$count=true")  # TBD
CBA_REQUEST_LIST = query.runQuery(query.queryBase + "CBA_REQUEST?$count=true")["Barcode"].tolist()
CBA_BATCH_LIST = query.runQuery(query.queryBase + "CBA_BATCH?$count=true")["Barcode"].tolist()
CBA_EXPERIMENTS = query.get_experiments()
CBA_EXPERIMENTS.remove('CBA_CAGE_SPLITTING_EXPERIMENT') # cage experiment is not a typical experiment

odata = {}

odata['CBA_LINE_LIST'] = CBA_LINE_LIST
odata['CBA_REQUEST_LIST'] = CBA_REQUEST_LIST
odata['CBA_BATCH_LIST'] = CBA_BATCH_LIST
odata['CBA_EXPERIMENTS'] = CBA_EXPERIMENTS

with open('cba_lists.txt', 'w') as outfile:
    json.dump(odata, outfile)

