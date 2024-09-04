import configparser
import os
import sys
import json
from runQuery import QueryHandler


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(BASE_DIR)

public_config = configparser.ConfigParser()
public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

private_config = configparser.ConfigParser()
private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

query = QueryHandler(SERVICE_USERNAME, SERVICE_PASSWORD,'KOMP') # The third param is the service abreviation: CBA, KOMP,...

KOMP_LINE_LIST = query.runLineQuery(query.queryBase + "MOUSE_SAMPLE?$select =Barcode&$expand=MOUSESAMPLE_STRAIN($select=Barcode)&$count=true")
KOMP_REQUEST_LIST = query.runQuery(query.queryBase + "KOMP_REQUEST?$count=true")["Barcode"].tolist()
KOMP_BATCH_LIST = query.runQuery(query.queryBase + "KOMP_BATCH?$count=true")["Barcode"].tolist()
# This gets all the frigging experiments!
KOMP_EXPERIMENTS = query.get_experiments() # Filter for "KOMP_"
odata = {}

odata['KOMP_LINE_LIST'] = KOMP_LINE_LIST
odata['KOMP_REQUEST_LIST'] = KOMP_REQUEST_LIST
odata['KOMP_BATCH_LIST'] = KOMP_BATCH_LIST
odata['KOMP_EXPERIMENTS'] = KOMP_EXPERIMENTS

with open('komp_lists.txt', 'w') as outfile:
    json.dump(odata, outfile)
