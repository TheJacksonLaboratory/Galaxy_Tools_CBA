import configparser
import os
import sys
import json
from runQuery import QueryHandler
import pandas as pd


# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(BASE_DIR)

public_config = configparser.ConfigParser()
public_config.read("/projects/galaxy/tools/cba/config/setup.cfg")
SERVICE_USERNAME = public_config["CORE LIMS"]["service username"]

private_config = configparser.ConfigParser()
private_config.read("/projects/galaxy/tools/cba/config/secret.cfg")
SERVICE_PASSWORD = private_config["CORE LIMS"]["service password"]

query = QueryHandler(SERVICE_USERNAME, SERVICE_PASSWORD,"KOMP") # The third param is the service abreviation: CBA, KOMP,...

KOMP_LINE_LIST = query.runLineQuery(query.queryBase + "MOUSE_SAMPLE?$select =Barcode&$expand=MOUSESAMPLE_STRAIN($select=Barcode)&$count=true")
KOMP_REQUEST_LIST = query.runQuery(query.queryBase + "KOMP_REQUEST?$count=true")["Barcode"].tolist()
KOMP_BATCH_LIST = query.runQuery(query.queryBase + "KOMP_BATCH?$count=true")["Barcode"].tolist()

# This gets all the frigging experiments!
KOMP_EXPERIMENTS = query.get_experiments() # Filter for "KOMP_"

# KOMP BWT LOVs
df = pd.read_csv("/projects/galaxy/tools/cba/data/KOMP_BWT_raw_data.csv")
series_ls = df["Strain"]
series_ls = series_ls.astype(str)
item_ls = series_ls.to_list()
KOMP_BWT_LINES = list(set(item_ls))   
KOMP_BWT_LINES.sort()

series_ls = df["Sample"]
series_ls = series_ls.astype(str)
item_ls = series_ls.to_list()
KOMP_BWT_SAMPLES = list(set(item_ls))   
KOMP_BWT_SAMPLES.sort()

series_ls = df["ExperimentName"]
series_ls = series_ls.astype(str)
item_ls = series_ls.to_list()
KOMP_BWT_EXPERIMENTS = list(set(item_ls))   
KOMP_BWT_EXPERIMENTS.sort()


series_ls = df["Experiment_Barcode"]
series_ls = series_ls.astype(str)
item_ls = series_ls.to_list()
KOMP_BWT_EXPERIMENT_BARCODES = list(set(item_ls))   
KOMP_BWT_EXPERIMENT_BARCODES.sort()

series_ls = df["Customer_Mouse_ID"]
series_ls = series_ls.astype(str)
item_ls = series_ls.to_list()
KOMP_BWT_CUSTOMER_SAMPLE_NAME = list(set(item_ls))   
KOMP_BWT_CUSTOMER_SAMPLE_NAME.sort()

KOMP_ALL_EXPERIMENTS = [
	"KOMP_BODY_WEIGHT_EXPERIMENT",
	"KOMP_AUDITORY_BRAINSTEM_RESPONSE_EXPERIMENT",
	"KOMP_BODY_COMPOSITION_EXPERIMENT",
	"KOMP_CLINICAL_BLOOD_CHEMISTRY_EXPERIMENT",
	"KOMP_ELECTROCARDIOGRAM_EXPERIMENT",
	"KOMP_ELECTRORETINOGRAPHY_EXPERIMENT",
	"KOMP_EYE_MORPHOLOGY_EXPERIMENT",
	"KOMP_FUNDUS_IMAGING_EXPERIMENT",
	"KOMP_GLUCOSE_TOLERANCE_TEST_EXPERIMENT",
	"KOMP_GRIP_STRENGTH_EXPERIMENT",
	"KOMP_HEART_WEIGHT_EXPERIMENT",
	"KOMP_HEMATOLOGY_EXPERIMENT",
	"KOMP_HOLEBOARD_EXPERIMENT",
	"KOMP_LIGHT_DARK_BOX_EXPERIMENT",
	"KOMP_OPEN_FIELD_EXPERIMENT",
	"KOMP_SHIRPA_DYSMORPHOLOGY_EXPERIMENT",
	"KOMP_STARTLE_PPI_EXPERIMENT"]

odata = {}

odata["KOMP_LINE_LIST"] = KOMP_LINE_LIST
odata["KOMP_REQUEST_LIST"] = KOMP_REQUEST_LIST
odata["KOMP_BATCH_LIST"] = KOMP_BATCH_LIST
odata["KOMP_EXPERIMENTS"] = KOMP_EXPERIMENTS
odata["KOMP_BWT_LINES"] = KOMP_BWT_LINES
odata["KOMP_BWT_SAMPLES"] = KOMP_BWT_SAMPLES
odata["KOMP_BWT_CUSTOMER_SAMPLE_NAME"] = KOMP_BWT_CUSTOMER_SAMPLE_NAME
odata["KOMP_BWT_EXPERIMENTS"] = KOMP_BWT_EXPERIMENTS
odata["KOMP_BWT_EXPERIMENT_BARCODES"] = KOMP_BWT_EXPERIMENT_BARCODES
odata["KOMP_ALL_EXPERIMENTS"] = KOMP_ALL_EXPERIMENTS

with open("komp_lists.txt", "w") as outfile:
    json.dump(odata, outfile)
