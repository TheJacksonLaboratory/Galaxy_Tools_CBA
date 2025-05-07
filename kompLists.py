
import os
import sys
import json
from runQuery import QueryHandler
import pandas as pd
import sqlite3

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(BASE_DIR)

# Populate sropdowns from dt warehouse
connection = sqlite3.connect(DATABASE_DIR + "KOMP-warehouse.db")
df = pd.read_sql_query("SELECT DISTINCT Experiment FROM vKompExperiment", connection)
KOMP_EXPERIMENTS = df.iloc[:, 0].to_list()

df = pd.read_sql_query("SELECT DISTINCT Strain FROM vKompStrain", connection)
KOMP_LINE_LIST = df.iloc[:, 0].to_list()
connection.close()

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

KOMP_EXP_STATUS = [
    "'Cancelled'",
    "'Data Public'",
    "'Data Sent to DCC'",
    "'Pending'",
    "'Pre-upload QC Failed'",
    "'Ready for Data Review'",
    "'Review Completed'",
    "'Review Passed'",
    "'Waiting for Final Review'" ]

odata = {}

odata["KOMP_LINE_LIST"] = KOMP_LINE_LIST
# Unused? odata["KOMP_REQUEST_LIST"] = KOMP_REQUEST_LIST
# Unused? odata["KOMP_BATCH_LIST"] = KOMP_BATCH_LIST
odata["KOMP_EXPERIMENTS"] = KOMP_EXPERIMENTS
odata["KOMP_BWT_LINES"] = KOMP_BWT_LINES
odata["KOMP_BWT_SAMPLES"] = KOMP_BWT_SAMPLES
odata["KOMP_BWT_CUSTOMER_SAMPLE_NAME"] = KOMP_BWT_CUSTOMER_SAMPLE_NAME
odata["KOMP_BWT_EXPERIMENTS"] = KOMP_BWT_EXPERIMENTS
odata["KOMP_BWT_EXPERIMENT_BARCODES"] = KOMP_BWT_EXPERIMENT_BARCODES
odata["KOMP_ALL_EXPERIMENTS"] = KOMP_ALL_EXPERIMENTS
odata["KOMP_ALL_EXPERIMENTS"] = KOMP_ALL_EXPERIMENTS
odata["KOMP_EXP_STATUS"] = KOMP_EXP_STATUS

with open("komp_lists.txt", "w") as outfile:
    json.dump(odata, outfile)
