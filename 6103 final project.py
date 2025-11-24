#scratch code
#6103 final project  

#in r console 
#library(reticulate)
#py_require("pandas")
#py_install("openpyxl")

#import packages
import numpy as np
import pandas as pd
import os 
import zipfile
import openpyxl

#set wd
data_dir = "/Users/bayleewechsler/6103 Final Project"

#load data function
def read_csv_safe(file_like):
    try:
        return pd.read_csv(file_like, encoding='utf-8')
    except (UnicodeDecodeError, pd.errors.EmptyDataError):
        pass
    # second attempt
    try:
        return pd.read_csv(file_like, encoding='cp1252')
    except (UnicodeDecodeError, pd.errors.EmptyDataError):
        return None

      
year_end_prison = read_csv_safe(os.path.join(data_dir, "year-end-prison-2021.csv"))
bjs_jail = read_csv_safe(os.path.join(data_dir, "BJS jail population overview.csv"))
county_treatment_courts = read_csv_safe(os.path.join(data_dir, "County Treatment Court Count.csv"))
incarceration_county = read_csv_safe(os.path.join(data_dir, "incarceration_trends_county.csv"))
incarceration_state = read_csv_safe(os.path.join(data_dir, "incarceration_trends_state.csv"))
treatment_facilities = pd.read_excel(os.path.join(data_dir, "FindTreament_Facility_listing_2025_11_05_245152.xlsx"), engine='openpyxl')

#zip data is more tricky to load
icpsr_zip = os.path.join(data_dir, "ICPSR_38048-V1.zip")  

def load_zip_csv(zip_path):
    dfs = {}
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if not file_name.lower().endswith('.csv'):
                continue
            if file_name.startswith('__MACOSX') or file_name.startswith('._'):
                continue
            with zip_ref.open(file_name) as f:
                df = read_csv_safe(f)
                if df is not None:
                    dfs[file_name] = df
                    print(f"{file_name} loaded, shape: {df.shape}")
                else:
                    print(f"{file_name} is empty, skipped.")
    return dfs

def load_zip_tsv(zip_path):
    dfs = {}
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file_name in zip_ref.namelist():
            if not file_name.lower().endswith('.tsv'):
                continue
            if file_name.startswith('__MACOSX') or file_name.startswith('._'):
                continue

            with zip_ref.open(file_name) as f:
                try:
                    df = pd.read_csv(f, sep='\t', encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(f, sep='\t', encoding='cp1252')
                dfs[file_name] = df
                print(f"{file_name} loaded, shape: {df.shape}")
    return dfs

#now load zip data
icpsr_df = load_zip_tsv(icpsr_zip)

#list the keys (CSV files) in the dictionary
list(icpsr_df.keys())

with zipfile.ZipFile(icpsr_zip, 'r') as z:
    print(z.namelist())

#load ACS microdata)
usa_dat = pd.read_csv(os.path.join(data_dir, "usa_00002.dat.gz"), compression='gzip')
