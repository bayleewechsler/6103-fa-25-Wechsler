#6103 final project  

#import packages
import numpy as np
import pandas as pd
import os 
import zipfile
import openpyxl

library(reticulate)
py_config()
py_install("openpyxl")


#set wd
data_dir = "/Users/bayleewechsler/6103 Final Project"

#load data

def read_csv_safe(path):
    try:
        return pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='cp1252')
      
year_end_prison = read_csv_safe(os.path.join(data_dir, "year-end-prison-2021.csv"))
bjs_jail = read_csv_safe(os.path.join(data_dir, "BJS jail population overview.csv"))
county_treatment_courts = read_csv_safe(os.path.join(data_dir, "County Treatment Court Count.csv"))
incarceration_county = read_csv_safe(os.path.join(data_dir, "incarceration_trends_county.csv"))
incarceration_state = read_csv_safe(os.path.join(data_dir, "incarceration_trends_state.csv"))
treatment_facilities = pd.read_excel(os.path.join(data_dir, "FindTreament_Facility_listing_2025_11_05_245152.xlsx"), engine='openpyxl')

