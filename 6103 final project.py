#wtf is happening w the acs sampled dataframe 


#6103 final project 
#issue 1- data acquisition (nearly done)
#issue 2- data wrangling (processing/ cleaning)
#issue 3- modeling (eda, stats, regression, one other maybe)
#issue 4- analyzing the results
#issue 5- writing up the report 

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
import gzip

#set wd
data_dir = "/Users/bayleewechsler/6103 Final Project"

#load data function
def read_csv_safe(file_like):
    try:
        return pd.read_csv(file_like, encoding='utf-8')
    except (UnicodeDecodeError, pd.errors.EmptyDataError):
        pass
    #second try
    try:
        return pd.read_csv(file_like, encoding='cp1252')
    except (UnicodeDecodeError, pd.errors.EmptyDataError):
        return None

#load csvs      
year_end_prison = read_csv_safe(os.path.join(data_dir, "year-end-prison-2021.csv"))
bjs_jail = read_csv_safe(os.path.join(data_dir, "BJS jail population overview.csv"))
county_treatment_courts = read_csv_safe(os.path.join(data_dir, "County Treatment Court Count.csv"))
incarceration_county = read_csv_safe(os.path.join(data_dir, "incarceration_trends_county.csv"))
incarceration_state = read_csv_safe(os.path.join(data_dir, "incarceration_trends_state.csv"))
treatment_facilities = pd.read_excel(os.path.join(data_dir, "FindTreament_Facility_listing_2025_11_05_245152.xlsx"), engine='openpyxl')

#zip data is more tricky to load
icpsr_zip = os.path.join(data_dir, "ICPSR_38048-V1.zip")  

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


#usa_dat = pd.read_csv(os.path.join(data_dir, "usa_00002.dat.gz"), compression='gzip')
#ACS file too big, need a smaller sample
input_file = "usa_00002.dat.gz"     
output_file = "usa_00002_sample.csv.gz"
sample_frac = 0.05                 
chunksize = 500000                

sampled_chunks = []

#load ACS microdata
for chunk in pd.read_csv(input_file, 
                         compression='gzip', 
                         chunksize=chunksize):
    sampled = chunk.sample(frac=sample_frac)
    sampled_chunks.append(sampled)

#combine all sampled ACS chunks
sampled_acs_df = pd.concat(sampled_chunks)
###fix acs dataframe, looks weird ###
#go back and remove everything I added that isn't a necessary function/package 
#and/or dataframe

#do I have common columns for merges? no
#bjs_jail merge-ready.
bjs_column_titles= bjs_jail.iloc[11]
bjs_jail= bjs_jail[12:]
bjs_jail.columns= bjs_column_titles
bjs_jail= bjs_jail.iloc[0:11]
bjs_jail = bjs_jail.loc[:, ~bjs_jail.columns.isna()]
#bjs_jail 'Year' column is 2013-2023

#year_end_prison merge-ready.
#need to add year column to year end prison to merge
value_columns = [c for c in year_end_prison.columns 
                 if "total_prison_pop_" in c]

year_end_prison_long = year_end_prison.melt(
    id_vars=["region", "state_name"],
    value_vars=value_columns,
    var_name="variable",
    value_name="total_prison_pop")

year_end_prison_long["Year"]= year_end_prison_long["variable"].str.extract(r"(\d{4})").astype(int)
year_end_prison_long= year_end_prison_long.drop(columns=["variable"])
print(year_end_prison_long.head())
year_end_prison_long['region'] = year_end_prison_long['region'].fillna('NA')
year_end_prison_long['state_name'] = year_end_prison_long['state_name'].fillna('NA')
year_end_prison_long.isna().sum()

#county_treatment_courts merge-ready
county_treatment_courts["Year"]= 2023
#"2024_County_Court_Count" is the same data but from 2024, not 2023

#incarceration_county merge-ready
#incarceration_state merge-ready
#treatment_facilities merge-ready
#icpsr_df merge-ready

#merge all dataframes together (except acs) 
merge1= pd.merge(year_end_prison,bjs_jail, on="shared column name", how= "outer")
merge2= pd.merge(merge1,county_treatment_courts, on="shared column name", how= "outer")
merge3= pd.merge(merge2,incarceration_county, on="shared column name", how= "outer")
merge4= pd.merge(merge3,incarceration_state, on="shared column name", how= "outer")
merge5= pd.merge(merge4,treatment_facilities, on="shared column name", how= "outer")
almost_all_df= pd.merge(merge5,icpsr_df, on="shared column name", how= "outer")
#all thats left is acs

#identify / drop NAs 
almost_all_df.isna().sum()
#almost_all_df.dropna()
#almost_all_df.fillna(0)
almost_all_df.info()


#var type needs to be right, factor, rename columns/ variables for legibility
almost_all_df.rename()

#other data cleaning? 

#check out data- what do we have
almost_all_df.head()


##circle back to research question (alternatives to incarceration 
#as it impacts recidivism and post-release economic outcomes)
#EDA
##exploratory stats
##some graphs (include titles, subtitles, labels, etc)

#regression 


#analyze results and start shaping out the presentation



