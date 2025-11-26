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

#load csv function
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

#load xlsx function
# define safe Excel loader
def read_xlsx_safe(file_path, header=None):
    try:
        df = pd.read_excel(file_path, engine="openpyxl", header=header)
        print(f"Loaded {os.path.basename(file_path)} → shape {df.shape}")
        return df
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
    return None

#load xlsxs
#replace treatment dataset (which sucked) with SAMHSA 2022-2024 mental health and substance abuse treatment facility data 
mh_2024 = read_xlsx_safe(os.path.join(data_dir, "National Directory MH 2024_Final.xlsx"), header=None)
sa_2024 = read_xlsx_safe(os.path.join(data_dir, "National Directory SU 2024_Final.xlsx"), header=None)
mh_2022 = read_xlsx_safe(os.path.join(data_dir, "National_Directory_MH_Facilities_2022.xlsx"), header=None)
sa_2022 = read_xlsx_safe(os.path.join(data_dir, "National_Directory_SA_Facilities_2022.xlsx"), header=None)
mh_2023 = read_xlsx_safe(os.path.join(data_dir, "national-directory-mh-facilities-2023.xlsx"), header=None)
sa_2023 = read_xlsx_safe(os.path.join(data_dir, "national-directory-su-facilities-2023.xlsx"), header=None)
county_treatment_courts_2024 = read_xlsx_safe(os.path.join(data_dir, "2024_County_Court_Count.xlsx"))

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
    
icpsr_data= icpsr_df['ICPSR_38048/DS0001/38048-0001-Data.tsv']

#usa_dat = pd.read_csv(os.path.join(data_dir, "usa_00002.dat.gz"), compression='gzip')
#ACS file too big, need a smaller sample
input_file = os.path.join(data_dir, "usa_00002.dat.gz")
output_file = "usa_00002_sample.csv.gz"

sampled_chunks = []

#load ACS microdata
for chunk in pd.read_csv(input_file, 
                         compression='gzip', 
                         chunksize=500000,
                         low_memory=False):
    sampled_chunks.append(chunk.sample(frac=.05))

#combine all sampled ACS chunks
sampled_acs_df = pd.concat(sampled_chunks, ignore_index=True)


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
county_treatment_courts= county_treatment_courts.fillna(0)
#"2024_County_Court_Count" is the same data but from 2024, not 2023
#2024_county_treatment_courts["Year"]= 2024
#2024_county_treatment_courts= county_treatment_courts.fillna(0)

#incarceration_county merge-ready
incarceration_county= incarceration_county.rename(columns={
  "year": "Year"})

incarceration_county= incarceration_county.fillna("mul")
#filled nas (just demographic pops) with means

#incarceration_state merge-ready
incarceration_state= incarceration_state.rename(columns={
  "year": "Year"})
incarceration_state= incarceration_state.fillna("mul")
#filled nas with means

#treatment_facilities merge-ready
treatment_facilities.head() #to find header row
treatment_facilities = pd.read_excel(
    os.path.join(data_dir, "FindTreament_Facility_listing_2025_11_05_245152.xlsx"),
    header=4,   # whatever row the real header is
    engine='openpyxl')










### something is wrong here- only one row ###












#icpsr_data merge-ready
icpsr_data.head()
#will need a datadictionary for these variables/ values
#not sure how to clean without understanding the data better
#need more info on in order to make a 'Year' variable/ column
#checked, and there are no NANs
#i would like to randomize the inmate ID number, so I need to figure out 
#how to do that with a set seed situation












### double check merge vs concat###
#separate into state, county, and individual-level dfs
#state: year_end_prison_long, bjs_jail, incarceration_state
state_df = (
    year_end_prison_long
    .merge(bjs_jail, on="Year", how="outer")
    .merge(incarceration_state, on="Year", how="outer"))

#county: county_treatment_courts, incarceration_county
county_df = (
    county_treatment_courts
    .merge(incarceration_county, on=["Year", "fips"], how="outer"))

#individual: icpsr_data, sampled_acs_df
person_df = icpsr_data.merge(sampled_acs_df, on=["some_id"], how="left")





#merge all dataframes together (except acs) 
merge1= pd.merge(year_end_prison_long,bjs_jail, on="Year", how= "outer")
merge2= pd.merge(merge1,county_treatment_courts, on="Year", how= "outer")
merge3= pd.merge(merge2,incarceration_county, on="Year", how= "outer")
merge4= pd.merge(merge3,incarceration_state, on="Year", how= "outer")
merge5= pd.merge(merge4,treatment_facilities, on="Year", how= "outer")
merge6= pd.merge(merge5,2024_county_treatment_courts, on="Year", how= "outer")
almost_all_df= pd.merge(merge5,icpsr_data, on="Year", how= "outer")
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








#go back and remove everything I added that isn't a necessary function/package 
#and/or dataframe
del merge1, merge2, merge3, merge4, merge5, merge6











##circle back to research question (alternatives to incarceration 
#as it impacts recidivism and post-release economic outcomes)
#EDA
##exploratory stats
##some graphs (include titles, subtitles, labels, etc)

#regression 


#analyze results and start shaping out the presentation



