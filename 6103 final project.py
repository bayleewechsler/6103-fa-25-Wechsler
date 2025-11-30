#6103 final project 
#issue 1- data acquisition (done)
#issue 2- data wrangling (processing/ cleaning) (nearly done)
#issue 3- modeling (eda, stats, regression, one other maybe)
#issue 4- analyzing the results
#issue 5- writing up the report 

#in r console 
#library(reticulate)
#py_require("pandas")
#py_install("openpyxl")
#reticulate::py_install("uszipcode", envname = "r-reticulate")

#import packages
import numpy as np
import pandas as pd
import os
import zipfile
import openpyxl
import gzip
from lxml import etree
import random

#set wd
data_dir = "/Users/bayleewechsler/6103 Final Project"

#load csv function
def read_csv_safe(file_like):
    try:
        return pd.read_csv(file_like, encoding='utf-8')
    except (UnicodeDecodeError, pd.errors.EmptyDataError):
        pass
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
def read_xlsx_safe(file_path):
    try:
        df = pd.read_excel(file_path, engine="openpyxl")
        return df
    except:
        return None

#load xlsxs
#replace treatment dataset (which sucked) with SAMHSA 2022-2024 mental health and substance abuse treatment facility data 
mh_2024 = read_xlsx_safe(os.path.join(data_dir, "National Directory MH 2024_Final.xlsx"))
sa_2024 = read_xlsx_safe(os.path.join(data_dir, "National Directory SU 2024_Final.xlsx"))
mh_2022 = read_xlsx_safe(os.path.join(data_dir, "National_Directory_MH_Facilities_2022.xlsx"))
sa_2022 = read_xlsx_safe(os.path.join(data_dir, "National_Directory_SA_Facilities_2022.xlsx"))
mh_2023 = read_xlsx_safe(os.path.join(data_dir, "national-directory-mh-facilities-2023.xlsx"))
sa_2023 = read_xlsx_safe(os.path.join(data_dir, "national-directory-su-facilities-2023.xlsx"))
county_treatment_courts_2024 = read_xlsx_safe(os.path.join(data_dir, "2024_County_Court_Count.xlsx"))

#zip data is more tricky to load
#load zip function
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
    return dfs
def get_text(elem):
    return elem.text if elem is not None else ""
def get_attrib(elem, attr, default=""):
    return elem.attrib.get(attr, default)

icpsr_zip = os.path.join(data_dir, "ICPSR_38048-V1.zip")

#load icpsr data
icpsr_df = load_zip_tsv(icpsr_zip)
icpsr_data = icpsr_df['ICPSR_38048/DS0001/38048-0001-Data.tsv']
icpsr_data['Year'] = 2019
ddi_file = "usa_00002_codebook.xml"
tree = etree.parse(ddi_file)
root = tree.getroot()
ns = {"ddi": "ddi:codebook:2_5"}
variables = {}
categories = {}
colspecs = []
colnames = []
current_pos = 0

#process icpsr data
for var in root.findall(".//ddi:var", ns):
    var_id = var.attrib["ID"]
    safe_name = var_id
    suffix = 1
    while safe_name in colnames:
        safe_name = f"{var_id}_{suffix}"
        suffix += 1
    colnames.append(safe_name)
    width_elem = var.find("ddi:varFormat/ddi:width", ns)
    width = int(get_text(width_elem) or 1)
    colspecs.append((current_pos, current_pos + width))
    current_pos += width
    var_name = get_attrib(var, "name", var_id)
    var_type = get_attrib(var.find("ddi:varFormat", ns), "type", "unknown")
    label = get_text(var.find("ddi:labl", ns))
    variables[safe_name] = {"name": var_name, "type": var_type, "label": label}
    cats = {}
    for cat in var.findall("ddi:catgry", ns):
        val = get_text(cat.find("ddi:catValu", ns))
        lab = get_text(cat.find("ddi:labl", ns))
        if val and lab:
            cats[val] = lab
    if cats:
        categories[safe_name] = cats

#load acs data 
data_file = "usa_00002.dat.gz"
sample_size = 200_000
reservoir = []
with gzip.open(data_file, 'rt') as f:
    for i, line in enumerate(f):
        if i < sample_size:
            reservoir.append(line)
        else:
            j = random.randint(0, i)
            if j < sample_size:
                reservoir[j] = line

with open("acs_sampled_temp.dat", "w") as tmp:
    tmp.writelines(reservoir)
acs_sampled = pd.read_fwf(
    "acs_sampled_temp.dat",
    colspecs=colspecs,
    names=colnames,
    dtype=str)
for var_id, cat_map in categories.items():
    if var_id in acs_sampled.columns:
        acs_sampled[var_id] = acs_sampled[var_id].map(cat_map).fillna(acs_sampled[var_id])
for var_id, meta in variables.items():
    if var_id in acs_sampled.columns and meta["type"].lower() == "numeric":
        acs_sampled[var_id] = pd.to_numeric(acs_sampled[var_id], errors="coerce")
#process acs data
acs_sampled["YEAR"] = 2022
acs_sampled["YEAR"] = pd.to_numeric(acs_sampled["YEAR"], errors="coerce").astype("Int64")
acs_sampled.to_csv("acs_processed.csv", index=False)

#process year_end_prison_long data
value_columns = [c for c in year_end_prison.columns if "total_prison_pop_" in c]
year_end_prison_long = year_end_prison.melt(
    id_vars=["region", "state_name"],
    value_vars=value_columns,
    var_name="variable",
    value_name="total_prison_pop")
year_end_prison_long["Year"] = year_end_prison_long["variable"].str.extract(r"(\d{4})").astype(int)
year_end_prison_long = year_end_prison_long.drop(columns=["variable"])
year_end_prison_long['region'] = year_end_prison_long['region'].fillna('NA')
year_end_prison_long['state_name'] = year_end_prison_long['state_name'].fillna('NA').str.title()
us_state_abbrev = {
    'Alabama': 'AL', 'Alaska': 'AK', 'Arizona': 'AZ', 'Arkansas': 'AR',
    'California': 'CA', 'Colorado': 'CO', 'Connecticut': 'CT', 'Delaware': 'DE',
    'Florida': 'FL', 'Georgia': 'GA', 'Hawaii': 'HI', 'Idaho': 'ID',
    'Illinois': 'IL', 'Indiana': 'IN', 'Iowa': 'IA', 'Kansas': 'KS',
    'Kentucky': 'KY', 'Louisiana': 'LA', 'Maine': 'ME', 'Maryland': 'MD',
    'Massachusetts': 'MA', 'Michigan': 'MI', 'Minnesota': 'MN', 'Mississippi': 'MS',
    'Missouri': 'MO', 'Montana': 'MT', 'Nebraska': 'NE', 'Nevada': 'NV',
    'New Hampshire': 'NH', 'New Jersey': 'NJ', 'New Mexico': 'NM',
    'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND',
    'Ohio': 'OH', 'Oklahoma': 'OK', 'Oregon': 'OR', 'Pennsylvania': 'PA',
    'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD',
    'Tennessee': 'TN', 'Texas': 'TX', 'Utah': 'UT', 'Vermont': 'VT',
    'Virginia': 'VA', 'Washington': 'WA', 'West Virginia': 'WV',
    'Wisconsin': 'WI', 'Wyoming': 'WY'}
year_end_prison_long["state_abbr"] = (year_end_prison_long["state_name"].map(us_state_abbrev))
year_end_prison_long["Year"] = pd.to_numeric(year_end_prison_long["Year"], errors="coerce").fillna(-1).astype(int)

#process bjs data 
bjs_column_titles = bjs_jail.iloc[11]
bjs_jail = bjs_jail[12:]
bjs_jail.columns = bjs_column_titles
bjs_jail = bjs_jail.iloc[0:11]
bjs_jail = bjs_jail.loc[:, ~bjs_jail.columns.isna()]
bjs_jail["Year"] = pd.to_numeric(bjs_jail["Year"], errors="coerce").fillna(-1).astype(int)

#process treatment courts datasets
county_treatment_courts["Year"] = 2023
county_treatment_courts = county_treatment_courts.fillna(0)
county_treatment_courts_2024["Year"] = 2024
county_treatment_courts_2024 = county_treatment_courts_2024.fillna(0)

#map county fips to datasets
county_fips_xwalk = pd.read_csv(
    "https://raw.githubusercontent.com/kjhealy/fips-codes/master/county_fips_master.csv",
    dtype=str,
    encoding="latin1")

county_fips_xwalk = county_fips_xwalk[['state','county_name','fips']]
county_fips_xwalk['state'] = county_fips_xwalk['state'].str.lower().str.strip()
county_fips_xwalk['county_name'] = (
    county_fips_xwalk['county_name']
    .str.lower()
    .str.replace(" county", "", regex=False)
    .str.strip())
county_fips_xwalk['fips'] = county_fips_xwalk['fips'].astype(str).str.zfill(5)

#xwalk function
def fix_courts_df(df, year):
    df = df.copy()
    df["State"] = df["State"].astype(str).str.lower().str.strip()
    df["County"] = (
        df["County"].astype(str)
        .str.lower()
        .str.replace(" county", "", regex=False)
        .str.strip())
    df["Year"] = int(year)
    out = (
        df.merge(
            county_fips_xwalk,
            left_on=["State", "County"],
            right_on=["state", "county_name"],
            how="left")
        .drop(columns=["state", "county_name"]))
    out["fips"] = out["fips"].astype(str).str.zfill(5)
    return out

county_treatment_courts = fix_courts_df(county_treatment_courts, 2023)
county_treatment_courts_2024 = fix_courts_df(county_treatment_courts_2024, 2024)

#process incarceration data 
incarceration_county = incarceration_county.rename(columns={'year': 'Year'})
incarceration_state["Year"] = pd.to_numeric(incarceration_state["Year"], errors="coerce").fillna(-1).astype(int)
incarceration_county = incarceration_county.fillna(incarceration_county.median(numeric_only=True))
incarceration_county['fips'] = incarceration_county['fips'].astype(str).str.zfill(5)
incarceration_county = incarceration_county.fillna(incarceration_county.median(numeric_only=True))
incarceration_state = incarceration_state.rename(columns={'year':'Year'})
incarceration_county["Year"] = pd.to_numeric(incarceration_county["Year"], errors="coerce").fillna(-1).astype(int)
incarceration_state = incarceration_state.fillna("mul")

#process samhsa data 
zip_fips = pd.read_csv(
    "https://www2.census.gov/geo/docs/maps-data/data/rel/zcta_county_rel_10.txt",
    sep=",")
zip_fips = zip_fips[['ZCTA5','COUNTY']]
zip_fips.rename(columns={'ZCTA5':'zip','COUNTY':'county_fips'}, inplace=True)
zip_fips['zip'] = zip_fips['zip'].astype(str).str.zfill(5)
zip_fips['county_fips'] = zip_fips['county_fips'].astype(str).str.zfill(5)

#samhsa cleaning function
def clean_samhsa_county(df, year):
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")
    df["zip"] = df["zip"].astype(str).str.extract(r"(\d{5})", expand=False)
    df["Year"] = year
    df = df.merge(zip_fips, on="zip", how="left")
    df_county = df.groupby(["county_fips","Year"]).size().reset_index(name="treatment_facility_count")
    return df_county

mh_2022_clean = clean_samhsa_county(mh_2022, 2022)
mh_2023_clean = clean_samhsa_county(mh_2023, 2023)
mh_2024_clean = clean_samhsa_county(mh_2024, 2024)
sa_2022_clean = clean_samhsa_county(sa_2022, 2022)
sa_2023_clean = clean_samhsa_county(sa_2023, 2023)
sa_2024_clean = clean_samhsa_county(sa_2024, 2024)

#samhsa data processing
for df, t in [(mh_2022_clean,'MH'),(mh_2023_clean,'MH'),(mh_2024_clean,'MH'),
              (sa_2022_clean,'SA'),(sa_2023_clean,'SA'),(sa_2024_clean,'SA')]:
    df['type'] = t
all_samhsa = pd.concat([mh_2022_clean, mh_2023_clean, mh_2024_clean,
                        sa_2022_clean, sa_2023_clean, sa_2024_clean], ignore_index=True)
samhsa_wide = all_samhsa.pivot_table(
    index=['county_fips','Year'],
    columns='type',
    values='treatment_facility_count',
    fill_value=0).reset_index()
samhsa_wide.rename(columns={'county_fips':'fips'}, inplace=True)

#county-level data merge
incarceration_county['fips'] = incarceration_county['fips'].astype(str).str.zfill(5)
samhsa_wide['fips'] = samhsa_wide['fips'].astype(str).str.zfill(5)
county_treatment_courts['fips'] = county_treatment_courts['fips'].astype(str).str.zfill(5)
county_treatment_courts_2024['fips'] = county_treatment_courts_2024['fips'].astype(str).str.zfill(5)
county_df = (
    incarceration_county
    .merge(samhsa_wide, on=['Year','fips'], how='left')
    .merge(county_treatment_courts, on=['Year','fips'], how='left')
    .merge(county_treatment_courts_2024, on=['Year','fips'], how='left'))
for col in ['MH','SA']:
    if col in county_df.columns:
        county_df[col] = county_df[col].fillna(0)
county_df["Year"] = pd.to_numeric(county_df["Year"], errors="coerce").fillna(-1).astype(int)

#state-level data merge
state_df = year_end_prison_long.merge(
    incarceration_state,
    on=["state_abbr", "Year"],
    how="outer")

#person-level data merge 
person_df = icpsr_data.merge(
    acs_sampled,
    left_on=['STATE','Year'],
    right_on=['STATEFIP','YEAR'],
    how='left')

person_df = person_df.loc[:, ~person_df.columns.duplicated()]
person_df["Year"] = pd.to_numeric(person_df["Year"], errors="coerce").fillna(-1).astype(int)

#bjs data is federal, include in all as a comparison?

#make sure merged dfs are good (NAs and var types)
state_df['state_name_x'].unique().tolist()
state_df.isna().sum()
state_df.info()
county_df.isna().sum()
county_df.info()
person_df.isna().sum()
person_df.info()


##circle back to research question (alternatives to incarceration 
#as it impacts recidivism and post-release economic outcomes)
#EDA
##exploratory stats
##some graphs (include titles, subtitles, labels, etc)

#regression 


#analyze results and start shaping out the presentation



