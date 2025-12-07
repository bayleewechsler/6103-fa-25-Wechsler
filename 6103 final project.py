#6103 final project 

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import statsmodels.api as sm
from tabulate import tabulate
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.metrics import mean_squared_error, r2_score
from linearmodels.panel import PanelOLS
import seaborn as sns
from sklearn.model_selection import train_test_split, KFold
from sklearn.linear_model import LinearRegression
import requests

#set working directory
data_dir = "/Users/bayleewechsler/6103 Final Project"

#build BJS data (pulled painstakingly from BJS PDF tables)
bjs_2022_data = [
    ['AL', 26421, 24053, 2368], ['AK', 4778, 4322, 456], ['AZ', 33865, 30748, 3117],
    ['AR', 17625, 16216, 1409], ['CA', 97608, 93876, 3732], ['CO', 17168, 15766, 1402],
    ['CT', 10506, 9719, 787], ['DE', 4954, 4641, 313], ['FL', 84678, 79154, 5524],
    ['GA', 48439, 45121, 3318], ['HI', 4149, 3711, 438], ['ID', 9110, 7814, 1296],
    ['IL', 29634, 28163, 1471], ['IN', 25286, 22756, 2530], ['IA', 8473, 7755, 718],
    ['KS', 8709, 7974, 735], ['KY', 19744, 17461, 2283], ['LA', 27296, 25860, 1436],
    ['ME', 1675, 1521, 154], ['MD', 15637, 15086, 551], ['MA', 6001, 5777, 224],
    ['MI', 32374, 30708, 1666], ['MN', 8636, 8075, 561], ['MS', 19802, 18208, 1594],
    ['MO', 23911, 21724, 2187], ['MT', 4691, 4026, 665], ['NE', 5649, 5272, 377],
    ['NV', 10304, 9456, 848], ['NH', 2086, 1932, 154], ['NJ', 12657, 12233, 424],
    ['NM', 4970, 4488, 482], ['NY', 31148, 29960, 1188], ['NC', 29627, 27340, 2287],
    ['ND', 1817, 1584, 233], ['OH', 45313, 41655, 3658], ['OK', 22941, 20709, 2232],
    ['OR', 12518, 11606, 912], ['PA', 38860, 36735, 2125], ['RI', 2393, 2388, 117],
    ['SC', 16318, 15233, 1220], ['SD', 3764, 3189, 575], ['TN', 24408, 22017, 2391],
    ['TX', 149264, 137085, 12179], ['UT', 6402, 5929, 473], ['VT', 1334, 1217, 117],
    ['VA', 27442, 25545, 1897], ['WA', 14441, 13526, 915], ['WV', 5800, 5181, 619],
    ['WI', 22418, 20904, 1514], ['WY', 2212, 1956, 256]]

bjs_2023_data = [
    ['AL', 27181, 24682, 2499], ['AK', 4478, 4058, 420], ['AZ', 34473, 31307, 3166],
    ['AR', 18503, 16935, 1568], ['CA', 95962, 92146, 3816], ['CO', 17459, 16030, 1429],
    ['CT', 11099, 10202, 897], ['DE', 4867, 4544, 323], ['FL', 87207, 81326, 5881],
    ['GA', 50425, 46760, 3665], ['HI', 3942, 3533, 409], ['ID', 9829, 8311, 1518],
    ['IL', 29828, 28295, 1533], ['IN', 25088, 22528, 2560], ['IA', 8831, 8112, 719],
    ['KS', 9125, 8333, 792], ['KY', 19175, 16973, 2202], ['LA', 28186, 26618, 1568],
    ['ME', 1873, 1676, 197], ['MD', 16236, 15607, 629], ['MA', 6002, 5830, 172],
    ['MI', 32986, 31285, 1701], ['MN', 8725, 8134, 591], ['MS', 19526, 17998, 1528],
    ['MO', 24223, 22094, 2129], ['MT', 4985, 4238, 747], ['NE', 5931, 5521, 410],
    ['NV', 10463, 9593, 870], ['NH', 2115, 1971, 144], ['NJ', 11675, 11203, 472],
    ['NM', 5586, 5044, 542], ['NY', 32583, 31261, 1322], ['NC', 30685, 28174, 2511],
    ['ND', 1899, 1656, 243], ['OH', 46530, 42679, 3851], ['OK', 22283, 20082, 2201],
    ['OR', 12316, 11415, 901], ['PA', 38860, 36735, 2125], ['RI', 2519, 2388, 131],
    ['SC', 16453, 15233, 1220], ['SD', 3764, 3189, 575], ['TN', 24408, 22017, 2391],
    ['TX', 149264, 137085, 12179], ['UT', 6402, 5929, 473], ['VT', 1334, 1217, 117],
    ['VA', 27442, 25545, 1897], ['WA', 14441, 13526, 915], ['WV', 5800, 5181, 619],
    ['WI', 22418, 20904, 1514], ['WY', 2212, 1956, 256]]

#convert to df and add year
df_2022 = pd.DataFrame(bjs_2022_data, columns=['state_abbrev','avg_jail_pop','male_pop','female_pop'])
df_2022['Year'] = 2022
df_2023 = pd.DataFrame(bjs_2023_data, columns=['state_abbrev','avg_jail_pop','male_pop','female_pop'])
df_2023['Year'] = 2023

#combine BJS data
bjs_combined = pd.concat([df_2022, df_2023], ignore_index=True)

#function to safely read Excel files
def read_xlsx_safe(file_path):
    try:
        return pd.read_excel(file_path, engine="openpyxl")
    except:
        return pd.DataFrame()

#load mental health (MH) and substance abuse (SA) facilities data
mh_2022 = read_xlsx_safe(os.path.join(data_dir, "National_Directory_MH_Facilities_2022.xlsx"))
mh_2023 = read_xlsx_safe(os.path.join(data_dir, "national-directory-mh-facilities-2023.xlsx"))
sa_2022 = read_xlsx_safe(os.path.join(data_dir, "National_Directory_SA_Facilities_2022.xlsx"))
sa_2023 = read_xlsx_safe(os.path.join(data_dir, "national-directory-su-facilities-2023.xlsx"))

#function to count facilities per state
def count_facilities(df, year, facility_type):
    if df.empty:
        return pd.DataFrame(columns=['state_abbrev','Year',facility_type])
    state_col = next((c for c in df.columns if c.lower() == 'state'), None)
    if state_col is None:
        raise ValueError(f"No 'State' column found for {year} {facility_type}")
    df_clean = df.copy()
    df_clean['state_abbrev'] = df_clean[state_col].str.strip().str.upper()
    df_clean['Year'] = year
    df_clean['facility_type'] = facility_type
    counts = df_clean.groupby('state_abbrev').size().reset_index(name='count')
    counts['Year'] = year
    counts = counts.rename(columns={'count': facility_type})
    return counts[['state_abbrev','Year',facility_type]]

#compute facility counts
mh_2022_count = count_facilities(mh_2022, 2022, 'MH')
mh_2023_count = count_facilities(mh_2023, 2023, 'MH')
sa_2022_count = count_facilities(sa_2022, 2022, 'SA')
sa_2023_count = count_facilities(sa_2023, 2023, 'SA')

#merge MH counts for 2022 and 2023
mh_counts = pd.concat([mh_2022_count.rename(columns={'MH':'MH_count'}),
                       mh_2023_count.rename(columns={'MH':'MH_count'})],
                      ignore_index=True)

#merge SA counts for 2022 and 2023
sa_counts = pd.concat([sa_2022_count.rename(columns={'SA':'SA_count'}),
                       sa_2023_count.rename(columns={'SA':'SA_count'})],
                      ignore_index=True)

#now merge MH and SA counts together by state and year
samhsa_counts = pd.merge(mh_counts, sa_counts, on=['state_abbrev','Year'], how='outer')

#fill NAs with 0
samhsa_counts[['MH_count','SA_count']] = samhsa_counts[['MH_count','SA_count']].fillna(0).astype(int)

print(samhsa_counts.head())

#merge BJS and SAMHSA data
bjs_samhsa = bjs_combined.merge(samhsa_counts, on=['state_abbrev','Year'], how='left')

#fill NAs for MH and SA
bjs_samhsa = bjs_samhsa.rename(columns={'MH_count':'MH', 'SA_count':'SA'})
bjs_samhsa[['MH','SA']] = bjs_samhsa[['MH','SA']].fillna(0).astype(int)

#check for duplicates and missing values
bjs_samhsa.duplicated().sum()
bjs_samhsa.isna().sum()

#peek at merged data
print(bjs_samhsa.head())

#reference table for states to help with acs merge
state_ref = pd.DataFrame([
    ['01','Alabama','AL'], ['02','Alaska','AK'], ['04','Arizona','AZ'], ['05','Arkansas','AR'],
    ['06','California','CA'], ['08','Colorado','CO'], ['09','Connecticut','CT'], ['10','Delaware','DE'],
    ['11','District of Columbia','DC'], ['12','Florida','FL'], ['13','Georgia','GA'], ['15','Hawaii','HI'],
    ['16','Idaho','ID'], ['17','Illinois','IL'], ['18','Indiana','IN'], ['19','Iowa','IA'],
    ['20','Kansas','KS'], ['21','Kentucky','KY'], ['22','Louisiana','LA'], ['23','Maine','ME'],
    ['24','Maryland','MD'], ['25','Massachusetts','MA'], ['26','Michigan','MI'], ['27','Minnesota','MN'],
    ['28','Mississippi','MS'], ['29','Missouri','MO'], ['30','Montana','MT'], ['31','Nebraska','NE'],
    ['32','Nevada','NV'], ['33','New Hampshire','NH'], ['34','New Jersey','NJ'], ['35','New Mexico','NM'],
    ['36','New York','NY'], ['37','North Carolina','NC'], ['38','North Dakota','ND'], ['39','Ohio','OH'],
    ['40','Oklahoma','OK'], ['41','Oregon','OR'], ['42','Pennsylvania','PA'], ['44','Rhode Island','RI'],
    ['45','South Carolina','SC'], ['46','South Dakota','SD'], ['47','Tennessee','TN'], ['48','Texas','TX'],
    ['49','Utah','UT'], ['50','Vermont','VT'], ['51','Virginia','VA'], ['53','Washington','WA'],
    ['54','West Virginia','WV'], ['55','Wisconsin','WI'], ['56','Wyoming','WY']], columns=['state_fips', 'state_name', 'state_abbrev'])

#ACS race variables of interest
race_vars = {
    "DP05_0033PE": "pct_white",
    "DP05_0037PE": "pct_black",
    "DP05_0038PE": "pct_aian",
    "DP05_0039PE": "pct_asian",
    "DP05_0044PE": "pct_nhpi",
    "DP05_0052PE": "pct_hispanic"}
race_var_list = ",".join(race_vars.keys())

#function to load ACS state race data
def load_acs_state_race(year):
    url = f"https://api.census.gov/data/{year}/acs/acs1/profile"
    params = {"get": f"{race_var_list},NAME", "for": "state:*"}
    r = requests.get(url, params=params)
    data = r.json()
    df = pd.DataFrame(data[1:], columns=data[0])
    df = df.rename(columns=race_vars)
    df["year"] = year
    keep_cols = ["state", "NAME", "year"] + list(race_vars.values())
    df = df[keep_cols]
    for col in race_vars.values():
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.rename(columns={"state": "state_fips_string", "NAME": "state_name_full"})
    df["state_fips_string"] = df["state_fips_string"].str.zfill(2)
    return df

#load ACS data for 2022 and 2023
acs_2022 = load_acs_state_race(2022)
acs_2023 = load_acs_state_race(2023)

#combine ACS data
acs_state_race = pd.concat([acs_2022, acs_2023], ignore_index=True)

#merge ACS with state reference table
acs_state_race_clean = acs_state_race.merge(
    state_ref,
    left_on="state_fips_string",
    right_on="state_fips",
    how='left')

#keep relevant columns
acs_state_race_clean = acs_state_race_clean[[
    "year","state_fips_string","state_fips","state_name","state_abbrev",
    "pct_white","pct_black","pct_aian","pct_asian","pct_nhpi","pct_hispanic"]]

print(acs_state_race_clean.head())

#prepare BJS/SAMHSA for final merge
bjs_samhsa_clean = bjs_samhsa.rename(columns={'Year': 'year'})

#merge all data
final_data = bjs_samhsa_clean.merge(acs_state_race_clean, on=['state_abbrev','year'], how='left')

#check for NAs
print(final_data.isna().sum())
print(final_data.head())

#save summary stats
numeric_cols = final_data.select_dtypes(include=np.number).columns.tolist()
summary_table = final_data[numeric_cols].describe().T.round(2).reset_index().rename(columns={'index':'Variable'})
summary_table.to_excel(os.path.join(data_dir, "final_data_summary_stats.xlsx"), index=False)
print(summary_table)











#EDA


#histm of avg carc pop
plt.figure(figsize=(8,5))
sns.histplot(final_data['avg_jail_pop'], bins=15, kde=True)
plt.title("Distribution of Average Incarcerated Population", fontsize=12, weight='bold', pad=30)
plt.text(0.5, 1.02,"Most incarceration facilities house small populations, while a few large outliers\n""hold extremely high inmate populations that right-skew the distribution.",ha='center', va='bottom',fontsize=10,transform=plt.gca().transAxes)
plt.figtext(0.01, 0.01, "SOURCE: BJS 2023", ha="left", fontsize=9)
plt.xlabel("Average Incarcerated Population")
plt.ylabel("Count")
plt.savefig("hist_avg_jail_pop.png", dpi=300, bbox_inches='tight')
plt.show()

#dist of mh facilities 
plt.figure(figsize=(8,5))
sns.histplot(final_data['MH'], bins=15, kde=True, color='skyblue')
plt.title("Distribution of Mental Health Facilities", fontsize=12, weight='bold', pad=30)
plt.text(0.5, 1.02,
"Most states have a moderate number of mental health facilities,\n"
"but a few states with high counts create a right-skewed distribution.",
ha='center', va='bottom', fontsize=10, transform=plt.gca().transAxes)
plt.figtext(0.01, 0.01, "SOURCE: SAMHSA 2022-2023", ha="left", fontsize=9)
plt.xlabel("Number of Mental Health Facilities (MH)")
plt.ylabel("Count")
plt.savefig("hist_MH.png", dpi=300, bbox_inches='tight')
plt.show()


#dist of sa facilities 
plt.figure(figsize=(8,5))
sns.histplot(final_data['SA'], bins=15, kde=True, color='salmon')
plt.title("Distribution of Substance Abuse Facilities", fontsize=12, weight='bold', pad=30)
plt.text(0.5, 1.02,
"Most states have relatively few substance abuse facilities,\n"
"while a small number of states have many creating a significant right-skew.",
ha='center', va='bottom', fontsize=10, transform=plt.gca().transAxes)
plt.figtext(0.01, 0.01, "SOURCE: SAMHSA 2022-2023", ha="left", fontsize=9)
plt.xlabel("Number of Substance Abuse Facilities (SA)")
plt.ylabel("Count")
plt.savefig("hist_SA.png", dpi=300, bbox_inches='tight')
plt.show()


#scatter MH vs avg carc pop
plt.figure(figsize=(8,5))
sns.scatterplot(data=final_data, x='MH', y='avg_jail_pop', hue='year', palette='tab10')
plt.title("Mental Health Facilities vs Average Carceral Population", fontsize=12, weight='bold', pad=30)
plt.text(0.5, 1.02,"There is a positive association between mental health\n""facilities and average incarceration popultation.", ha='center', va='bottom', fontsize=10, transform=plt.gca().transAxes)
plt.figtext(0.01, 0.01, "SOURCE: BJS & SAMHSA 2022,2023", ha="left", fontsize=9)
plt.xlabel("Number of Mental Health Facilities (MH)")
plt.ylabel("Average Jail Population")
plt.legend(title="Year", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.savefig("scatter_MH_vs_jail.png", dpi=300, bbox_inches='tight')
plt.show()

#scatter SA vs avg carc pop
plt.figure(figsize=(8,5))
sns.scatterplot(data=final_data, x='SA', y='avg_jail_pop', hue='year', palette='tab10')
plt.title("Substance Abuse Facilities vs Average Carceral Population", fontsize=12, weight='bold', pad=30)
plt.text(0.5, 1.02,"There is a positive association between substance abuse\n""facilities and average incarceration popultation.",ha='center', va='bottom', fontsize=10, transform=plt.gca().transAxes)
plt.figtext(0.01, 0.01, "SOURCE: BJS & SAMHSA 2022,2023", ha="left", fontsize=9)
plt.xlabel("Number of Substance Abuse Facilities (SA)")
plt.ylabel("Average Jail Population")
plt.legend(title="Year", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.savefig("scatter_SA_vs_jail.png", dpi=300, bbox_inches='tight')
plt.show()










#modelling 

#predictors for VIF check
predictors = ['MH', 'SA', 'pct_white', 'pct_black', 'pct_aian','pct_asian', 'pct_nhpi', 'pct_hispanic','female_pop']

#drop missing
X = final_data[predictors].dropna()

#add constant
X_const = sm.add_constant(X)

#compute VIFs
vif_data = pd.DataFrame({"variable": X_const.columns, "VIF": [variance_inflation_factor(X_const.values, i) for i in range(X_const.shape[1])]})
print(vif_data)

#panelOLS requires multi-index
final_data = final_data.set_index(['state_abbrev','year'])
y = final_data['avg_jail_pop']

#model 1: MH + SA counts only
X1 = final_data[['MH','SA']]
mod1 = PanelOLS(y, X1, entity_effects=False, time_effects=False).fit(cov_type='clustered', cluster_entity=True)
print(mod1.summary)

#model 2: State FE + MH + SA
mod2 = PanelOLS(y, X1, entity_effects=True, time_effects=False).fit(cov_type='clustered', cluster_entity=True)
print(mod2.summary)

#model 3: State FE + Counts + Demographics + female_pop + Interaction
demographics = ['pct_black','pct_aian','pct_asian','pct_nhpi','pct_hispanic','female_pop']
final_data['MHxSA'] = final_data['MH'] * final_data['SA']
X3 = final_data[['MH','SA','MHxSA'] + demographics]
mod3 = PanelOLS(y, X3, entity_effects=True, time_effects=False).fit(cov_type='clustered', cluster_entity=True)
print(mod3.summary)

#function to extract coefficients for presentation
def extract_coefs(model, model_name):
    df = pd.DataFrame({
        'Variable': model.params.index,
        'Coefficient': model.params.values,
        'Std. Error': model.std_errors.values,
        'T-stat': model.tstats.values,
        'P-value': model.pvalues.values})
    df['Significance'] = df['P-value'].apply(lambda p: '***' if p<0.01 else '**' if p<0.05 else '*' if p<0.1 else '')
    df['Model'] = model_name
    return df

#combine coefficient tables
coef_all = pd.concat([
    extract_coefs(mod1, "Model 1"),
    extract_coefs(mod2, "Model 2"),
    extract_coefs(mod3, "Model 3")], ignore_index=True)
print(coef_all)









#model efficacy 

#panel data needs to be "demeaned" to evaluate models
def demean_panel(X, y, entity=None, time=None):
    X_d = X.copy()
    y_d = y.copy()
    if entity is not None:
        X_d = X_d - X_d.groupby(entity).transform('mean')
        y_d = y_d - y_d.groupby(entity).transform('mean')
    if time is not None:
        X_d = X_d - X_d.groupby(time).transform('mean')
        y_d = y_d - y_d.groupby(time).transform('mean')
    return X_d, y_d

#get CV-RMSE function
def cv_rmse_panel(X, y, entity=None, time=None, n_splits=5):
    X_d, y_d = demean_panel(X, y, entity, time)
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    rmses = []
    for train_idx, test_idx in kf.split(X_d):
        model = LinearRegression().fit(X_d.iloc[train_idx], y_d.iloc[train_idx])
        y_pred = model.predict(X_d.iloc[test_idx])
        rmse = np.sqrt(mean_squared_error(y_d.iloc[test_idx], y_pred))
        rmses.append(rmse)
    return np.mean(rmses)

#get AIC and BIC functions
def compute_loglik(residuals):
    n = len(residuals)
    sigma2 = np.mean(residuals**2)
    return -0.5 * n * (np.log(2 * np.pi) + np.log(sigma2) + 1)

def compute_aic_bic(residuals, num_params):
    llf = compute_loglik(residuals)
    n = len(residuals)
    aic = 2*num_params - 2*llf
    bic = np.log(n)*num_params - 2*llf
    return aic, bic

#pull panel indices
state_index = final_data.index.get_level_values('state_abbrev').to_series()
time_index = final_data.index.get_level_values('year').to_series()

#calculate CV-RMSE for models
rmse1 = cv_rmse_panel(X1, y)                          
rmse2 = cv_rmse_panel(X1, y, entity='state_abbrev')  
rmse3 = cv_rmse_panel(X3, y, entity='state_abbrev')      

#model metrics
metrics = []

for X, entity, time, name, cv in zip(
    [X1, X1, X3],
    [None, 'state_abbrev', 'state_abbrev'],
    [None, None, None],
    ["Model 1: Facility counts only", 
     "Model 2: State FE + Facility counts", 
     "Model 3: State FE + Facility counts + Demographics + Interactions"],
    [rmse1, rmse2, rmse3]):
    X_d, y_d = demean_panel(X, y, entity, time)
    model = LinearRegression().fit(X_d, y_d)
    residuals = y_d - model.predict(X_d)
    num_params = X_d.shape[1]
    aic, bic = compute_aic_bic(residuals, num_params)
    r2_within = 1 - np.sum(residuals**2)/np.sum((y_d - np.mean(y_d))**2)
    metrics.append({
        "Model": name,
        "Fixed Effects": "None" if entity is None else "State",
        "CV_RMSE": cv,
        "R2_Within": r2_within,
        "AIC": aic,
        "BIC": bic})

metrics_df = pd.DataFrame(metrics)
print(metrics_df)

#coefficients for presentation
def extract_coefs(model, model_name):
    df = pd.DataFrame({
        'Variable': model.params.index,
        'Coefficient': model.params.values,
        'Std. Error': model.std_errors.values,
        'T-stat': model.tstats.values,
        'P-value': model.pvalues.values})
    df['Significance'] = df['P-value'].apply(lambda p: '***' if p<0.01 else '**' if p<0.05 else '*' if p<0.1 else '')
    df['Model'] = model_name
    return df

#combine coefficients into one table for readability
coef_all = pd.concat([
    extract_coefs(mod1, "Model 1: MH + SA Counts Only"),
    extract_coefs(mod2, "Model 2: State FE + MH + SA Counts"),
    extract_coefs(mod3, "Model 3: State FE + Counts + Demographics + Interactions")],
    ignore_index=True)

print(coef_all)

##predict 2024 using model 3 data 
#remove index in order to run this prediction
df_2024 = final_data.reset_index()
df_2024 = df_2024[df_2024['year'] == 2023].copy()
df_2024['year'] = 2024

#restore multi-index
df_2024.set_index(['state_abbrev','year'], inplace=True)

#create interaction term
df_2024['MHxSA'] = df_2024['MH'] * df_2024['SA']

#pick vars
X_pred = df_2024[['MH','SA','MHxSA','pct_black','pct_aian','pct_asian','pct_nhpi','pct_hispanic','female_pop']]

#use model 3 coefs for prediction
coefs = mod3.params
df_2024['pred_avg_jail_pop'] = (
    X_pred['MH']*coefs.get('MH',0) +
    X_pred['SA']*coefs.get('SA',0) +
    X_pred['MHxSA']*coefs.get('MHxSA',0) +
    X_pred['pct_black']*coefs.get('pct_black',0) +
    X_pred['pct_aian']*coefs.get('pct_aian',0) +
    X_pred['pct_asian']*coefs.get('pct_asian',0) +
    X_pred['pct_nhpi']*coefs.get('pct_nhpi',0) +
    X_pred['pct_hispanic']*coefs.get('pct_hispanic',0) +
    X_pred['female_pop']*coefs.get('female_pop',0))

print(df_2024[['pred_avg_jail_pop']])

#teswt/ train
panel_df = final_data.reset_index().copy()

#create interaction term
panel_df['MHxSA'] = panel_df['MH'] * panel_df['SA']

#define features and target for model 3
X_vars = ['MH','SA','MHxSA','pct_black','pct_aian','pct_asian','pct_nhpi','pct_hispanic','female_pop']
y_var = 'avg_jail_pop'

#75/25 train-test split
train_idx, test_idx = train_test_split(panel_df.index, test_size=0.25, random_state=42)
train_panel = panel_df.loc[train_idx].set_index(['state_abbrev','year'])
test_panel = panel_df.loc[test_idx].set_index(['state_abbrev','year'])

#define  features and target
y_train = train_panel[y_var]
X_train = train_panel[X_vars]
y_test = test_panel[y_var]
X_test = test_panel[X_vars]

#now train

model_train_FE = PanelOLS(y_train, X_train, entity_effects=True).fit(
    cov_type='clustered', cluster_entity=True)
print(model_train_FE.summary)

#now predict
y_pred_test = model_train_FE.predict(X_test)

#how did we do?
rmse_test = np.sqrt(mean_squared_error(y_test, y_pred_test))
r2_test = r2_score(y_test, y_pred_test)
print(f"Test Set RMSE: {rmse_test:.2f}")
print(f"Test Set R²: {r2_test:.3f}")

#save it
test_results_FE = X_test.copy()
test_results_FE['actual_jail_pop'] = y_test
test_results_FE['predicted_jail_pop'] = y_pred_test
test_results_FE.to_csv("train_test_predictions_state_FE.csv")



