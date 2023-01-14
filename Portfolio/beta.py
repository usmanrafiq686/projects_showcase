import pandas as pd
import numpy as np
pd.set_option('mode.use_inf_as_na', True)
import statsmodels.api as sm
from statsmodels.regression.rolling import RollingOLS
df_cls = pd.read_csv('D:\DFKI_Job\Data_code_4chan\Compile_smaller_data\Final_file_cls_data.csv',
                     parse_dates=['Date'], index_col=['Date'], dayfirst=True).fillna(method='ffill') #readcsv with FFIL
df_mkt_cap = pd.read_csv('D:\DFKI_Job\Data_code_4chan\Compile_smaller_data\Final_file_mc_data.csv',
                         parse_dates=['Date'], index_col=['Date'], dayfirst=True).fillna(method="ffill")

df_fm = pd.read_csv('D:\DFKI_Job\Data_code_4chan\Compile_smaller_data\Fama_french_three_factor_17_21.csv',
                    parse_dates=['Date'], index_col=['Date'], dayfirst=True)

daily_return = df_cls.apply(lambda x: round(np.log(x/x.shift(1)),9)) #daily reutrn
df = pd.merge(daily_return,df_fm['RF'], on='Date', how='left')
CMKT = df_mkt_cap.apply(lambda x: ((x/df_mkt_cap.sum(axis=1)))).mul(daily_return.to_numpy())
CMKT['CMKT'] = CMKT.sum(axis=1) #ind_var
dep_df = daily_return.apply(lambda x: x - df['RF'], axis=0) # Ri-Rf
list_beta=[]
list_std_error=[]
exog = sm.add_constant(CMKT["CMKT"])
for i in range(len(dep_df.columns)):
    endog = dep_df.iloc[:,i]
    exog = sm.add_constant(CMKT["CMKT"])
    rols = RollingOLS(endog, exog, window=365, missing='drop')
    reg = rols.fit().params.iloc[:,0] #beta
    reg.name = endog.name
    std_err = rols.fit().bse.iloc[:,0] #error
    std_err.name = endog.name
    list_std_error.append(std_err)
    list_beta.append(reg)
beta = pd.DataFrame(list_beta)
std_error = pd.DataFrame(list_std_error)
beta.to_csv('beta_check.csv', encoding='utf-8')
std_error.to_csv('std_error.csv', encoding='utf-8')