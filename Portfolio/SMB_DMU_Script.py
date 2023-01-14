import pandas as pd
from scipy import stats
import numpy as np

df_cls = pd.read_csv('cls_data.csv', parse_dates=['Date'], index_col=['Date'], dayfirst=True).fillna(method='ffill')
df_mkt_cap = pd.read_csv('mc_data.csv', parse_dates=['Date'], index_col=['Date'], dayfirst=True).fillna(method="ffill")

weighted_values = pd.DataFrame()
total_cap['Total Cap'] = df_mkt_cap.sum(axis=1).to_frame()

#######---------------------------------------------------#########
names = list(df_mkt_cap.columns.values)
temp_1 = []
temp_2 = []
count = 0
return_val = df_cls.apply(lambda x: round(np.log(x/x.shift(1)),9)) #daily reutrn
MKT = df_mkt_cap.apply(lambda x: ((x/df_mkt_cap.sum(axis=1)))).mul(return_val.to_numpy())
MKT['MKT'] = CMKT.sum(axis=1)

######--------------------------------------------------------#########
total_cap = pd.DataFrame()
total_cap['Total Cap'] = df_mkt_cap.sum(axis=1).to_frame()
df_mkt_cap['ten_per'] = total_cap['Total Cap'] * 0.1

df_mkt_cap['Top_90'] = df_mkt_cap.apply(lambda x: list(x.index[x > x.ten_per]), axis=1)
df_mkt_cap['Below_10'] = df_mkt_cap.drop('Top_90', axis=1).apply(lambda x: list(x.index[x < x.ten_per]), axis=1)

below_10 = []
for i in return_val.index:
    sp = df_mkt_cap.at[i, "Below_10"]
    t = []
    if len(sp) == 0:
        pass
    if len(sp) > 0:
        for k in range(len(sp)):
            t.append(return_val.shift(1).at[i, sp[k]])
    below_10.append(t)
df_below_10 = pd.DataFrame()
df_below_10['Below_10_Return_Prev_Values'] = below_10
df_below_10 = df_below_10.set_index(return_val.index)
# ######-------------------------------------------########
top_90 = []
df_top_90 = pd.DataFrame()
for j in return_val.index:
    sp1 = df_mkt_cap.at[j, "Top_90"]
    t1 = []
    if len(sp1) == 0:
        pass
    if len(sp1) > 0:
        for f in range(len(sp1)):
            t1.append(return_val.shift(1).at[j, sp1[f]])
    top_90.append(t1)
############-------------------------------------------------------######################
df_top_90['Top_90_Return_Prev_Values'] = top_90
df_top_90 = df_top_90.set_index(return_val.index)

df_below_10['Small_Port_Names'] = pd.DataFrame(df_mkt_cap['Below_10'])
df_top_90['Big_Port_Name'] = pd.DataFrame(df_mkt_cap['Top_90'])
df_top_90['Percentile_Top_90'] = df_top_90['Top_90_Return_Prev_Values'].apply(
    lambda x: [round(stats.percentileofscore(x, a1, 'rank'), 7) for a1 in x])
df_below_10['Percentile_Below_10'] = df_below_10["Below_10_Return_Prev_Values"].apply(
    lambda x: [round(stats.percentileofscore(x, b1, 'rank'), 7) for b1 in x])
df_below_10['Indexes_SU'] = df_below_10['Percentile_Below_10'].apply(
    lambda x: [a2 for a2, value_1 in enumerate(x) if value_1 > 70.0])
df_below_10['Indexes_SM'] = df_below_10['Percentile_Below_10'].apply(
    lambda x: [a3 for a3, value_2 in enumerate(x) if value_2 > 30.0 < 70.0])
df_below_10['Indexes_SD'] = df_below_10['Percentile_Below_10'].apply(
    lambda x: [a4 for a4, value in enumerate(x) if value < 30.0])

df_below_10['Names_SU'] = df_below_10.apply(lambda x: [x['Small_Port_Names'][index_su] for index_su in x['Indexes_SU']], axis=1)
df_below_10['Names_SM'] = df_below_10.apply(lambda x: [x['Small_Port_Names'][index_sm] for index_sm in x['Indexes_SM']], axis=1)
df_below_10['Names_SD'] = df_below_10.apply(lambda x: [x['Small_Port_Names'][index_sd] for index_sd in x['Indexes_SD']], axis=1)

df_top_90['Indexes_BU'] = df_top_90['Percentile_Top_90'].apply(
    lambda x: [counter_1 for counter_1, value in enumerate(x) if value > 70.0])
df_top_90['Indexes_BM'] = df_top_90['Percentile_Top_90'].apply(
    lambda x: [counter_2 for counter_2, value in enumerate(x) if value > 30.0 < 70.0])
df_top_90['Indexes_BD'] = df_top_90['Percentile_Top_90'].apply(
    lambda x: [counter_3 for counter_3, value in enumerate(x) if value < 30.0])

df_top_90['Names_BU'] = df_top_90.apply(lambda x: [x['Big_Port_Name'][index_bu] for index_bu in x['Indexes_BU']], axis=1)
df_top_90['Names_BM'] = df_top_90.apply(lambda x: [x['Big_Port_Name'][index_bm] for index_bm in x['Indexes_BM']], axis=1)
df_top_90['Names_BD'] = df_top_90.apply(lambda x: [x['Big_Port_Name'][index_bd] for index_bd in x['Indexes_BD']], axis=1)

df_strategies = pd.concat([df_below_10, df_top_90], axis=1, join='outer')
#######-----------------Return of Small UP-------------------------###############
bl = []
for i in return_val.index:
    sp = df_below_10.at[i, 'Names_SM']
    t1 = []
    if len(sp) == 0:
        pass
    if len(sp) > 0:
        for k in range(len(sp)):
            a = return_val.at[i, sp[k]]
            t1.append(a)
    bl.append(t1)
rtn_SU = pd.DataFrame()
rtn_SU['SU_AVG_Return'] = bl
rtn_SU['SU_AVG_Return'] = rtn_SU['SU_AVG_Return'].apply(lambda x: sum(x) / len(x) if (len(x) > 0) else len(x))
rtn_SU = rtn_SU.set_index(return_val.index)
#######--------------------Return of Small Medium----------------------###############
b2 = []
for i in return_val.index:
    sp = df_below_10.at[i, 'Names_SU']
    t2 = []
    if len(sp) == 0:
        pass
    if len(sp) > 0:
        for k in range(len(sp)):
            a = return_val.at[i, sp[k]]
            t2.append(a)
    b2.append(t2)
rtn_SM = pd.DataFrame()
rtn_SM['SM_AVG_Return'] = b2
rtn_SM['SM_AVG_Return'] = rtn_SM['SM_AVG_Return'].apply(lambda x: sum(x) / len(x) if (len(x) > 0) else len(x))
rtn_SM = rtn_SM.set_index(return_val.index)

#######--------------------Return of Small Down----------------------###############
b3 = []
for i in return_val.index:
    sp = df_below_10.at[i, 'Names_SD']
    t3 = []
    if len(sp) == 0:
        pass
    if len(sp) > 0:
        for k in range(len(sp)):
            a = return_val.at[i, sp[k]]
            t3.append(a)
    b3.append(t3)
rtn_SD = pd.DataFrame()
rtn_SD['SD_AVG_Return'] = b3
rtn_SD['SD_AVG_Return'] = rtn_SD['SD_AVG_Return'].apply(lambda x: sum(x) / len(x) if (len(x) > 0) else len(x))
rtn_SD = rtn_SD.set_index(return_val.index)

#######--------------------Return of Big Up----------------------###############
b4 = []
for i in return_val.index:
    sp = df_top_90.at[i, 'Names_BU']
    t4 = []
    if len(sp) == 0:
        pass
    if len(sp) > 0:
        for k in range(len(sp)):
            a = return_val.at[i, sp[k]]
            t4.append(a)
    b4.append(t4)
rtn_BU = pd.DataFrame()
rtn_BU['BU_AVG_Return'] = b4
rtn_BU['BU_AVG_Return'] = rtn_BU['BU_AVG_Return'].apply(lambda x: sum(x) / len(x) if (len(x) > 0) else len(x))
rtn_BU = rtn_BU.set_index(return_val.index)

#######--------------------Return of Big Medium----------------------###############
b5 = []
for i in return_val.index:
    sp = df_top_90.at[i, 'Names_BM']
    t5 = []
    if len(sp) == 0:
        pass
    if len(sp) > 0:
        for k in range(len(sp)):
            a = return_val.at[i, sp[k]]
            t5.append(a)
    b5.append(t5)
rtn_BM = pd.DataFrame()
rtn_BM['BM_AVG_Return'] = b5
rtn_BM['BM_AVG_Return'] = rtn_BM['BM_AVG_Return'].apply(lambda x: sum(x) / len(x) if (len(x) > 0) else len(x))
rtn_BM = rtn_BM.set_index(return_val.index)

#######--------------------Return of Big Down----------------------###############
b6 = []
for i in return_val.index:
    sp = df_top_90.at[i, 'Names_BD']
    t6 = []
    if len(sp) == 0:
        pass
    if len(sp) > 0:
        for k in range(len(sp)):
            a = return_val.at[i, sp[k]]
            t6.append(a)
    b6.append(t6)
rtn_BD = pd.DataFrame()

rtn_BD['BD_AVG_Return'] = b6
rtn_BD['BD_AVG_Return'] = rtn_BD['BD_AVG_Return'].apply(lambda x: sum(x) / len(x) if (len(x) > 0) else len(x))
rtn_BD = rtn_BD.set_index(return_val.index)
df_port_return = pd.concat([rtn_SU, rtn_SM, rtn_SD, rtn_BU, rtn_BM, rtn_BD], axis=1, join='outer')
df_port_return['SMB'] = 1.0 / 3.0 * (df_port_return['SU_AVG_Return'] +
                                     df_port_return['SM_AVG_Return'] +
                                     df_port_return['SD_AVG_Return']) - 1.0 / 3.0 * (df_port_return['BU_AVG_Return']
                                                                                     + df_port_return['BM_AVG_Return']
                                                                                     + df_port_return['BD_AVG_Return'])

df_port_return['DMU'] = 1.0 / 2.0 * (df_port_return['BD_AVG_Return']
                                     + df_port_return['SD_AVG_Return']) - 1.0 / 2.0 * (df_port_return['BU_AVG_Return']
                                                                                       + df_port_return[
                                                                                           'SU_AVG_Return'])
df_port_return.to_csv('MOM_prof.csv', encoding = 'utf-8')
