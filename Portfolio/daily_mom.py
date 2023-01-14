import pandas as pd
import numpy as np
df_cls = pd.read_csv('D:\DFKI_Job\Data_code_4chan\Compile_smaller_data\Final_file_cls_data.csv',
                     parse_dates=['Date'], index_col=['Date'], dayfirst=True).fillna(method='ffill')

#######---------------------------------------------------#########
daily_return = pd.DataFrame()
daily_return = df_cls.apply(lambda x: round(np.log(x/x.shift(1)),5))
previous_day_return = daily_return.shift(1).dropna(how='all')
"""
Winner calculation
"""
previous_day_return['quantile'] = previous_day_return.apply(lambda row: pd.qcut([a for a in row ], 5, labels=False, duplicates='drop').tolist(), axis=1)
previous_day_return['winner_index'] = previous_day_return['quantile'].apply(lambda row: [i for i, e in enumerate(row) if e == 4])
previous_day_return['winner'] = previous_day_return['winner_index'].apply(lambda row: [previous_day_return.columns[i] for i in row])
winner_list = []
for i in previous_day_return.index:
    DR_valve = previous_day_return.at[i, 'winner']
    win_temp = []
    if len(DR_valve) == 0:
        pass
    if len(DR_valve) > 0:
        for k in range(len(DR_valve)):
            a = daily_return.at[i, DR_valve[k]]
            win_temp.append(a)
    winner_list.append(win_temp)

previous_day_return['winner_daily_return'] = winner_list
previous_day_return['winner_daily_return_mean'] = previous_day_return['winner_daily_return'].apply(lambda row: round(sum(row) / len(row),5) if (len(row) > 0) else len(row))
"""
Looser calculation
"""
previous_day_return['looser_index'] = previous_day_return['quantile'].apply(lambda row: [i for i, e in enumerate(row) if e == 0])
previous_day_return['looser'] = previous_day_return['looser_index'].apply(lambda row: [previous_day_return.columns[i] for i in row])
looser_list = []
for i in previous_day_return.index:
    loos_DR_valve = previous_day_return.at[i, 'looser']
    loos_temp = []
    if len(loos_DR_valve) == 0:
        pass
    if len(loos_DR_valve) > 0:
        for k in range(len(loos_DR_valve)):
            a = daily_return.at[i, loos_DR_valve[k]]
            loos_temp.append(a)
    looser_list.append(loos_temp)

previous_day_return['looser_daily_return'] = looser_list
previous_day_return['looser_daily_return_mean'] = previous_day_return['looser_daily_return'].apply(lambda row: round(sum(row) / len(row),5) if (len(row) > 0) else len(row))
previous_day_return['mom_profit'] = previous_day_return['winner_daily_return_mean'] - previous_day_return['looser_daily_return_mean']
previous_day_return.to_csv('quantiles.csv', encoding = 'utf-8')