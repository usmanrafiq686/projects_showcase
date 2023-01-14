import pandas as pd
import numpy as np
pd.set_option('mode.use_inf_as_na', True) # discuss
df_cls = pd.read_csv('cls_data.csv', parse_dates=['Date'], index_col=['Date'],
                     dayfirst=True).fillna(method='ffill')
df_mkt_cap = pd.read_csv('mc_data.csv',
                         parse_dates=['Date'], index_col=['Date'], dayfirst=True).fillna(method="ffill")
df_vol = pd.read_csv('volume.csv',
                     parse_dates=['Date'], index_col=['Date'], dayfirst=True).fillna(method="ffill")

abs_daily_return = df_cls.apply(lambda x: abs(round(np.log(x/x.shift(1)),9))) #ABS daily return
df_pricevol = pd.DataFrame(df_vol.values*df_cls.values, columns=df_cls.columns, index=df_cls.index) #price*volume
amihud = pd.DataFrame(1000000*(abs_daily_return.values/df_pricevol.values),
                      columns=abs_daily_return.columns, index=abs_daily_return.index)
amihud.to_csv('amihud.csv', encoding='utf-8')
