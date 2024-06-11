import time
import requests
import pandas as pd
import datetime
import csv

df=pd.read_csv('ai-crypto-project-3-live-btc-krw.csv').apply(pd.to_numeric, errors='ignore')
quantity=0
cuml_pnl=0
for group in df.iterrows():
	if group[1]['side']==0:
		quantity+=group[1]['quantity']
	else:
		quantity-=group[1]['quantity']
	cuml_pnl+=group[1]['amount']
	if (-1e-7<quantity<1e-7):
		print(cuml_pnl)
		cuml_pnl=0

df.loc['Total']=pd.Series(df['amount'].sum(), index=['amount'])
print("Total PNL of file w/out considering quantity:\n", df)



