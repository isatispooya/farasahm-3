
import pandas as pd
from setting import farasahmDb



df = pd.DataFrame(farasahmDb['newComer'].find({'symbol':'visa'},{'_id':0,'date':1,'allVol':1,'allCntBuy':1,'newCnt':1,'allCntSel':1}))
df.columns =['date','volume','countBuyer','countNewBuyer - label','countSeller']

tradeDf = pd.DataFrame(farasahmDb['trade'].find({'symbol':'visa'},{'_id':0,'قیمت هر سهم':1,'تعداد سهم':1,'تاریخ معامله':1}))
tradeDf['value'] = tradeDf['تعداد سهم'] * tradeDf['قیمت هر سهم']
tradeDf = tradeDf.groupby('تاریخ معامله').sum().reset_index()
tradeDf['قیمت هر سهم'] = tradeDf['value'] / tradeDf['تعداد سهم']
tradeDf = tradeDf.drop(columns='تعداد سهم')
tradeDf.columns = ['date','price','value']
df = df.set_index('date').join(tradeDf.set_index('date'))
df['perSellerValue'] = df['value'] / df['countSeller']
df['perSellerVolume'] = df['volume'] / df['countSeller']
df['perBuyerValue'] = df['value'] / df['countBuyer']
df['perBuyerVolume'] = df['volume'] / df['countBuyer']
df['powerBuyerToSeller'] = df['perBuyerValue'] / df['perSellerValue']
df = df.sort_index()

for i in ['price', 'volume','value','countSeller','perSellerValue','perSellerVolume','perBuyerValue','countBuyer','powerBuyerToSeller']:
    for j in [5,10,20,50]:
        df[f'avg_{i}_{j}'] = df[i].rolling(window=j,min_periods=1).mean()
        df[f'avg_{i}_{j}/{i}'] = df[f'avg_{i}_{j}'] / df[i]
        df[f'max_{i}_{j}'] = df[i].rolling(window=j,min_periods=1).max()
        df[f'max_{i}_{j}/{i}'] = df[f'max_{i}_{j}'] / df[i]
        df[f'min_{i}_{j}'] = df[i].rolling(window=j,min_periods=1).min()
        df[f'min_{i}_{j}/{i}'] = df[f'min_{i}_{j}'] / df[i]
        df[f'std_{i}_{j}'] = df[i].rolling(window=j,min_periods=1).std()
        df[f'std_{i}_{j}/{i}'] = df[f'std_{i}_{j}'] / df[i]
        df[f'rank_{i}_{j}'] = df[i].rolling(window=j,min_periods=1).rank()
        df[f'rank_{i}_{j}/{i}'] = df[f'rank_{i}_{j}'] / df[i]
        df[f'median_{i}_{j}'] = df[i].rolling(window=j,min_periods=1).median()
        df[f'median_{i}_{j}/{i}'] = df[f'median_{i}_{j}'] / df[i]
        df[f'var_{i}_{j}'] = df[i].rolling(window=j,min_periods=1).var()
        df[f'var_{i}_{j}/{i}'] = df[f'var_{i}_{j}'] / df[i]
        df[f'std_{i}_{j}/avg_{i}_{j}'] = df[f'std_{i}_{j}'] / df[f'avg_{i}_{j}']
        df[f'max_{i}_{j}/min_{i}_{j}'] = df[f'max_{i}_{j}'] / df[f'min_{i}_{j}']
        df[f'avg_{i}_{j}/median_{i}_{j}'] = df[f'avg_{i}_{j}'] / df[f'median_{i}_{j}']
        df[f'avg_{i}_{j}/median_{i}_{j}'] = df[f'avg_{i}_{j}'] / df[f'min_{i}_{j}']
        df[f'avg_{i}_{j}/max_{i}_{j}'] = df[f'avg_{i}_{j}'] / df[f'max_{i}_{j}']

for i in df.columns:
    df[f'']