import pandas as pd
import pymongo
client = pymongo.MongoClient()
import json


client = pymongo.MongoClient()

farasahmDb = client['farasahm2']

symbols = ['visa','bazargam']

for sym in symbols:
    dfTradeStation = pd.DataFrame(farasahmDb['trade'].find({'symbol':sym},{'_id':0,'تاریخ معامله':1,'کد خریدار':1,'نام کارگزار خریدار':1,'کد فروشنده':1,'نام کارگزار فروشنده':1}))
    dates = pd.DataFrame(farasahmDb['register'].find({'symbol':sym},{'_id':0,'تاریخ گزارش':1}))['تاریخ گزارش'].to_list()
    for d in dates:
        print(sym,d)
        dfTradeStationd = dfTradeStation[dfTradeStation['تاریخ معامله']<=d]
        dfTradeStationd = dfTradeStationd.sort_values('تاریخ معامله',ascending=False)
        dfTradeStationBy = dfTradeStationd.copy()[['کد خریدار','نام کارگزار خریدار']]
        dfTradeStationSel = dfTradeStationd.copy()[['کد فروشنده','نام کارگزار فروشنده']]
        dfTradeStationBy = dfTradeStationBy.drop_duplicates(subset=['کد خریدار'],keep='first')
        dfTradeStationSel = dfTradeStationSel.drop_duplicates(subset=['کد فروشنده'],keep='first')
        dfRegister = farasahmDb['register'].find({'symbol':sym,'تاریخ گزارش':d},{'_id':0})
        dfTradeStationBy = dfTradeStationBy[dfTradeStationBy['کد خریدار'].isin(dfRegister['کد سهامداری'].to_list())]
        dfTradeStationSel = dfTradeStationSel[dfTradeStationSel['کد فروشنده'].isin(dfRegister['کد سهامداری'].to_list())]
        dfTradeStationBy = dfTradeStationBy.rename(columns={'کد خریدار':'کد سهامداری','نام کارگزار خریدار':'اخرین کارگزاری خرید'})
        dfTradeStationSel = dfTradeStationSel.rename(columns={'کد فروشنده':'کد سهامداری','نام کارگزار فروشنده':'اخرین کارگزاری فروش'})
        dfRegister = dfRegister.set_index('کد سهامداری').join(dfTradeStationBy.set_index('کد سهامداری'),how='left')
        dfRegister = dfRegister.join(dfTradeStationSel.set_index('کد سهامداری'),how='left').reset_index()
        dfRegister = dfRegister.fillna('')
        brokerList = farasahmDb['borkerList'].find({},{'_id':0})
        for brk in brokerList:
            name = list(brk.keys())[0]
            members = brk[name]
            for m in members:
                dfRegister.loc[dfRegister["اخرین کارگزاری خرید"].str.contains(m), "اخرین کارگزاری خرید"] = m
                dfRegister.loc[dfRegister["اخرین کارگزاری فروش"].str.contains(m), "اخرین کارگزاری فروش"] = m
        farasahmDb['register'].delete_many({'تاریخ گزارش':d,'symbol':sym})
        farasahmDb['register'].insert_many(dfRegister.to_dict('records'))

