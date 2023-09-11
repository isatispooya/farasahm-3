import function
import ApiMethods
import pandas as pd
import encryption
import datetime
import pymongo
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']





for month in range(5,7):
    beforTime = datetime.datetime.now()
    for day in range(1,32):
        pipeline = [{"$match": {"DateYrInt": 1402,"DateMnInt": month,"DateDyInt":day}},{"$group": {"_id": None,"max_page": { "$max": "$page" }}}]
        result = list(farasahmDb['TradeListBroker'].aggregate(pipeline))
        if len(result)>0:
             page = result[0]["max_page"] + 1
        else:
            page = 1
        while True:
                dilay = (datetime.datetime.now()- beforTime).total_seconds()
                beforTime = datetime.datetime.now()
                print(1402,month,day,' => ',page,' Time: ' ,dilay)
                df = ApiMethods.GetDailyTradeList(1402,month,day,page,1000)
                if len(df) == 0:
                    break
                df = pd.DataFrame(df)
                df['DateYrInt'] = 1402
                df['DateMnInt'] = month
                df['DateDyInt'] = day
                df['page'] = page
                df['Update'] = datetime.datetime.now()
                df = df.to_dict('records')
                farasahmDb['TradeListBroker'].delete_many({'DateYrInt':1402,'DateMnInt':month,'DateYrDateDyIntInt':day,'page':page})
                farasahmDb['TradeListBroker'].insert_many(df)
                page = page + 1
