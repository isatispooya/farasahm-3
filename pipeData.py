import Fnc
import time
import ApiMethods

while True:
        toDay = Fnc.toDayJalaliListYMD()
        toDayIntJal = Fnc.todayIntJalali()
        ApiMethods.GetAllTradeInDate(toDay,toDayIntJal)
        ApiMethods.get_asset_funds()
        Fnc.getTse30LastDay()
        ApiMethods.GetAllTradeLastDate()
        ApiMethods.getAssetCoustomerByFixincome()
        time.sleep(60*5)
        print('sleep 15min loop')
