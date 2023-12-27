import Fnc
import time
import ApiMethods
from dataManagment import desk_broker_volumeTrade_cal, desk_broker_turnover_cal

while True:
        toDay = Fnc.toDayJalaliListYMD()
        toDayIntJal = Fnc.todayIntJalali()
        ApiMethods.GetAllTradeInDate(toDay,toDayIntJal)
        desk_broker_volumeTrade_cal()
        desk_broker_turnover_cal()
        ApiMethods.get_asset_funds()
        Fnc.getTse30LastDay()
        ApiMethods.GetAllTradeLastDate()
        ApiMethods.getAssetCoustomerByFixincome()
        time.sleep(60*5)
        print('sleep 15min loop')
