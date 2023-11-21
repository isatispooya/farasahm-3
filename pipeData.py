import Fnc
import time
import ApiMethods
from dataManagment import desk_broker_volumeTrade_cal, desk_broker_turnover_cal
from crawlingTse import TseCrawling

Tse = TseCrawling()

while True:
    
        ApiMethods.GetAllTradeInDate()
        desk_broker_volumeTrade_cal()
        desk_broker_turnover_cal()
        ApiMethods.get_asset_funds()
        Fnc.getTse30LastDay()
        ApiMethods.GetAllTradeLastDate()
        time.sleep(60*3)
        print('sleep 60 one loop')




