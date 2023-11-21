import Fnc
import time
import ApiMethods
from dataManagment import desk_broker_volumeTrade_cal, desk_broker_turnover_cal
from crawlingTse import TseCrawling

Tse = TseCrawling()

while True:
        #Fnc.getTseDate()
        #Tse.getOragh()
        #Tse.get_all_fund()
        Tse.getAmariNav()
        #time.sleep(60*10)
        #print('sleep 10 min one loop')



