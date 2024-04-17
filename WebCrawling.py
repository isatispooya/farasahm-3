import Fnc
import time
from crawlingTse import TseCrawling

Tse = TseCrawling()

while True:
        Fnc.getTseDate()
        Tse.getOragh()
        Tse.getOraghBoursi()
        Tse.getAmariNav()
        time.sleep(60*10)
        print('sleep 10 min one loop')



