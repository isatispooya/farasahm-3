from persiantools.jdatetime import JalaliDate
import datetime




def gorgianIntToJalaliInt(date):
    y = str(date)[:4]
    m = str(date)[4:6]
    d = str(date)[6:8]
    Jalali = (JalaliDate.to_jalali(int(y),int(m),int(d)))
    return int(str(Jalali).replace('-',''))

