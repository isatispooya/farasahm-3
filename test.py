import pymongo
client = pymongo.MongoClient()
farasahmDb = client['farasahm2']



listCompany = [
    {'symbol':'visa','fullname':'سرمايه گذاري ايساتيس پويا','name':'ویسا','icon':'visa.png','type':'Bourse'},
    {'symbol':'fevisa','fullname':'صنایع مفتول ایساتس پویا','name':'فویسا','icon':'fevisa.png','type':'NoBourse'},
    {'symbol':'devisa','fullname':'کارگزاری رسمی بیمه ایساتیس پویا','name':'دویسا','icon':'devisa.png','type':'NoBourse'},
    {'symbol':'bevisa','fullname':'بیمه زندگی ایساتیس','name':'بویسا','icon':'bevisa.png','type':'NoBourse'},
    {'symbol':'sevisa','fullname':'عمرانی و گردشگری سفیران پردیسان یزد','name':'ثویسا','icon':'sevisa.png','type':'NoBourse'},
    {'symbol':'evisa','fullname':'کارگزاري ايساتيس پويا','name':'اویسا','icon':'evisa.png','type':'NoBourse'},
    {'symbol':'nevisa','fullname':'سبدگردانی ایساتیس پویا','name':'نویسا','icon':'nevisa.png','type':'NoBourse'},
    {'symbol':'nikvisa','fullname':'توسعه سرمایه و انرژی نیک ایساتیس','name':'نیک ویسا','icon':'visa.png','type':'NoBourse'},
    {'symbol':'hevisa','fullname':'سرمایه هوشمند دیجیتال فردا','name':'هویسا','icon':'hevisa.png','type':'NoBourse'},
    {'symbol':'bahar','fullname':'خدمات بیمه ای زندگی برتر سفیران بهار ما','name':'بهار','icon':'bahar.png','type':'NoBourse'},
    {'symbol':'modiriat','fullname':'سرمايه گذاري ايساتيس پويا','name':'ویسا','icon':'modiriat.png','type':'NoBourse'},
    {'symbol':'visa','fullname':'توسعه و مدیریت سرمایه','name':'مدیریت','icon':'visa.png','type':'NoBourse'},
    {'symbol':'safiran','fullname':'سرمایه گذاری سفیران بهار یزد','name':'سفیران','icon':'safiran.png','type':'NoBourse'},
    {'symbol':'shirkohe','fullname':'نمایندگی بیمه زندگی برتر کارآفرین شیرکوه','name':'شیرکوه','icon':'shirkohe.png','type':'NoBourse'},
    {'symbol':'toloe','fullname':'نمایندگی بیمه طلوع زندگی امن سرمد','name':'طلوع','icon':'toloe.png','type':'NoBourse'},
    {'symbol':'varzesh','fullname':'فرهنگی  ورزشی ایساتیس پویا','name':'ورزش','icon':'varzesh.png','type':'NoBourse'},
    {'symbol':'yazdan','fullname':'سرمایه گذاری سرزمین کویر یزدان','name':'یزدان','icon':'yazdan.png','type':'NoBourse'},
    {'symbol':'gevisa','fullname':'گروه مالی و سرمایه گذاری ایساتیس پویا','name':'گویسا','icon':'gevisa.png','type':'NoBourse'},
    {'symbol':'niko','fullname':'نیکو خوب روشنای کویر برتر','name':'نیکو','icon':'niko.png','type':'NoBourse'},
]

farasahmDb['companyList'].insert_many(listCompany)