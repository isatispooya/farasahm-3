from pymongo import MongoClient

# آدرس سرور MongoDB
host = '192.168.11.13'
# پورت پیش‌فرض MongoDB
port = 27017


client = MongoClient(host, port)
farasahmDb = client['farasahm2']




