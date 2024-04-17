from pymongo import MongoClient

def copy_data(source_client, db, target_client):
    # اتصال به دیتابیس منبع
    source_db = source_client[db]
    # اتصال به دیتابیس مقصد
    target_db = target_client[db]
    # گرفتن لیست کالکشن‌ها
    collections = source_db.list_collection_names()
    # حلقه برای کپی کردن داده‌ها از هر کالکشن
    for collection_name in collections:
        # اتصال به کالکشن منبع
        source_collection = source_db[collection_name]
        # اتصال به کالکشن مقصد
        target_collection = target_db[collection_name]
        # گرفتن تمامی داده‌ها از کالکشن منبع
        data_to_copy = source_collection.find()
        # حلقه برای قرار دادن داده‌ها در کالکشن مقصد
        for data in data_to_copy:
            # چک کردن وجود رکورد با همان مقادیر کلیدی در کالکشن مقصد
            existing_record = target_collection.find_one({"_id": data["_id"]})
            if existing_record:
                # اگر رکورد با مقادیر کلیدی مشابه وجود داشت، آن را به‌روزرسانی می‌کنیم
                target_collection.update_one({"_id": data["_id"]}, {"$set": data})
            else:
                # در غیر این صورت رکورد جدید را درج می‌کنیم
                target_collection.insert_one(data)
        print(f"Data has been copied from {db}.{collection_name} to {db}.{collection_name} successfully!")



# اتصال به دیتابیس MongoDB منوگو دیبی
client_mongodebi = MongoClient()

# اتصال به دیتابیس MongoDB کلود
client_cloud = MongoClient('mongodb://root:6f43J51WDN7RRZkncc832Krd@sinai.liara.cloud:34139/my-app?authSource=admin')

# فراخوانی تابع برای کپی کردن تمامی کالکشن‌ها
copy_data(client_mongodebi, 'farasahm2', client_cloud)
