from setting import farasahmDb

fields = ["TradeNumber", "TradeCode", "TradeSymbol", 'dateInt', 'NetPrice']
batch_size = 10000  # تعداد رکوردها در هر دسته

# استفاده از نشانگر برای پردازش اسناد به صورت دسته‌ای
cursor = farasahmDb['TradeListBroker'].find({}, projection={field: 1 for field in fields}).batch_size(batch_size)

processed_count = 0

while True:
    batch = []
    try:
        # جمع‌آوری اسناد به صورت دسته‌ای
        for _ in range(batch_size):
            batch.append(next(cursor))
    except StopIteration:
        # پایان اسناد
        if not batch:
            break

    # گروه‌بندی و پیدا کردن تکراری‌ها
    duplicates = farasahmDb['TradeListBroker'].aggregate([
        {"$match": {"_id": {"$in": [doc["_id"] for doc in batch]}}},
        {"$group": {
            "_id": {field: f"${field}" for field in fields},
            "uniqueIds": {"$addToSet": "$_id"},
            "count": {"$sum": 1}
        }},
        {"$match": {"count": {"$gt": 1}}}
    ])
    
    
    dropimg = 0
    # حذف اسناد تکراری
    for duplicate in duplicates:
        unique_ids = duplicate["uniqueIds"]
        unique_ids.pop(0)
        # حذف اسناد تکراری
        farasahmDb['TradeListBroker'].delete_many({"_id": {"$in": unique_ids}})
        dropimg = dropimg +1
        

    
    len_batch = len(batch)/1000000
    processed_count += len_batch
    print(f"processing: {processed_count}, dropimg:{dropimg}")

print("Duplicate documents processing completed.")
