from setting import farasahmDb
import json


def check_config () :
    existing_config = farasahmDb['marketing_config'].find_one({"True": True})
    if not existing_config:
        return json.dumps({"reply": False, "msg": 'تنظیمات فعال یافت نشد'})
    print(existing_config)
