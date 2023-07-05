import pandas as pd

# تعیین مسیر فایل و رمز آن
file_path = '"C:\Users\isatis pouya\Desktop\New folder (3)\ww.xlsx"'
password = 'your_password'

# خواندن داده‌ها از فایل اکسل با رمز
try:
    df = pd.read_excel(file_path, engine='openpyxl', sheet_name='Sheet1', password=password)
except Exception as e:
    print("خطا در باز کردن فایل: ", e)
    exit(1)

# اکنون داده‌ها بدون رمز در دیتافریم `df` ذخیره شده‌اند و می‌توانید مراحل دیگر را با آن انجام دهید

# برای نمایش دیتافریم بدون رمز
print(df)

# برای ذخیره دیتافریم در فایل جدید بدون رمز
output_file_path = 'output_file.xlsx'
try:
    df.to_excel(output_file_path, index=False, engine='openpyxl')
    print("فایل بدون رمز با موفقیت ذخیره شد.")
except Exception as e:
    print("خطا در ذخیره فایل: ", e)
