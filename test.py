import pandas as pd

# Sample DataFrame
data = {
    'id': ['desksabad', 'desksabad'],
    'symbol': ['aa', 'ccc'],
    'title': ['bb', 'ddd'],
    'discription': [3, 3],
    'force': [3, 3],
    'importent': [3, 3],
    'date': ['2023-08-13', '2023-08-13'],
    'datejalali': [20230813, 20230813],
    'repetition': ['monthly', 'monthly'],
    'timestamp': [1691872200, 1691872200]
}

df = pd.DataFrame(data)

# List of new dates
new_dates = ["2023-08-14", "2023-08-15"]

# Create a list of DataFrames with modified dates
dfs = [
    df.assign(date=new_date, datejalali=int(new_date.replace("-", "")))
    for new_date in new_dates
]

# Concatenate the list of DataFrames to create a new DataFrame
new_df = pd.concat(dfs, ignore_index=True)

# Display the new DataFrame
print(new_df)
