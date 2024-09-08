from setting import farasahmDb
import pandas as pd

df = pd.DataFrame(farasahmDb['registerNoBours'].find({'symbol':'devisa'}))
df = df[df['date'] == df['date'].max()]

print(df['date'].max())