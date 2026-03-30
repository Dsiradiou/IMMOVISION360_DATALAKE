import pandas as pd

df = pd.read_csv(r'C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\filtered_elysee.csv')

print('=== SHAPE ===')
print(df.shape)

print('\n=== NaN PAR COLONNE ===')
nan = df.isna().sum()
print(nan[nan > 0].sort_values(ascending=False))

print('\n=== STATS NUMERIQUES ===')
print(df.describe().round(2))

print('\n=== TYPES ===')
print(df.dtypes)