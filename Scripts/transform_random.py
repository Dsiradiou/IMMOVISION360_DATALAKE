import pandas as pd
import numpy as np

INPUT  = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\filtered_elysee.csv"
OUTPUT = r"C:\Users\Siradiou\Documents\collecte et exploration de données\ImmoVision360_DataLake\Data\processed\transformed_elysee.csv"

df = pd.read_csv(INPUT, low_memory=False)
df["standardization_score"] = np.random.choice([1, 0, -1], size=len(df))
df["neighborhood_impact"]   = np.random.choice([1, 0, -1], size=len(df))
df.to_csv(OUTPUT, index=False, encoding="utf-8-sig")
print(f"[✓] {len(df)} annonces sauvegardées avec scores aléatoires")