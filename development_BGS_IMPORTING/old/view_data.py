import pandas as pd

df=pd.read_csv(r'C:\Users\OGONAHT\OneDrive - Jacobs\NEAR\Python importing\GeoIndexData.txt', delimiter='\t')
print(df.head(10))
print(df.size, df.shape)