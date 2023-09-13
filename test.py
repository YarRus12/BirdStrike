import pandas as pd

df = pd.read_csv(filepath_or_buffer='isd-history.csv')
print(df.columns)
df['station'] = df['USAF'].astype(str) + df["WBAN"].astype(str)
a = df.query("`END` >= 20180101").query("`CTRY` == 'US'")
print(a[['station', 'BEGIN', 'END', 'LAT', 'LON']])


"""
INSERT INTO DDS.weather_observation
()




"""
