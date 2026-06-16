import pandas as pd
df = pd.read_csv("C:\\Users\\USER\\Downloads\\legacy_schema.csv")
for x in df['column_name']:
    print(x + ',')