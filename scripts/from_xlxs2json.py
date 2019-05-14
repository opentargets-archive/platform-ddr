import json
import itertools
import pandas as pd

# http://123.59.132.21/lncrna2target/download.jsp
# https://stackoverflow.com/questions/54021712/how-to-properly-convert-an-xlsx-file-to-a-tsv-file-in-python
# https://stackoverflow.com/a/45570174/1197021

#Read excel file into a dataframe
data_xlsx = pd.read_excel('lncrna.xlsx', 'lncrna_info', index_col=None)

#Replace all columns having spaces with underscores
data_xlsx.columns = [c.replace(' ', '_').lower() for c in data_xlsx.columns]

#Replace all fields having line breaks with space
df = data_xlsx.replace('\n', ' ',regex=True)

df.to_json('lncrna.json', orient="records", lines=True)

