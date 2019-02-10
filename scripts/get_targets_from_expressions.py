from __future__ import print_function
import pandas as pd
import json


def generate_gene_lut(filename, id_field, name_field):
    lut = {}
    with open(filename, "r") as f:
        for l in f:
            obj = json.loads(l)
            lut[obj[id_field]] = obj[name_field]
    return lut


gene_lut = generate_gene_lut('gene_dictionary.json', 'gene_id', 'gene_name')
exp_df = pd.read_csv('expression_zscore.tsv',delimiter='\t',encoding='utf-8')
print(list(exp_df.columns.values))
print(exp_df.tail(35))

columns = (list(exp_df.columns.values))[1:-1]

for c in columns:
    tmpDF = exp_df[['ID', c]]
    filteredDF = tmpDF[tmpDF[c] >= 1]
    filteredDF['object'] = c
    filteredDF.columns = ['subject', 'score', 'object']
    filteredDF['score'] = filteredDF['score'] / 6.0
    filteredDF['subject'] = filteredDF['subject'].apply(lambda r: gene_lut[r] if r in gene_lut else '')
    filteredDF = filteredDF[filteredDF['subject'] != '']
    with open('expressions_targets.json','a') as jsonf:
        filteredDF.to_json(jsonf, orient='records', lines=True)
