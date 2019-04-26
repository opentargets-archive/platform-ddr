from __future__ import print_function
import pandas as pd
import json
from opentargets import OpenTargetsClient
import more_itertools as miters


c = OpenTargetsClient()

targets = c.get_associations_for_disease(disease="EFO_0003767", direct=True)

for t in targets.to_object():
    print(t.target.gene_info.symbol)