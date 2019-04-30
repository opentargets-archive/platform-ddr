# Platform DDR

### Requirements

1. scala 2.11.x (through SDKMAN is simple)
2. ammonite REPL
3. the raw data (the script `from_go_to_jsonl.py` is needed to compute the slim-GO version)

### Run the scala scripts

```sh
export JAVA_OPTS="-Xms512m -Xmx3096m"
# to compute the dataset
amm computeTargets.sc
# to train the model and generate similiar targets
amm computeTargetsSimilarities.sc
```

### Generate the input file from ES5

```sh
elasticdump --input=http://localhost:9200/19.02_efo-data --output=19.02_efo-data.json --type=data --limit 10000 --sourceOnly
elasticdump --input=http://localhost:9200/19.02_gene-data --output=19.02_gene-data.json --type=data --limit 10000 --sourceOnly
elasticdump --input=http://localhost:9200/19.02_expression-data --output=19.02_expression-data.json --type=data --limit 10000 --sourceOnly
elasticdump --input=http://localhost:9200/19.02_association-data --output=19.02_association-data.json --type=data --limit=10000 --sourceOnly --searchBody '{"query": { "match_all": {} }, "_source": {"excludes": ["private.*", ".private.*"]}}'
```

# Copyright
Copyright 2014-2018 Biogen, Celgene Corporation, EMBL - European Bioinformatics Institute, GlaxoSmithKline, Takeda Pharmaceutical Company and Wellcome Sanger Institute

This software was developed as part of the Open Targets project. For more information please see: http://www.opentargets.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
