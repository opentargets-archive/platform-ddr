# OpenFDA Adverse events Pipeline

### Requirements

1. scala 2.12.x (through SDKMAN is simple)
2. ammonite REPL

### Run the scala script

```sh
export JAVA_OPTS="-Xms512m -Xmx<mostofthememingigslike100G>"
# to compute the dataset
time amm platformDataProcessFDA.sc \
    --drugSetPath "/data/jsonl/19.06_drug-data.json" \
    --inputPathPrefix "/data/eirini/raw/**/*.jsonl" \
    --outputPathPrefix /data/eirini/out
```

### Generate the drug dump from ES5 (drugSetPath file)

You will need to either connect to a machine containing the ES or forward the ssh port from it
```sh
elasticdump --input=http://localhost:9200/19.06_drug-data \
    --output=19.06_drug-data.json \
    --type=data  \
    --limit 10000 \
    --sourceOnly
```

### Access to the raw jsonl files

You just need to copy from the GCS `gs://ot-snapshots/eirini/eirini/raw/`, although that
dataset is already copied into the current machine that has been used to run this pipeline.
You might want to

1. start the machine `be-debian-worker-openfda` and login
2. copy the data if it is a empty machine otherwise dont copy or install anything
3. run the pipeline script (it will take around ~5min)
4. copy results to GCS
5. stop the machine

#### Produce the raw jsonl from scratch

In the case you may want to generate all data again even the raw data this is the
piece of bash scripts I used to produce it

```bash
gsutil cat gs://ot-snapshots/eirini/eirini/download.json | \
    jq -r '.results.drug.event.partitions[].file' > files.txt

# download all the files in files.txt using wget
#uncompress each file in a separate unique folder
files=$(ls -1 *.zip)
for f in $files; do echo unzip $f; unzip $f -d $(uuidgen -r); done

# convert the file into json lines
# my suggestion is list all produced folders with find and then split that list
# into separated files and run each file per core. 
# use split -n l/<numcores> file_with_folders.txt dirs_
# and then pass each file into this script

#!/bin/bash
dir_file=$(cat "$1")
for d in $dir_file; do
        echo folder $d
        raw_file="$d/result_lines.jsonl"
        (cd $d; cat *.json| jq -r '.results[] | @json' > result_lines.jsonl; rm -f *.json)
done
```

### Montecarlo implementation for the critical value

Here the gist link to the **R** implementation `https://gist.github.com/mkarmona/101f6f5ce3befe0996966711e847f5f0`

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
