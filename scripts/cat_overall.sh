#!/bin/bash

target_file=$1
output_file=$2

for t in $(cat $target_file)
do
    cat ../output-all/part-*  | jq -r "select (.subject == \"${t}\")| .subject_synonyms[]._1" >> $output_file
done

