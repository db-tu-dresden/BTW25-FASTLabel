#!/bin/bash

# Seeds to be used
integers=(47 11 95 83 38)
db_name="<db_name>"
query_path="<query_path/>"
config_path="<config_path/>"
archive_path="<archive_path>.csv"
statistics="<statistic_path/>"
use_contexts=true
encoded_query_path="<path_to_pre_encoded_queries>.json"

# Loop over seeds
for i in "${integers[@]}"; do
    # Loop over splits 10 to 100 with a step of 10
    for j in $(seq 0.1 0.1 1.0); do
        j_int=$(echo "$j * 100 / 1" | bc)
        echo "Running FASTgres prediction at split: $j_int, seed: $i"
        save_name="<save_path/save_name>_${j_int}_seed_${i}.csv"

        # leave -ecp if no queries were pre-encoded -> note this might take longer for more combinations
        python evaluate_workload_simple.py "$query_path" -o "$save_name" -c "$config_path" -db "$db_name" \
        -a "$archive_path" "$([ $use_contexts = true ] && echo "-uc")" -ts "$j" -stats "$statistics" \
        -ecp "$encoded_query_path" -s "$i"

        if [ $? -ne 0 ]; then
          echo "FAILED FASTgres eval on split: $j_int, seed: $i"
          exit 1
        fi

        echo "Finished iteration."
    done
done