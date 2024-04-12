#!/bin/bash

# SLURM account
ACCOUNT=mp309
# Time limit
TIME_LIMIT=10:00
# Quality of Service
QOS=interactive
# CPU type
CPU_TYPE=cpu
# Dataset path
DATASET=/pscratch/sd/c/chongyef/cs267_hw3_2023/my_datasets/test.txt
# Output file
OUTPUT_FILE=timing_19_mers_60.txt

echo "Node Count, Run, T-init, T-total" > $OUTPUT_FILE

# Function to perform a single run and extract timing information
run_experiment() {
    NODES=$1
    RUN=$2
    OUTPUT=$(srun -N $NODES -n $((NODES * 60)) ./kmer_hash_19 $DATASET)
    T_INIT=$(echo "$OUTPUT" | grep 'Finished inserting in' | awk '{print $4}')
    T_TOTAL=$(echo "$OUTPUT" | grep 'Assembled in' | awk '{print $3}')
    echo "$NODES, $RUN, $T_INIT, $T_TOTAL" >> $OUTPUT_FILE
}

# Nodes to test
NODE_COUNTS=(1 2 4 8)

# Number of runs for each node count
RUNS=3

for NODES in "${NODE_COUNTS[@]}"; do
    for (( RUN=1; RUN<=RUNS; RUN++ )); do
        run_experiment $NODES $RUN
    done
done
