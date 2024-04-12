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
DATASET=/pscratch/sd/c/chongyef/cs267_hw3_2023/my_datasets/human-chr14-synthetic.txt
# Output file
OUTPUT_FILE=intra_timing_51_mers.txt

echo "Tasks, Run, T-init, T-total" > $OUTPUT_FILE

# Function to perform a single run and extract timing information
run_experiment() {
    TASKS=$1
    RUN=$2
    OUTPUT=$(srun -N 1 -n $TASKS ./kmer_hash_51 $DATASET)
    T_INIT=$(echo "$OUTPUT" | grep 'Finished inserting in' | awk '{print $4}')
    T_TOTAL=$(echo "$OUTPUT" | grep 'Assembled in' | awk '{print $3}')
    echo "$TASKS, $RUN, $T_INIT, $T_TOTAL" >> $OUTPUT_FILE
}

# Tasks to test within a single node
TASK_COUNTS=(1 10 20 30 40 50 60 64)

# Number of runs for each task count
RUNS=3

for TASKS in "${TASK_COUNTS[@]}"; do
    for (( RUN=1; RUN<=RUNS; RUN++ )); do
        run_experiment $TASKS $RUN
    done
done
