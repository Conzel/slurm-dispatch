#!/bin/bash

# --- Slurm Directives ---
#SBATCH --job-name=my_test_job       # Name of your job
#SBATCH --output=logs/job_%j.out     # Standard output log (%j = job ID)
#SBATCH --error=logs/job_%j.err      # Standard error log (%j = job ID)
#SBATCH --partition=cpu-ferranti           # Partition or queue to submit to (e.g., standard, gpu, debug)
#SBATCH --nodes=1                    # Number of nodes requested
#SBATCH --ntasks=1                   # Number of tasks (usually 1 for simple scripts)
#SBATCH --cpus-per-task=4            # Number of CPU cores per task
#SBATCH --mem=16G                    # Total memory requested (e.g., 16 Gigabytes)
#SBATCH --time=00:00:30              # Maximum time limit (HH:MM:SS)
#SBATCH --mail-type=END,FAIL         # Email notifications (NONE, BEGIN, END, FAIL, ALL)

sleep 10
mkdir results
cd results
echo Hello > test.txt
