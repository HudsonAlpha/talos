#!/bin/bash
#SBATCH -J hail-repart
#SBATCH -N 1
#SBATCH -c 128
#SBATCH --mem=1264G
#SBATCH -t 48:00:00
#SBATCH -p highmem
#SBATCH -o hail_repart.log

module load cluster/singularity
singularity exec -B /cluster -B /scratch /cluster/lab/gcooper/hg38/talos/images/talos-build_7.5.2.sif python repartition.py
