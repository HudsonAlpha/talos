#!/bin/bash
#SBATCH -p highmem
#SBATCH -n 192
#SBATCH --mem=1433G
#SBATCH -J hail_merge
#SBATCH -o merge_illumina.log
#SBATCH -N 1

export SPARK_CORES=192
export SPARK_LOCAL_DIRS=/scratch/lab/gcooper/hail_merge_$SLURM_JOBID
mkdir -p $SPARK_LOCAL_DIRS

module load cluster/singularity
singularity exec -B /cluster -B /scratch /cluster/lab/gcooper/hg38/talos/images/talos-build_7.5.2.sif python combine_mt.py --output /scratch/lab/gcooper/combined_illumina.mt/ --mode intersection /cluster/lab/gcooper/hg38/talos/annotated_data/strelka_batch_4439_20250801.mt/ /cluster/lab/gcooper/hg38/talos/annotated_data/dnascope_merge_229_20250801.mt/
rm -rf $SPARK_LOCAL_DIRS
