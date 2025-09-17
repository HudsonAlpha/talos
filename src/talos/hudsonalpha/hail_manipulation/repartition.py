import hail as hl

# Init Spark with lots of cores, big memory
hl.init(
    master="local[128]",  # match SLURM --cpus-per-task
    tmp_dir="file:///scratch/lab/gcooper/hail_tmp",  # scratch space for shuffles
    min_block_size=128,  # MB, optional tuning for shuffle parallelism
    spark_conf={
        'spark.driver.memory': '200g',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
	'spark.local.dir': '/scratch/lab/gcooper/spark'
    }
)


mt = hl.read_matrix_table("/scratch/lab/gcooper/combined_illumina.mt/")

# Repartition target: ~5000 partitions
# (361,685,595 rows / 5000 = ~72k rows/partition, very healthy)
target_partitions = 5000
mt = mt.repartition(target_partitions, shuffle=True)

# Write checkpoint to scratch (fast VAST storage)
mt = mt.checkpoint("/scratch/lab/gcooper/combined_illumina_repartitioned.mt", overwrite=True)

print(f"Repartitioned MT: {mt.count_rows()} rows, {mt.n_partitions()} partitions")
