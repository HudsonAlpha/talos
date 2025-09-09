process RunHailFiltering {
    memory '512 GB'
    cpus 64
    container params.container

    
    // runs the hail small-variant filtering
    publishDir params.output_dir, mode: 'copy'

    input:
        path mt
        path panelapp_data
        path pedigree
        path clinvar
        path talos_config

    output:
        tuple \
            path("${params.cohort}_small_variants_labelled.vcf.bgz"), \
            path("${params.cohort}_small_variants_labelled.vcf.bgz.tbi")

    // untar the ClinvArbitration data directory. Happy to keep doing this, it's much easier to distribute tar'd
    """
       export SPARK_DRIVER_MEMORY='400g'
       export SPARK_LOCAL_DIRS='/scratch/lab/gcooper/spark/'
       export SPARK_SERIALIZER='org.apache.spark.serializer.KryoSerializer'
       export SPARK_SQL_SHUFFLE_PARTITIONS='2000'

    export TALOS_CONFIG=${talos_config}

    tar --no-same-owner -zxf ${clinvar}

    RunHailFiltering \
        --input ${mt} \
        --panelapp ${panelapp_data} \
        --pedigree ${pedigree} \
        --output ${params.cohort}_small_variants_labelled.vcf.bgz \
        --clinvar clinvarbitration_data/clinvar_decisions.ht \
        --pm5 clinvarbitration_data/clinvar_decisions.pm5.ht \
        --checkpoint checkpoint
    """
}
