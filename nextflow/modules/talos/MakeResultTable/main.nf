process MakeResultTable {
    container params.container

    // flag results in the result JSON which are good phenotypic matches
    publishDir params.output_dir, mode: 'copy'

    input:
        path talos_result_json

    output:
        path "${talos_result_json.baseName}.tsv"

    """
    MakeResultTable \
        --input ${talos_result_json} \
        --output ${talos_result_json.baseName}.tsv
    """
}
