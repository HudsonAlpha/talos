# Example of how to manually run CreateTalosHTML.py with Singularity
export TALOS_CONFIG=/cluster/lab/gcooper/hg38/talos/filtering_config/config.toml
singularity exec -B /cluster -B /scratch docker://harbor.apps.haib.org/gcooperlab/talos:8.0.2.1 python /cluster/home/jlawlor/talos/src/talos/CreateTalosHTML.py \
        --input /cluster/home/jlawlor/talos/runs/outputs/illumina_september/illumina_september_full_report_2025-09-10_20-07.rkresults.json \
        --panelapp /cluster/home/jlawlor/talos/runs/outputs/illumina_september/illumina_september_panelapp.json \
        --output /cluster/home/jlawlor/talos/runs/outputs/illumina_september/withrk_illumina_september_report.html --ext_ids /cluster/lab/gcooper/hg38/talos/pedigrees/2025-09/talos_id_map.tsv 

