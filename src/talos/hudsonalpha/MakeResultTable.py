import json, argparse, requests, os, sys, csv
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
argparser = argparse.ArgumentParser(description="Make a result table from a JSON file")
argparser.add_argument("--input", help="Path to the talos results json file")
argparser.add_argument("--output", help="Path to the output CSV file")

args = argparser.parse_args()

def parse_csq(var_data):
        """
        Parse CSQ variant string returning:
            - set of "consequences" from MANE transcripts
            - Set of variant effects in p. nomenclature (or c. if no p. is available)

        condense massive cdna annotations, e.g.
        c.4978-2_4978-1insAGGTAAGCTTAGAAATGAGAAAAGACATGCACTTTTCATGTTAATGAAGTGATCTGGCTTCTCTTTCTA
        """
        mane_consequences = set()
        mane_hgvsps = set()
        genes = set()

        for csq in var_data['transcript_consequences']:
            if 'consequence' not in csq:
                continue

            if csq['gene']:
                genes.add(csq['gene'])

            if csq['mane_id']:
                mane_consequences.update(csq['consequence'].split('&'))
                if aa := csq.get('amino_acid_change'):
                    mane_hgvsps.add(f'{csq["ensp"]}: {aa}')
                # TODO (MattWellie) add HGVS c. notation
                # TODO (MattWellie) add HGVS p. notation
                # elif csq['hgvsc']:
                #     hgvsc = csq['hgvsc'].split(':')[1]
                #
                #     # if massive indel base stretches are included, replace with a numerical length
                #     if match := CDNA_SQUASH.search(hgvsc):
                #         hgvsc.replace(match.group('bases'), str(len(match.group('bases'))))
                #
                #     mane_hgvsps.add(hgvsc)

        # simplify the consequence strings
        mane_consequences = ', '.join(_csq.replace('_variant', '').replace('_', ' ') for _csq in mane_consequences)
        mane_hgvsps = ', '.join(mane_hgvsps)
        genes = ', '.join(genes)

        return mane_consequences, mane_hgvsps, genes
    
def map_talos_result(result_dict):
    mapped_data = []
    for library in result_dict['results']:
        for variant in result_dict['results'][library]['variants']:
            row = {
                'library': variant['sample'],
                'proband_genotype': variant['genotypes'].get(variant['sample']),
                'all_genotypes': str(variant['genotypes']),
                'family': variant['family'],
                'gcooper_returned': variant['gcooper_returned'],
                'gcooper_acmg_score': variant['gcooper_acmg_score'],
                'chrom': variant['var_data']['coordinates']['chrom'],
                'pos': variant['var_data']['coordinates']['pos'],
                'ref': variant['var_data']['coordinates']['ref'],
                'alt': variant['var_data']['coordinates']['alt'],
                'moi': variant['reasons'],
                'gene': parse_csq(variant['var_data'])[2],
                'ensg': variant['gene'],
                # phenotype match
                'hpo_gene_matches': ",".join(variant['phenotype_labels']),
                'cohort_forced_panel_matches': ",".join([f'{v} ({k})' for k,v in variant['panels']['forced'].items()]),
                'phenotype_panel_matches': ",".join([f'{v} ({k})' for k,v in variant['panels']['matched'].items()]),
                'support_categories': ",".join(variant['categories'].keys()),
                'gnomad_ac': variant['var_data']['info'].get('gnomad_ac'),
                'gnomad_af': variant['var_data']['info'].get('gnomad_af'),
                'first_tagged': variant['first_tagged'],
                'mane_consequences': parse_csq(variant['var_data'])[0],
                'mane_hgvsps': parse_csq(variant['var_data'])[1],
                'clinvarbitration_significance': variant['var_data']['info']['clinvar_significance'],
                'clinvarbitration_stars': variant['clinvar_stars'],
                'clinvar_increase': variant['clinvar_increase'],
                'pm5': ",".join(variant['var_data']['info'].get('pm5_data', {}).keys()),
                'flags': ",".join(variant['flags']),
                'support_variants': ",".join(variant['support_vars']),
                'found_in_current_run': variant['found_in_current_run'],
                'evidence_last_updated': variant['evidence_last_updated'],
                'cohort_ac': variant['var_data']['info']['ac'],
                'cohort_an': variant['var_data']['info']['an'],
                'cohort_af': variant['var_data']['info']['af'],
                'cohort_ac_hom': variant['var_data']['info']['ac_hom'],
                'cohort_ac_het': variant['var_data']['info']['ac_het'],
                'cohort_ac_hemi': variant['var_data']['info']['ac_hemi'],
                

            }
            mapped_data.append(row)

    return mapped_data, mapped_data[0].keys()
        



def cli_main():
    with open(args.input) as input_data:
        talos_data = json.load(input_data)
        

    with open(args.output, "w") as output_handle:
        mapped_data, headers = map_talos_result(talos_data)
        writer = csv.DictWriter(output_handle, fieldnames=headers, dialect=csv.excel_tab)
        writer.writeheader()
        writer.writerows(mapped_data) 

if __name__ == "__main__":
	cli_main()        
