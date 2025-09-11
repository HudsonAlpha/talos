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

        for csq in var_data['transcript_consequences']:
            if 'consequence' not in csq:
                continue

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

        return mane_consequences, mane_hgvsps

def robokevin_check_result(library, chrom, pos, ref, alt):
    url=f"https://murphy.haib.org/api/v1/result-check/{library}/{chrom}/{pos}/{ref}/{alt}"
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        result = json.loads(response.json())['is_returned']
        return result
    else:
        return None
    
def map_talos_result(result_dict):
    mapped_data = []
    for library in result_dict['results']:
        for variant in result_dict['results'][library]['variants']:
            row = {
                'library': variant['sample'],
                'chrom': variant['var_data']['coordinates']['chrom'],
                'pos': variant['var_data']['coordinates']['pos'],
                'ref': variant['var_data']['coordinates']['ref'],
                'alt': variant['var_data']['coordinates']['alt'],
                'ensg': variant['gene'],
                'support_categories': ",".join(variant['var_data']['support_categories']),
                'first_tagged': variant['first_tagged'],
                'flags': ",".join(variant['flags']),
                'disease_model': variant['reasons'],
                'is_returned': robokevin_check_result(variant['sample'], variant['var_data']['coordinates']['chrom'], variant['var_data']['coordinates']['pos'], variant['var_data']['coordinates']['ref'], variant['var_data']['coordinates']['alt']),
                'clinvar_stars': variant['clinvar_stars'],
                'clinvar_increase': variant['clinvar_increase'],
                'found_in_current_run': variant['found_in_current_run'],
                'family': variant['family'],
                'evidence_last_updated': variant['evidence_last_updated'],
                'mane_consequences': parse_csq(variant['var_data'])[0],
                'mane_hgvsps': parse_csq(variant['var_data'])[1],
            }
            mapped_data.append(row)

    return mapped_data, mapped_data[0].keys()
        




with open(args.input) as input_data:
    talos_data = json.load(input_data)
    

with open(args.output, "w") as output_handle:
    mapped_data, headers = map_talos_result(talos_data)
    writer = csv.DictWriter(output_handle, fieldnames=headers, dialect=csv.excel_tab)
    writer.writeheader()
    writer.writerows(mapped_data) 

    
