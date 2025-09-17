import json, argparse, requests, os, sys, csv
import copy
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
argparser = argparse.ArgumentParser(description="Make a result table from a JSON file")
argparser.add_argument("--input", help="Path to the talos results json file")
argparser.add_argument("--output", help="Path to the output json file")

args = argparser.parse_args()

def robokevin_check_result(library, chrom, pos, ref, alt):
    url=f"https://murphy.haib.org/api/v1/result-check/library/{library}/{chrom}/{pos}/{ref}/{alt}"
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        return response.json()['is_returned'], response.json().get('acmg_score')
    else:
        return None, None

def cli_main():
    with open(args.input) as input_data:
        talos_data = json.load(input_data)

    talos_variants = 0
    plucked_variants = 0
    for library in talos_data['results']:
        for variant_object in talos_data['results'][library]['variants']:
            library, chrom, pos, ref, alt = (
                variant_object['sample'],
                'chr' + variant_object['var_data']['coordinates']['chrom'],
                variant_object['var_data']['coordinates']['pos'],
                variant_object['var_data']['coordinates']['ref'],
                variant_object['var_data']['coordinates']['alt'],
            )
            talos_variants += 1
            res, acmg_score = robokevin_check_result(library, chrom, pos, ref, alt)
            variant_object['gcooper_returned'] = res
            variant_object['gcooper_acmg_score'] = acmg_score

    with open(args.output, 'w') as output_data:
        json.dump(talos_data, output_data, indent=4)
