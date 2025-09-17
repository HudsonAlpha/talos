import json, argparse, requests, os, sys, csv
import copy
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
argparser = argparse.ArgumentParser(description="Make a result table from a JSON file")
argparser.add_argument("input", help="Path to the talos results json file")


args = argparser.parse_args()

def robokevin_check_result(library, chrom, pos, ref, alt):
    url=f"https://murphy.haib.org/api/v1/result-check/library/{library}/{chrom}/{pos}/{ref}/{alt}"
    response = requests.get(url, verify=False)
    if response.status_code == 200:
        result = response.json()['is_returned']
        return result
    else:
        return None
def cli_main():
    with open(args.input) as input_data:
        talos_data = json.load(input_data)

    talos_variants = 0
    plucked_variants = 0
    plucked_data = copy.deepcopy(talos_data)
    returned_data = copy.deepcopy(talos_data)
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
            res = robokevin_check_result(library, chrom, pos, ref, alt)
            if res is True:
                plucked_data['results'][library]['variants'].remove(variant_object)
                plucked_variants += 1
            else:
                returned_data['results'][library]['variants'].remove(variant_object)
    print(f"Plucked {plucked_variants} variants out of {talos_variants} total variants", file=sys.stderr)
    with open(args.input.replace('.json', '.plucked.json'), 'w') as output_data:
        json.dump(plucked_data, output_data, indent=4)
    with open(args.input.replace('.json', '.returned.json'), 'w') as output_data:
        json.dump(returned_data, output_data, indent=4)