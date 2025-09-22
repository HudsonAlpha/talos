import json, argparse, requests, sys
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
argparser = argparse.ArgumentParser(description="Make a result table from a JSON file")
argparser.add_argument("--input", help="Path to the talos results (annotated with gcooper returns) json file")
argparser.add_argument("--output", help="Path to the output VCF file")

args = argparser.parse_args()



def map_talos_result(result_dict):
    required_chroms = set(['chr1','chr2','chr3','chr4','chr5','chr6','chr7','chr8','chr9','chr10','chr11','chr12','chr13','chr14','chr15','chr16','chr17','chr18','chr19','chr20','chr21','chr22','chrX','chrY'])
    seen_chroms = set()
    mapped_data = set()
    skipped_variant_count = 0
    processed_variant_count = 0
    for library in result_dict['results']:
        for variant in result_dict['results'][library]['variants']:
            row = {
                'gcooper_returned': variant['gcooper_returned'],
                'gcooper_acmg_score': variant['gcooper_acmg_score'],
                'chrom': 'chr' + variant['var_data']['coordinates']['chrom'],
                'pos': variant['var_data']['coordinates']['pos'],
                'ref': variant['var_data']['coordinates']['ref'],
                'alt': variant['var_data']['coordinates']['alt'],
                'id': '.',
                'qual': '.',
                'filter': '.',
                'info': '.',
                'format': 'GT:AD',
                'gt': '0/1:10,10',
            }
            processed_variant_count += 1
            if row['gcooper_returned'] and (row['gcooper_acmg_score'] == 'pathogenic' or row['gcooper_acmg_score'] == 'likely-pathogenic'):
                skipped_variant_count += 1
                continue
            mapped_data.add("\t".join([str(row[col]) for col in ['chrom', 'pos', 'id', 'ref', 'alt', 'qual', 'filter', 'info', 'format', 'gt']]) + "\n")
            seen_chroms.add(row['chrom'])
            
        
    missing_chroms = required_chroms - seen_chroms
    print(f"Missing: {missing_chroms}", file=sys.stderr)
    for chrom in missing_chroms:
        row = {
            'chrom': chrom,
            'pos': '1',
            'ref': 'A',
            'alt': 'T',
            'id': '.',
            'qual': '.',
            'filter': '.',
            'info': '.',
            'format': 'GT:AD',
            'gt': '0/1:0,0',
        }
        mapped_data.add("\t".join([str(row[col]) for col in ['chrom', 'pos', 'id', 'ref', 'alt', 'qual', 'filter', 'info', 'format', 'gt']]) + "\n")

    print(f"Skipped {skipped_variant_count} / {processed_variant_count} variants that were returned to GCooper as (likely) pathogenic", file=sys.stderr)
    mapped_data = sorted(mapped_data, key=lambda x: (x.split("\t")[0], int(x.split("\t")[1]))) #sort in a dumb roundabout way
    return mapped_data 

def cli_main():
    with open(args.input) as input_data:
        talos_data = json.load(input_data)
        mapped_data = map_talos_result(talos_data)
    

    with open(args.output, "wt") as vcf_output:
        vcf_output.write("##fileformat=VCFv4.1\n")
        vcf_output.write("##contig=<ID=chr1,length=248956422>\n")
        vcf_output.write("##contig=<ID=chr2,length=242193529>\n")
        vcf_output.write("##contig=<ID=chr3,length=198295559>\n")
        vcf_output.write("##contig=<ID=chr4,length=190214555>\n")
        vcf_output.write("##contig=<ID=chr5,length=181538259>\n")
        vcf_output.write("##contig=<ID=chr6,length=170805979>\n")
        vcf_output.write("##contig=<ID=chr7,length=159345973>\n")
        vcf_output.write("##contig=<ID=chr8,length=145138636>\n")
        vcf_output.write("##contig=<ID=chr9,length=138394717>\n")
        vcf_output.write("##contig=<ID=chr10,length=133797422>\n")
        vcf_output.write("##contig=<ID=chr11,length=135086622>\n")
        vcf_output.write("##contig=<ID=chr12,length=133275309>\n")
        vcf_output.write("##contig=<ID=chr13,length=114364328>\n")
        vcf_output.write("##contig=<ID=chr14,length=107043718>\n")
        vcf_output.write("##contig=<ID=chr15,length=101991189>\n")
        vcf_output.write("##contig=<ID=chr16,length=90338345>\n")
        vcf_output.write("##contig=<ID=chr17,length=83257441>\n")
        vcf_output.write("##contig=<ID=chr18,length=80373285>\n")
        vcf_output.write("##contig=<ID=chr19,length=58617616>\n")
        vcf_output.write("##contig=<ID=chr20,length=64444167>\n")
        vcf_output.write("##contig=<ID=chr21,length=46709983>\n")
        vcf_output.write("##contig=<ID=chr22,length=50818468>\n")
        vcf_output.write("##contig=<ID=chrX,length=156040895>\n")
        vcf_output.write("##contig=<ID=chrY,length=57227415>\n")
        vcf_output.write("##FORMAT=<ID=GT,Number=1,Type=String,Description=\"Genotype\">\n")
        vcf_output.write("##FORMAT=<ID=AD,Number=.,Type=Integer,Description=\"Allelic depths for the ref and alt alleles in the order listed.\">\n")
        vcf_output.write("\t".join(["#CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO", "FORMAT", "filler_sample_id\n"]))
        for row in mapped_data:
            vcf_output.write(row)


if __name__ == "__main__":
    cli_main()