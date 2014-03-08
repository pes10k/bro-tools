import sys
import argparse
import bro
import logging

parser = argparse.ArgumentParser(description='Read bro data and look for redirecting domains.')
parser.add_argument('--workers', '-w', default=8, type=int,
                    help="Number of worker processe to use for processing bro data")
parser.add_argument('--inputs', '-i', nargs='*',
                    help='A list of gzip files to parse bro data from. If not provided, reads a list of files from stdin')
parser.add_argument('--time', '-t', type=float, default=.5,
                    help='The time interval between a site being visited and redirecting to be considered an automatic redirect.')
parser.add_argument('--domains', '-d', action='store_true', default=False,
                    help="If set, only referrer chains consiting of unique domains will be recorded.")
parser.add_argument('--steps', '-s', type=int, default=3,
                    help="Number of steps in a chain to look for in the referrer chains. Defaults to 3")
parser.add_argument('--output', '-o', default=None,
                    help="File to write general report to. Defaults to stdout.")
parser.add_argument('--verbose', '-v', action='store_true',
                    help="Prints some debugging / feedback information to the console")
parser.add_argument('--veryverbose', '-vv', action='store_true',
                    help="Prints lots of debugging / feedback information to the console")

parser.add_argument
args = parser.parse_args()

input_files = args.inputs.split(" ") if args.inputs else sys.stdin.read().strip().split("\n")
logger = logging.getLogger("bro-records")

if args.veryverbose:
    logger.setLevel(logging.DEBUG)
elif args.verbose:
    logger.setLevel(logging.INFO)
else:
    logging.setLevel(logging.ERROR)

logger.info("Finished parsing records, generating master report")
referrers = bro.find_referrers(input_files, args.workers)
print referrers

# Now we need to merge all the referrer records together into a final report
# and print it out to disk at the given location

master_referrers = {}
for r in referrers:
    for combined_url, (domains, first_url, second_url, third_level_urls) in r.items():
        if combined_url not in master_referrers:
            master_referrers[combined_url] = (domains, first_url, second_url, third_level_urls)
        else:
            for d in domains:
                if d not in master_referrers[combined_url][0]:
                    master_referrers[combined_url][0].append(d)
            for tl_url in third_level_urls:
                if tl_url not in master_referrers[combined_url][3]:
                    master_referrers[combined_url][3].append(tl_url)

output_h = open(args.output, 'w') if args.output else sys.stdout
bro.print_report(master_referrers, output_h)

if args.output:
    logger.info("Finished writing report to {0}".format(args.output))
