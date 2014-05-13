import brotools.merge
import brotools.reports
import argparse
import logging
import sys
import os

try:
    import cPickle as pickle
except ImportError:
    import pickle

parser = argparse.ArgumentParser(description='Read bro data and look for redirecting chains.')
parser.add_argument('--workers', '-w', default=8, type=int,
                    help="Number of worker processe to use for processing bro data")
parser.add_argument('--workpath', '-p', default="/tmp", type=str,
                    help="A path on disk to write intermediate work files to.")
parser.add_argument('--lite', '-l', action="store_true",
                    help="If true, merged files won't be saved, and will be deleted from disk right after they are used.")
parser.add_argument('--inputs', '-i', nargs='*',
                    help='A list of gzip files to parse bro data from. If not provided, reads a list of files from stdin')
parser.add_argument('--time', '-t', type=float, default=.5,
                    help='The time interval between a site being visited and redirecting to be considered an automatic redirect.')
parser.add_argument('--steps', '-s', type=int, default=3,
                    help="Minimum of steps in a chain to look for in the referrer chains. Defaults to 3")
parser.add_argument('--pickle', default=None, help="If set, a path to write out a pickled version of all found bro chains to.")
parser.add_argument('--output', '-o', default=None,
                    help="File to write general report to. Defaults to stdout.")
parser.add_argument('--verbose', '-v', action='store_true',
                    help="Prints some debugging / feedback information to the console")
parser.add_argument('--veryverbose', '-vv', action='store_true',
                    help="Prints lots of debugging / feedback information to the console")
args = parser.parse_args()

input_files = args.inputs.replace("\n", " ").split(" ") if args.inputs else sys.stdin.read().strip().split("\n")

logging.basicConfig()
logger = logging.getLogger("brorecords")

if args.veryverbose:
    logger.setLevel(logging.DEBUG)
elif args.verbose:
    logger.setLevel(logging.INFO)
else:
    logger.setLevel(logging.ERROR)

paths = [(k, os.path.join(args.workpath, v)) for k, v in brotools.merge.group_records(input_files)]
relevant_chains = brotools.reports.find_chains(paths, workers=args.workers, time=args.time, min_length=args.steps, lite=args.lite)

if args.pickle:
    with open(args.pickle, 'w') as h:
        if args.output:
            logger.info("Writing pickled version of chains to {0}".format(args.pickle))
        pickle.dump(relevant_chains, h)
        if args.output:
            logger.info("Finished pickling chains to {0}".format(args.pickle))

output_h = open(args.output, 'w') if args.output else sys.stdout

for chains in relevant_chains:
    for c in chains:
        output_h.write(str(c))
        output_h.write("------\n\n")

        if args.veryverbose:
            for record in c:
                logger.debug(record)
            logger.debug("------\n")

if args.output:
    logger.info("Finished writing report to {0}".format(args.output))
