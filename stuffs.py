"""Finds all instances of page requests that fit the following conditions:
    * Request to amazon.com
    * That is a leaf node (ie is not the referrer to any requests)
    * includes the affiliate cookie tag (&tag= or ?tag=)
    * is a request collected by the `extract.py` script (so requests for
      html documents only)
"""

import argparse
import re
import sys

try:
    import cPickle as pickle
except ImportError:
    import pickle

parser = argparse.ArgumentParser(description="Find possible cookie stuffing " +
                                 "instances in a pickled collection of " +
                                 "BroRecordGraph objects.")
parser.add_argument('--inputs', '-i', nargs='*',
                    help="A list of gzip files to parse BroRecordGraph data " +
                    "from. If not provided, reads a list of files from STDIN.")
parser.add_argument('--regex', default=None,
                    help="An alternate regular expression to use when " +
                    "looking for possible cookie stuffing instances.")
parser.add_argument('--output', '-o', default=None,
                    help="File to write general report to. Defaults to STDOUT.")
parser.add_argument('--verbose', '-v', action="store_true",
                    help="If provided, prints out status information to " +
                    "STDOUT.")
args = parser.parse_args()

verbose = args.verbose

def debug(msg):
    if verbose:
        print msg

AMZ_COOKIE_URL = re.compile(args.regex or r'www\.amazon\.com.*(?:&|\?)tag=')

input_files = args.inputs if args.inputs else sys.stdin.read().split("\n")
output_h = open(args.output, 'w') if args.output else sys.stdout

for pickle_path in input_files:
    debug("Considering {0}".format(pickle_path))
    with open(pickle_path, 'r') as h:
        debug(" * Unpickled {0}".format(pickle_path))
        graphs = pickle.load(h)
        debug(" * {0} graphs found".format(len(graphs)))
        for g in graphs:
            for n in g.leaves():
                if AMZ_COOKIE_URL.search(n.url()):
                    debug(" * * Found possible url: {0}".format(n.url()))
                    chain = g.chain_from_node(n)
                    output_h.write(str(chain))
                    output_h.write("\n-------\n\n")
