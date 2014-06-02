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

AMZ_COOKIE_URL = re.compile(r'(?:&|\?)tag=')

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
args = parser.parse_args()

input_files = args.inputs if args.inputs else sys.stdin.read().split("\n")
output_h = open(args.output, 'w') if args.output else sys.stdout

for pickle_path in input_files:
    with open(pickle_path, 'r') as h:
        graphs = pickle.load(h)
        for g in graphs:
            for n in g.leaves():
                if AMZ_COOKIE_URL.match(n.url()):
                    chain = g.chain_from_node(n)
                    output_h.write(str(chain))
                    output_h.write("\n-------\n\n")
