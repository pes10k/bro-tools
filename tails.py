"""Takes a pickled colletion of bro log chain data and prints out the URL
of the final url visited in the chain (ie where the user's browser was
directed to in the chain)"""

import sys
from brotools.brocollections import BroRecord, BroRecordChain
import argparse
try:
    import cPickle as pickle
except ImportError:
    import pickle

parser = argparse.ArgumentParser(description='Extracts the tail of each referrer chain from pickled bro log data.')
parser.add_argument('--input', '-i', default=None,
                    help="Path to the pickle data to read bro chains from.  If not provided, pickel data is read from STDIN.")
parser.add_argument('--output', '-o', default=None,
                    help="Path to write the tail URLs to (one per line).  If not provided, then STDOUT is used.")
args = parser.parse_args()

input_handle = open(args.input, 'r') if args.input else sys.stdin
output_handle = open(args.output, 'w') if args.output else sys.stdout

for chain in pickle.load(input_handle):
    output_handle.write(chain.url())
    output_handle.write("\n")
    sys.exit()
