import sys
import argparse

try:
    import cPickle as pickle
except ImportError:
    import pickle

parser = argparse.ArgumentParser(description="Parse a pickled set of brolog " +
                                             "chains.")
parser.add_argument('--input', '-i',
                    help="Path to a pickled collection of chains of brolog " +
                    "records to parse and read.")
args = parser.parse_args()

with open(args.input, 'r') as h:
    for chains_in_file in pickle.load(h):
        for chain in chains_in_file:
            print chain
            sys.exit()
