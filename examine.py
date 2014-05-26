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
parser.add_argument("--output", "-o",
                    help="Path to write a report of found domains ")
args = parser.parse_args()

target_domain = "amazon.com"

with open(args.input, 'r') as h:
    for chains_in_file in pickle.load(h):
        for chain in chains_in_file:
            should_print_chain = False
            for record in chain:
                if target_domain in chain.host and "tag=" in chain.uri:
                    should_print_chain = True
                    break
            print chain
            # first_record = chain.head()
            # last_record = chain.tail()
            # if target_domain in first_record.host:
            #     continue

            # if "tag=" not in last_record.uri:
            #     continue

            # if target_domain not in last_record.host:
            #     continue

            # print chain
