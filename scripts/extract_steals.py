#!/usr/bin/env python
"""Commandline utility for examining pickled collections of BroRecordGraph
objects and looking for instances where cookie stuffing "stole" a
credit from someone who looks like a valid cookie setter."""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import brotools.reports
from stuffing.affiliate import STUFF, SET

try:
    import cPickle as pickle
except ImportError:
    import pickle

parser = brotools.reports.marketing_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--ttl', type=int, default=84600,
                    help="The time, in seconds, that an Amazon set affiliate "
                    "marketing cookie is expected to be valid.  Default is "
                    "one day (84600 seconds)")
parser.add_argument('--secs', type=int, default=3600,
                    help="The minimum time in seconds that must pass between "
                    "a client's requests to the marketers 'add to cart' page "
                    "for those requests to be treated as a seperate checkout")
cli_params = brotools.reports.parse_marketing_cli_args(parser)
count, ins, out, debug, marketers, args = cli_params

# Multi indexed dict, in the following format:
#
# "Marketer name 1": {
#    "client 1 hash": history object,
#    "client 2 hash": history object
# },
# "Marketer name 2": {
#    "client_hash": history object
# },
history_by_client = {}
index = 0
debug("Preparing to start reading {0} pickled data".format(count))
for path, graphs in ins():
    index += 1
    debug("{0}-{1}. Considering {2}".format(index, count, path))
    debug("{0}-{1}. Found {2} graphs".format(index, count, len(graphs)))
    for g in graphs:
        for marketer in marketers:

            # See if we can find a session tracking cookie for this visitor
            # in this graph.  If not, then we know there are no cookie stuffs,
            # checkouts, or other relevant activity in the graph we
            # care about, so we can continue
            hash_key = marketer.session_id_for_graph(g)
            if not hash_key:
                continue

            # First extract the dict for this marketer
            try:
                client_dict = history_by_client[marketer.name()]
            except:
                client_dict = {}
                history_by_client[marketer.name()] = client_dict

            # Next, try to extract a history object for this client
            # out of the dict of clients for the given marketer
            try:
                history = client_dict[hash_key]
            except KeyError:
                history = marketer(g)
                client_dict[hash_key] = history

for marketer_name, histories in history_by_client.items():
    for client_hash, h in histories.items():
        for c in h.checkouts(seconds=args.secs, cookie_ttl=args.ttl):
            set_indexes = []
            stuff_indexes = []
            for i, (r, g, t) in enumerate(c.cookie_history()):
                if t == STUFF:
                    stuff_indexes.append(i)
                elif t == SET:
                    set_indexes.append(i)
            if (len(set_indexes) > 0 and len(stuff_indexes) > 0 and
               stuff_indexes[-1] > set_indexes[-1]):
                pickle.dump(c, out)
