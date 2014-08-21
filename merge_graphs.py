"""Merges graphs together, to stitch together graphs that were
split apart across different bro logs."""

import sys
import brotools.reports
import brotools.records
from brotools.graphs import merge_graphs

try:
    import cPickle as pickle
except:
    import pickle

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--time', '-t', type=float, default=10,
                    help="The number of seconds that can pass between " +
                    "requests for them to still be counted in the same graph.")
parser.add_argument('--unchaged', '-p', required=True,
                    help="The file where unchanged graphs should be written " +
                    "to.")
parser.add_argument('--changed', '-c', required=True,
                    help="A file that changed graphs should be written to.")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to reading {0} sets of graphs".format(count))

changed_handle = open(args.changed, 'w')
unchanged_handle = open(args.unchaged, 'w')

counts = {
    "in" : 0,
    "out" : 0
}

def just_graphs():
    for path, graph in ins():
        counts['in'] += 1
        yield graph

num_graphs = 0
for graph, is_changed in merge_graphs(just_graphs(), args.time):
    counts['out'] += 1
    if is_changed:
        pickle.dump(graph, changed_handle)
    else:
        pickle.dump(graph, unchanged_handle)

out.write("Found graphs: {0}\n".format(counts['in']))
out.write("Written graphs: {0}\n".format(counts['out']))
