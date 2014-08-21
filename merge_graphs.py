"""Checks a collection of bro log graphs to measure two values, 1) how many
graph heads have referrer heads, and 2) what hosts each of those referrer heads
are for."""

import sys
import brotools.reports
import brotools.records
from .brotools.graphs import merge_graphs

try:
    import cPickle as pickle
except:
    import pickle

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--time', '-t', type=float, default=10,
                    help="The number of seconds that can pass between " +
                    "requests for them to still be counted in the same graph.")
parser.add_argument('--workpath', '-p', default="/tmp/merged-graphs.pickels",
                    help="The file that merged graphs should be written to.")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to reading {0} sets of graphs".format(count))

dest_handle = open(args.workpath, 'w')

counts = {
    "in" : 0,
    "out" : 0
}

def just_graphs():
    for path, graph in ins():
        counts['in'] += 1
        yield graph

num_graphs = 0
for graph in merge_graphs(just_graphs(), args.time):
    counts['out'] += 1
    pickle.dump(graph, dest_handle)

out.write("Found graphs: {0}\n".format(counts['in']))
out.write("Written graphs: {0}\n".format(counts['out']))
