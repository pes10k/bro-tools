"""Merges graphs together, to stitch together graphs that were
split apart across different bro logs."""

import sys
import brotools.reports
import brotools.records
from brotools.graphs import merge

try:
    import cPickle as pickle
except:
    import pickle

parser = brotools.reports.default_cli_parser(sys.modules[__name__].__doc__)
parser.add_argument('--time', '-t', type=float, default=10,
                    help="The number of seconds that can pass between " +
                    "requests for them to still be counted in the same graph.")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to read {0} sets of graphs".format(count))

counts = {
    "in" : 0,
    "out" : 0
}

num_graphs = 0
for path, graph, is_changed, state in merge(ins(), args.time, state=True):
    counts['out'] += 1
    counts['in'] = state['count']
    if is_changed:
        with open(path + ".changed", 'a') as h:
            pickle.dump(graph, h)
    else:
        with open(path + ".unchanged", 'a') as h:
            pickle.dump(graph, h)


out.write("""Changed: {}
Unchanged: {}
Merges: {}
Count: {}\n""".format(state['# changed'], state['# unchanged'], state['merges'], state['count']))
out.write("Found graphs: {0}\n".format(counts['in']))
out.write("Written graphs: {0}\n".format(counts['out']))
