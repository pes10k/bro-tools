"""Merges graphs together, to stitch together graphs that were
split apart across different bro logs.

This script will create two new files for every input file, a '.changed' and a
'.unchanged' version, the former containg bro graphs that contain at least the
records from at least two graphs in the input file, and the second containg
graphs that are unchanged from the merge operation."""

import sys
import os
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
parser.add_argument('--light', '-l', action="store_true",
                    help="If this argument is passed, the input files will " +
                    "be deleted after the merge operation.")
count, ins, out, debug, args = brotools.reports.parse_default_cli_args(parser)

debug("Preparing to read {0} collections of graphs".format(count))

counts = {
    "in" : 0,
    "out" : 0
}

prev_path = None
for path, graph, is_changed, state in merge(ins(), args.time, state=True):
    removed_dirty_path = False
    if args.light and prev_path and prev_path != path:
        try:
            os.remove(prev_path)
        except OSError:
            pass
        removed_dirty_path = True


    counts['out'] += 1
    counts['in'] = state['count']
    if is_changed:
        with open(path + ".changed", 'a') as h:
            pickle.dump(graph, h)
    else:
        with open(path + ".unchanged", 'a') as h:
            pickle.dump(graph, h)
    if not removed_dirty_path:
        prev_path = path

if args.light and prev_path and prev_path != path:
    try:
        os.remove(prev_path)
    except OSError:
        pass

out.write("""Changed: {}
Unchanged: {}
Merges: {}
Count: {}\n""".format(state['# changed'], state['# unchanged'], state['merges'], state['count']))
out.write("Found graphs: {0}\n".format(counts['in']))
out.write("Written graphs: {0}\n".format(counts['out']))
