#!/usr/bin/env python
"""Merges graphs together, to stitch together graphs that were
split apart across different bro logs.

This script will create two new files for every input file, a '.changed' and a
'.unchanged' version, the former containg bro graphs that contain at least the
records from at least two graphs in the input file, and the second containg
graphs that are unchanged from the merge operation."""

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import os
import brotools.reports
import brotools.records
import datetime
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

input_paths = args.inputs
try:
    in_paths = [p for p in input_paths.split("\n")]
except AttributeError: # Catch if we're calling split on a list of files
    in_paths = input_paths

# Next, do some simple trimming to make sure we deal with common issues,
# like a trailing empty string in a list, etc.
input_files = [p.strip() for p in in_paths if len(p.strip()) > 0]
input_files.sort()

written_files = set()
parsed_files = []
removed_files = []
for path, graph, is_changed in merge(input_files, args.time):

    if path not in parsed_files:
        parsed_files.append(path)
        now = datetime.datetime.now()
        out.write("{0}: Moving onto file {1}\n".format(str(now), path))

    # If we've been passed the `light` flag (meaning we should try and
    # limit use of the filesystem), delete each source file once we're
    # done processing it (ie its not the current one we're reading from)
    if args.light and path != parsed_files[-1] and path not in removed_files:
        try:
            removed_files.append(path)
            os.remove(path)
        except OSError:
            pass

    if is_changed:
        dst_path = path + ".changed"
        written_files.add(dst_path)
        with open(dst_path, 'a') as h:
            pickle.dump(graph, h)
    else:
        dst_path = path + ".unchanged"
        written_files.add(dst_path)
        with open(dst_path, 'a') as h:
            pickle.dump(graph, h)
    if path not in parsed_files:
        parsed_files.append(parsed_files)

if args.light:
    for prev_path in parsed_files:
        try:
            os.remove(prev_path)
        except OSError:
            pass

out.write("Finished writing {0} files\n".format(len(written_files)))
