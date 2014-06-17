"""Misc functions mostly useful for one off multiprocessing tasks"""

import os
import merge
import logging
import multiprocessing
import sys
import argparse
from .graphs import graphs

try:
    import cPickle as pickle
except ImportError:
    import pickle

sys.modules[__name__].counter = multiprocessing.Value('i', 0)

# Helpers for extracting chains from bro data

def _find_graphs_helper(args):

    def _filter(r):
        short_content_type = r.content_type[:9]
        return (short_content_type in ('text/plai', 'text/html') or
                r.status_code == "301")

    merge_rules, time, min_length, lite = args
    files, dest = merge_rules
    log = logging.getLogger("brorecords")

    # First check and see if there is already a pickled version of
    # extracted graphs from this given work set.  If so, we can quick out
    # here.  For simplicty sake, we just append .pickle to the name of the
    # path for the combined bro records
    picked_path = "{0}.pickle".format(dest)
    if os.path.isfile(picked_path):
        log.info("Found picked records already at {0}".format(picked_path))
        return picked_path

    log.info("Merging {0} files into {1}".format(len(files), dest))

    if not merge.merge(files, dest):
        return []

    log.info("{0}: Begining parsing".format(dest))

    with open(dest, 'r') as h:
        intersting_graphs = []
        try:
            for g in graphs(h, time=time, record_filter=_filter):
                if len(g) < min_length:
                    continue
                intersting_graphs.append(g)
        except Exception, e:
            err = "Ignoring {0}: formatting errors in the log".format(dest)
            log.error(err)
            raise e
            return []

    log.info("{0}: Found {1} graphs".format(dest, len(intersting_graphs)))

    if lite:
        os.remove(dest)

    # Now write the resulting collection of graphs to disk as a pickled
    # collection.
    with open(picked_path, 'w') as ph:
        pickle.dump(intersting_graphs, ph)
    return picked_path

def find_graphs(file_sets, workers=8, time=.5, min_length=3, lite=True):
    p = multiprocessing.Pool(workers)
    work_sets = ((f, time, min_length, lite) for f in file_sets)
    graphs = p.map(_find_graphs_helper, work_sets)
    return graphs

def default_cli_parser(description=None):
    """Returns a default command line parser argument, to reduce the number of
    times we need to have initilize the same parser.

    Args:
        description -- Description of the program we're parsing arguments for

    Return:
        An initilized `argparse.ArgumentParser` instance with defaults
        for reading and writing IO.
    """
    p = argparse.ArgumentParser(description=description)
    p.add_argument('--inputs', '-i', nargs='*',
                   help="A list of pickeled BroRecordGraph records to read " +
                   "from. If not provided, reads a list of files from STDIN.")
    p.add_argument('--output', '-o', default=None,
                   help="File to write general report to. Defaults to STDOUT.")
    p.add_argument('--verbose', '-v', action="store_true",
                   help="If provided, prints out status information to " +
                   "STDOUT.")
    return p

def parse_default_cli_args(parser):
    """Parses the arguments passed with the commandline, and returns objects
    instantiated to handle the above common case options.

    Args:
        parser -- An `argparse.ArgumentParser` instance, likely returned
                  from the `default_cli_parser` function in this module

    Return:
        A tuple with four values,
            - an iterator for reading unpickled data from
            - a file handle for writing output to
            - a function that should be used for writing error messages
            - the `Namespace` object returned from calling `parser.parse_args()`
    """
    args = parser.parse_args()

    inputs = unpickled_inputs(args.inputs or sys.stdin.read())
    output_h = open(args.output, 'w') if args.output else sys.stdout

    is_debug = args.verbose
    def debug(msg):
        if is_debug:
            print msg

    return inputs, output_h, debug, args

def unpickled_inputs(paths):
    """Returns an iterator for reading pickled files off disk.

    Args:
        paths -- a list of paths to pickled objects on disk, or a single string
                 that contains multiple lines of text, each with a single file
                 name read in (such as read from STDIN).

    Returns:
        An iterator that returns two values, the first being the path
        on disk, as a string, that was unpickled, and the second being the
        object that was unpickled.
    """
    log = logging.getLogger("brorecords")

    # First try assuming we've gotten a single string of file paths, and if
    # that doesn't seem right, assume we've gotten a list of file paths
    try:
        in_paths = (p for p in paths.split("\n"))
    except AttributeError: # Catch if we're calling split on a list of files
        in_paths = paths

    # Next, do some simple trimming to make sure we deal with common issues,
    # like a trailing empty string in a list, etc.
    processed_in_paths = (p.strip() for p in in_paths if len(p.strip()) > 0)
    for p in processed_in_paths:
        with open(p, 'r') as h:
            try:
                yield p, pickle.load(h)
            except:
                log.info(" * Pickle error, skipping: {0}".format(p))
                pass

