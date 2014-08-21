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
    tmp_path = "{0}.pickles.tmp".format(dest)
    final_path = "{0}.pickles".format(dest)
    if os.path.isfile(final_path):
        log.info("Found picked records already at {0}".format(final_path))
        return final_path

    log.info("Merging {0} files into {1}".format(len(files), dest))

    if not merge.merge(files, dest):
        return None

    log.info("{0}: Begining parsing".format(dest))
    graph_count = 0
    with open(dest, 'r') as source_h, open(tmp_path, 'w') as dest_h:
        try:
            for g in graphs(source_h, time=time, record_filter=_filter):
                graph_count += 0
                if len(g) < min_length:
                    continue
                pickle.dump(g, dest_h)
        except Exception, e:
            err = "Ignoring {0}: formatting errors in the log".format(dest)
            log.error(err)
            raise e
            return None

    log.info("{0}: Found {1} graphs".format(dest, graph_count))

    if lite:
        os.remove(dest)

    # Now write the resulting collection of graphs to disk as a pickled
    # collection.
    os.rename(tmp_path, final_path)
    log.info("{0}: Successfully completed work, wrote to {1}".format(dest, final_path))
    return picked_path

def find_graphs(file_sets, workers=8, time=.5, min_length=3, lite=True):
    p = multiprocessing.Pool(workers, maxtasksperchild=1)
    work_sets = [(f, time, min_length, lite) for f in file_sets]
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

def marketing_cli_parser(description=None):
    """Provides the same extended functionality of the `default_cli_parser`
    function, but also adds additional arguments for which affiliate marketers
    the user would like to include in the operation.

    Args:
        description -- Description of the program we're parsing arguments for

    Return:
        An initilized `argparse.ArgumentParser` instance with defaults
        for reading and writing IO, and for selecting which affiliate marketers
        we'd like to investigate.
    """
    parser = default_cli_parser(description)
    parser.add_argument('--amazon', action="store_true",
                        help="Whether to look for Amazon cookie stuffing. " +
                        "If no marketer is specified, all will be used " +
                        "(Amazon, GoDaddy, etc.)")
    parser.add_argument('--godaddy', action="store_true",
                        help="Whether to look for GoDaddy cookie stuffing. " +
                        "If no marketer is specified, all will be used " +
                        "(Amazon, GoDaddy, etc.)")
    parser.add_argument('--pussycash', action="store_true",
                        help="Whether to look for PussyCash affilate " +
                        "marketing cookie stuffing.")
    parser.add_argument('--sextronics', action="store_true",
                        help="Whether to look for Sextronics affilate " +
                        "marketing cookie stuffing.")
    parser.add_argument('--moreniche', action="store_true",
                        help="Whether to look for MoreNitch affiliate " +
                        "marketing cookie stuffing.")
    return parser

def parse_default_cli_args(parser):
    """Parses the arguments passed with the commandline, and returns objects
    instantiated to handle the above common case options.

    Args:
        parser -- An `argparse.ArgumentParser` instance, likely returned
                  from the `default_cli_parser` function in this module

    Return:
        A tuple with five values,
            - the count of the number of items that are provided to process
            - an iterator for reading unpickled data from
            - a file handle for writing output to
            - a function that should be used for writing error messages
            - the `Namespace` object returned from calling `parser.parse_args()`
    """
    args = parser.parse_args()

    num_inputs, inputs = unpickled_inputs(args.inputs or sys.stdin.read())

    output_h = open(args.output, 'w') if args.output else sys.stdout

    is_debug = args.verbose
    def debug(msg):
        if is_debug:
            print msg

    return num_inputs, inputs, output_h, debug, args

def parse_marketing_cli_args(parser):
    """Parses the arguments passed with the commandline, and returns objects
    instantiated to handle the above common case options.

    Args:
        parser -- An `argparse.ArgumentParser` instance, likely returned
                  from the `marketing_cli_parser` function in this module

    Return:
        A tuple with five values,
            - the count of the number of items that are provided to process
            - an iterator for reading unpickled data from
            - a file handle for writing output to
            - a function that should be used for writing error messages
            - a list of AffiliateHistory subclasses to examine the graphs with
            - the `Namespace` object returned from calling `parser.parse_args()`
    """
    num_inputs, inputs, output_h, debug, args = parse_default_cli_args(parser)

    marketers = []
    any_affiliates = any([args.amazon, args.godaddy, args.pussycash,
                          args.sextronics, args.moreniche])

    if not any_affiliates or args.pussycash:
        import stuffing.pussycash
        marketers += stuffing.pussycash.CLASSES

    if not any_affiliates or args.sextronics:
        import stuffing.sextronics
        marketers += stuffing.sextronics.CLASSES

    if not any_affiliates or args.amazon:
        import stuffing.amazon
        marketers.append(stuffing.amazon.AmazonAffiliateHistory)

    if not any_affiliates or args.godaddy:
        import stuffing.godaddy
        marketers.append(stuffing.godaddy.GodaddyAffiliateHistory)

    if not any_affiliates or args.moreniche:
        import stuffing.moreniche
        marketers += stuffing.moreniche.CLASSES

    return num_inputs, inputs, output_h, debug, marketers, args


def unpickled_inputs(paths):
    """Returns the count of files that will try to be unpickled, along with
    a iterator function that returns the contents of those unpickled files.

    Args:
        paths -- a list of paths to pickled objects on disk

    Returns:
        Two values, first an integer count of the number of values it will
        parse and return, and second, a generator function that returns
        pairs of values, the first being the path on disk, as a string, that was
        unpickled, and the second being the object that was unpickled.
    """
    log = logging.getLogger("brorecords")

    # First try assuming we've gotten a single string of file paths, and if
    # that doesn't seem right, assume we've gotten a list of file paths
    try:
        in_paths = [p for p in paths.split("\n")]
    except AttributeError: # Catch if we're calling split on a list of files
        in_paths = paths

    # Next, do some simple trimming to make sure we deal with common issues,
    # like a trailing empty string in a list, etc.
    processed_in_paths = [p.strip() for p in in_paths if len(p.strip()) > 0]
    processed_in_paths.sort()

    def _unpickled_files():
        for p in processed_in_paths:
            with open(p, 'r') as h:
                while True:
                    try:
                        yield p, pickle.load(h)
                    except EOFError:
                        return
                    except:
                        log.info(" * Pickle error, skipping: {0}".format(p))
                        pass

    return len(processed_in_paths), _unpickled_files


