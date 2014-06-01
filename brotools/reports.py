"""Misc functions mostly useful for one off multiprocessing tasks"""

import os
import merge
import gzip
import logging
import multiprocessing
import sys
from .graphs import graphs
from .records import BroRecordWindow, bro_records

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
        except:
            err = "Ignoring {0}: formatting errors in the log".format(dest)
            log.error(err)
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

# Helpers for extracting chaing referrers from bro data

def _find_helper(job):
    return referrer_chains(*job)

# Ugly hack to create a process safe iterator by attaching it to the current
# module, so that all processes can track process
def find_referrers(paths, workers=8, time=.5, chain_length=3, domains=True, verbose=False, veryverbose=False):
    num_jobs = len(paths)
    p = multiprocessing.Pool(workers)
    referrers = p.map(_find_helper, ((p, time, chain_length, domains, num_jobs) for p in paths))
    return referrers


def main_domain(domain):
    """Returns the domain just below the TLD.  F example.org, would return
    'example'

    Args:
        domain -- A fully qualified domain name, such as www.example.org

    Returns:
        A string, containing just the domain just below the top level domain.
    """
    return ".".join(domain.split(".")[-2:])


def referrer_chains(path, time=.5, chain_length=3, domains=True, total=None):
    """Takes a file handle to a stream of bro records and looks for redirects
    of a given length.  It then pickles the resulting dictionary describing
    the records to a handle at out handle

    Args:
        path -- the path to gzipped bro data on disk

    Keyword Args:
        time         -- the maximum number of seconds that can appear between
                        redirections to be counted as a valid chain
        chain_length -- the length of chains to try and extract from the given
                        bro data
        domains      -- whether the all domains must be unique in a referrer
                        chain in order for the chain to be included / saved
        total        -- the total number of jobs being processed

    Return:
        A dictionary of referrer chains extracted from data.
    """
    if total:
        sys.modules[__name__].counter.value += 1
        job_string = "({0}/{1}) {2}".format(sys.modules[__name__].counter.value, total, path)
    else:
        job_string = path
    log = logging.getLogger("brorecords")
    log.info("{0}: Begining parsing".format(job_string))

    collection = BroRecordWindow(time=time, steps=chain_length)
    redirects = {}

    for record in bro_records(gzip.open(path, 'r')):

        # filter out some types of records that we don't care about at all.
        # Below just grabs out the first 9 letters of the mime type, which is
        # enough to know if its text/plain or text/html of any encoding
        record_type = record.content_type[0:9]
        if record_type not in ('text/plai', 'text/html') and record.status_code != "301":
            continue

        collection.append(record)

        record_referrers = collection.referrer(record)
        if record_referrers:

            # If the "domains" flag is passed, check and make sure that all
            # referrers come from unique domains / hosts, and if not, ignore
            if domains and len(set([main_domain(r.host) for r in record_referrers])) != chain_length:
                continue

            root_referrer = record_referrers[0]
            root_referrer_url = root_referrer.host + root_referrer.uri

            intermediate_referrer = record_referrers[1]
            intermediate_referrer_url = intermediate_referrer.host + intermediate_referrer.uri

            combined_root_referrers = root_referrer_url + "::" + intermediate_referrer_url

            bad_site = record_referrers[2]
            bad_site_url = bad_site.host + bad_site.uri

            if combined_root_referrers not in redirects:
                redirects[combined_root_referrers] = ([], root_referrer_url, intermediate_referrer_url, [])

            if bad_site_url not in redirects[combined_root_referrers][3]:
                # Before adding the URL and BroRecord to the collection of
                # directed to urls, check and make sure that these are unique
                # domains too (if flag is passed)
                if (not domains or
                        main_domain(bad_site.host) not in redirects[combined_root_referrers][0]):
                    redirects[combined_root_referrers][3].append(bad_site_url)
                    redirects[combined_root_referrers][0].append(main_domain(bad_site.host))

    log.info("{0}: Found {1} chains".format(job_string, len(redirects)))
    return redirects

def print_report(referrer_chains, output_h, min_chain_nodes=2):
    """Pretty formats a set of referrer chain records

    Args:
        referrer_chains -- a dictionary describing a set of rererrer chains, in
                           the format returned by `referrer_chains`
        output_h        -- a file handle to write the report to

    Keyword Args:
        min_chain_nodes -- The minimum number of redirections in the referrer
                            chains to an end destination to be included in the
                            formatting
    """
    keys = referrer_chains.keys()
    keys.sort()

    for k in keys:
        domains, first_url, second_url, third_level_urls = referrer_chains[k]
        if len(third_level_urls) >= min_chain_nodes:
            output_h.write(first_url + "\n")
            output_h.write("\t -> " + second_url + "\n")
            for url in third_level_urls:
                output_h.write("\t\t -> " + url + "\n")
            output_h.write("---\n\n")
