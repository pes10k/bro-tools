"""Finds nodes in collections of HTTP request data that sometimes point to
amazon-cookie-setting domains, and sometimes point elsewhere.  Idea is that
these redirecting domains may be pay-for-play folks."""

import urlparse
import argparse
import re
import sys

try:
    import cPickle as pickle
except ImportError:
    import pickle

parser = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
parser.add_argument('--inputs', '-i', nargs='*',
                    help="A list of pickeled BroRecordGraph records to read " +
                    "from. If not provided, reads a list of files from STDIN.")
parser.add_argument('--output', '-o', default=None,
                    help="File to write general report to. Defaults to STDOUT.")
parser.add_argument('--verbose', '-v', action="store_true",
                    help="If provided, prints out status information to " +
                    "STDOUT.")
parser.add_argument('--cache', '-c', default="/tmp/bro-redirectors",
                    help="If set, a root stub name that intermediate results " +
                    "can be stored at.  So if given /tmp/store, this script " +
                    "could write to /tmp/store.data, /tmp/store.pickle, etc.")
args = parser.parse_args()

is_debug = args.verbose
def debug(msg):
    if is_debug:
        print msg

AMZ_COOKIE_URL = re.compile(r'&?tag=')

input_files_raw = args.inputs if args.inputs else sys.stdin.read().split("\n")
input_files = [f for f in input_files_raw if len(f.strip()) > 0]

# First step is to build a collection of all nodes in all collections
# that point to hosts that redirect to amazon cookie stuffing domains.
redirecting_cache_path = args.cache + ".redirects.pickle"
try:
    with open(redirecting_cache_path, 'r') as h:
        redirecting_domains = pickle.load(h)
        debug("Successfully loaded 'cached redirecting domains' from {0}".format(
            redirecting_cache_path))
except IOError: # If the cache doesn't exist, then build it
    debug("Not able to load 'cached redirecting domains' from {0}. Building now".format(
        redirecting_cache_path))
    # This set will store all domains that refer to an amazon affiliate
    # cookie setting request at least once
    redirecting_domains = set()
    domains_size = 0
    for in_path in input_files:
        try:
            debug(" * Attempting to find redirecting domains in {0}".format(
                in_path))
            with open(in_path, 'r') as h:
                graphs = pickle.load(h)
                for g in graphs:
                    for n in g.leaves():
                        q = urlparse.urlparse(n.url()).query
                        if "amazon.com" in n.host and AMZ_COOKIE_URL.search(q):
                            parent = g.parent_of_node(n)
                            if g.parent_of_node(parent):
                                redirecting_domains.add(parent.host)
                                if len(redirecting_domains) != domains_size:
                                    domains_size = len(redirecting_domains)
                                    debug(" * * Added domain: {0}".format(
                                        parent.host))
                                else:
                                    debug(" * * Skipped existing domain: {0}".format(
                                        parent.host))
        except IOError:
            pass
    with open(redirecting_cache_path, 'w') as cache_h:
        pickle.dump(redirecting_domains, cache_h)
        debug(" * Wrote cached file to {0}".format(redirecting_cache_path))

debug("")

# Next, find all instances of nodes that are part of chains where the referring
# node 1) refers to an amazon url at least once, and 2) refers to a non amazon
# host at least once, and 3) has at least one referrer of its own
chains_cache_path = args.cache + ".chains.pickle"
try:
    with open(chains_cache_path, 'r') as h:
        redirection_chains = pickle.load(h)
        debug("Successfully loaded 'cached redirection chains' from {0}".format(
            chains_cache_path))
except IOError:
    debug("Unable to load 'cached redirection chains' from {0}. Building...".format(
        chains_cache_path))
    # Keys here will be the domains of hosts that sometimes redirect to
    # amazon setting domains.  We special case do not include amazon domains
    # that refer to amazon domains.
    # Values are dictionaries too, with keys being domains directed to, and
    # and values of those sub dictionaries being a list of chains
    redirection_chains = {}
    for in_path in input_files:
        try:
            debug(" * Attempting to find redirection chains in {0}".format(
                in_path))
            with open(in_path, 'r') as h:
                graphs = pickle.load(h)
                for g in graphs:
                    domain_mapping = g.node_domains()
                    for d in redirecting_domains:
                        if d not in domain_mapping or "amazon.com" in d:
                            continue
                        for n in domain_mapping[d]:
                            if n.host not in redirection_chains:
                                redirection_chains[n.host] = {}

                            children = g.children_of_node(n)
                            for cn in children:
                                # No point in including posts redirecting to
                                # the same domain
                                if cn.host == n.host:
                                    continue

                                debug(" * * {0} appears to be suspect".format(
                                    n.host))
                                if cn.host not in redirection_chains[n.host]:
                                    redirection_chains[n.host][cn.host] = []
                                chain = g.chain_from_node(cn)
                                redirection_chains[n.host][cn.host].append(chain)

        except IOError:
            pass
    with open(chains_cache_path, 'w') as cache_h:
        pickle.dump(redirection_chains, cache_h)
        debug(" * Wrote cached redirection chains to {0}".format(chains_cache_path))


output_h = open(args.output, 'w') if args.output else sys.stdout
for source_domain, dest_domains in redirection_chains.items():
    if len(dest_domains) == 1:
        continue
    output_h.write("{0}\n###n".format(source_domain))
    for dest_domain, chains in dest_domains.items():
        for c in chains:
            output_h.write(str(c))
            output_h.write("\n")
    output_h.write("\n\n")
