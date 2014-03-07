from bro import bro_records, BroRecordWindow, main_domain
import argparse
import sys
import os
try:
    import cPickel as pickle
except:
    import pickle

parser = argparse.ArgumentParser(description='Read bro data and look for redirecting domains.')
parser.add_argument('--input', '-i', default=None, type=str,
                    help='Path to bro data to read from. Defaults to stdin.')
parser.add_argument('--time', '-t', type=float, default=.5,
                    help='The time interval between a site being visited and redirecting to be considered an automatic redirect.')
parser.add_argument('--verbose', '-v', action='store_true', default=False,
                    help="Print extra debugging information.")
parser.add_argument('--domains', '-d', action='store_true', default=False,
                    help="If set, only referrer chains consiting of unique domains will be recorded.")
parser.add_argument('--steps', '-s', type=int, default=3,
                    help="Number of steps in a chain to look for in the referrer chains. Defaults to 3")
parser.add_argument('--output', '-o', default=None,
                    help="File to write general report to. Defaults to stdout.")
parser.add_argument('--linenumbers', '-l', action="store_true", default=False,
                    help="Prints a running count of number of lines processed.")
parser.add_argument('--state', '-p', default=None,
                    help="If provided, will be used for serializing and restoring the state of the colleciton between run, to allow merging between multiple logs.")
args = parser.parse_args()

input_handle = sys.stdin if not args.input else open(args.input, 'r')
output_handle = sys.stdout if not args.output else open(args.output, 'w')

# Keep track of found redirects that we've only found redirecting to one
# item so far.  This will be a list of urls (the key) to a list of destintations
# redirected to (the value)
if args.state and os.path.isfile(args.state):
    state_handle = open(args.state, 'r')
    redirects = pickle.load(state_handle)
    state_handle.close()
else:
    redirects = {}

collection = BroRecordWindow(time=args.time, steps=args.steps)

def log(msg):
    if args.verbose:
        print msg

if args.linenumbers:
    line_count = 0

for record in bro_records(input_handle):

    if args.linenumbers:
        line_count += 1
        if line_count % 100 == 0:
            print line_count

    # filter out some types of records that we don't care about at all.
    # Below just grabs out the first 9 letters of the mime type, which is
    # enough to know if its text/plain or text/html of any encoding
    record_type = record.content_type[0:9]
    if record_type not in ('text/plai', 'text/html') and record.status_code != "301":
        continue

    removed = collection.append(record)

    record_referrers = collection.referrer(record)
    if record_referrers:

        # If the "domains" flag is passed, check and make sure that all
        # referrers come from unique domains / hosts, and if not, ignore
        if args.domains and len(set([main_domain(r.host) for r in record_referrers])) != args.steps:
            log("found referrer chain, but didn't have distinct domains")
            continue

        root_referrer = record_referrers[0]
        root_referrer_url = root_referrer.host + root_referrer.uri

        intermediate_referrer = record_referrers[1]
        intermediate_referrer_url = intermediate_referrer.host + intermediate_referrer.uri

        combined_root_referrers = root_referrer_url + "::" + intermediate_referrer_url

        bad_site = record_referrers[2]
        bad_site_url = bad_site.host + bad_site.uri

        if combined_root_referrers not in redirects:
            redirects[combined_root_referrers] = ([], [], (root_referrer_url, intermediate_referrer_url))

        if bad_site_url not in redirects[combined_root_referrers][0]:
            # Before adding the URL and BroRecord to the collection of
            # directed to urls, check and make sure that these are unique
            # domains too (if flag is passed)
            if (not args.domains or
                main_domain(bad_site.host) not in [main_domain(r.host) for r in redirects[combined_root_referrers][1]]):
                redirects[combined_root_referrers][0].append(bad_site_url)
                redirects[combined_root_referrers][1].append(bad_site)

            if len(redirects[combined_root_referrers]) > 1:
                log("possible detection at {0} -> {1} -> {2}".format(root_referrer_url, intermediate_referrer_url, bad_site_url))

# If we have a state path, write the state to disk too
if args.state:
    state_out_handle = open(args.state, 'w')
    pickle.dump(redirects, state_out_handle)
    state_out_handle.close()

for combined_url, (third_level_urls, (first_url, second_url)) in redirects.items():
    if len(third_level_urls) > 1:
        output_handle.write(first_url + "\n")
        output_handle.write("\t -> " + second_url + "\n")
        for url in third_level_urls:
            output_handle.write("\t\t -> " + url + "\n")
        output_handle.write("---\n\n")
