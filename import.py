from bro import bro_records, BroRecordWindow
import argparse
import sys
from pprint import pprint

parser = argparse.ArgumentParser(description='Read bro data and look for redirecting domains.')
parser.add_argument('--input', '-i', default=None, type=str,
                    help='Path to bro data to read from. Defaults to stdin.')
parser.add_argument('--time', '-t', type=float, default=.5,
                    help='The time interval between a site being visited and redirecting to be considered an automatic redirect.')
parser.add_argument('--verbose', '-v', action='store_true', default=False,
                    help="Print extra debugging information.")
parser.add_argument('--steps', '-s', type=int, default=3,
                    help="Number of steps in a chain to look for in the referrer chains.")
args = parser.parse_args()

input_handle = sys.stdin if not args.input else open(args.input, 'r')

# Keep track of found redirects that we've only found redirecting to one
# item so far.  This will be a list of urls (the key) to a list of destintations
# redirected to (the value)
redirects = {}

collection = BroRecordWindow(time=args.time, steps=args.steps)

def log(msg):
    if args.verbose:
        print msg

for record in bro_records(input_handle):

    # filter out some types of records that we don't care about at all.
    # Below just grabs out the first 9 letters of the mime type, which is
    # enough to know if its text/plain or text/html of any encoding
    record_type = record.content_type[0:9]
    if record_type not in ('text/plai', 'text/html'):
        continue

    collection.append(record)

    record_referrers = collection.referrer(record)
    if record_referrers:
        root_referrer = record_referrers[0]
        root_referrer_url = root_referrer.host + root_referrer.uri

        intermediate_referrer = record_referrers[1]
        intermediate_referrer_url = intermediate_referrer.host + intermediate_referrer.uri

        combined_root_referrers = root_referrer_url + "::" + intermediate_referrer_url

        bad_site = record_referrers[2]
        bad_site_url = bad_site.host + bad_site.uri

        if combined_root_referrers not in redirects:
            redirects[combined_root_referrers] = []

        if bad_site_url not in redirects[combined_root_referrers]:
            redirects[combined_root_referrers].append(bad_site_url)

            if len(redirects[combined_root_referrers]) > 1:
                log("possible detection at {0} -> {1} -> {2}".format(root_referrer_url, intermediate_referrer_url, bad_site_url))

for url, values in redirects.items():
    if len(values) > 1:
        print url
        print values
        print "---"
