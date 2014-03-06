from bro import bro_records, BroRecordWindow
import argparse
import sys

parser = argparse.ArgumentParser(description='Read bro data and look for redirecting domains.')
parser.add_argument('--input', '-i', default=None, type=str,
                    help='Path to bro data to read from. Defaults to stdin.')
parser.add_argument('--time', '-t', type=float, default=.5,
                    help='The time interval between a site being visited and redirecting to be considered an automatic redirect.')
parser.add_argument('--verbose', '-v', action='store_true', default=False, help="Print extra debugging information.")
args = parser.parse_args()

input_handle = sys.stdin if not args.input else open(args.input, 'r')

# Keep track of found redirects that we've only found redirecting to one
# item so far.  This will be a list of urls (the key) to a list of destintations
# redirected to (the value)
redirects = {}

collection = BroRecordWindow(time=args.time)

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

    records_referrer = collection.referrer(record)
    if records_referrer:
        referrer_url = records_referrer.host + records_referrer.uri
        record_url = record.host + record.uri
        log("found referrer from {0} -> {1}".format(referrer_url, record_url))

        if referrer_url not in redirects:
            redirects[referrer_url] = []

        if record_url not in redirects[referrer_url]:
            redirects[referrer_url].append(record_url)

            if len(redirects[referrer_url]) > 1:
                log("possible detection at {0}".format(referrer_url))

for url, values in redirects.items():
    if len(values) > 1:
        print url
        print values
        print "---"
