from connection import bro_records, BroRecordCollection
import argparse
import sys

parser = argparse.ArgumentParser(description='Read bro data and look for redirecting domains.')
parser.add_argument('--input', '-i', default=None, type=str,
                    help='Path to bro data to read from. Defaults to stdin.')
parser.add_argument('--time', '-t', type=float, default=.5,
                    help='The time interval between a site being visited and redirecting to be considered an automatic redirect.')
args = parser.parse_args()

input_handle = sys.stdin if not args.input else open(args.input, 'r')

# Keep track of found redirects that we've only found redirecting to one
# item so far.  This will be a list of urls (the key) to a list of destintations
# redirected to (the value)
redirects = {}

collection = BroRecordCollection(time=args.time)

for record in bro_records(input_handle):

    # filter out some types of records that we don't care about at all.
    # Below just grabs out the first 9 letters of the mime type, which is
    # enough to know if its text/plain or text/html of any encoding
    record_type = record.type[0:9]
    if record_type not in ('text/plai', 'text/html'):
        continue

    collection.append(record)

