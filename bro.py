import gzip
import logging
from collections import namedtuple
import multiprocessing

def _find_helper(job):
    return referrer_chains(*job)

def find_referrers(paths, workers=8, time=.5, chain_length=3, domains=True):
    p = multiprocessing.Pool(workers)
    referrers = p.map(_find_helper, ((p, time, chain_length, domains) for p in paths))
    return referrers

def main_domain(domain):
    return ".".join(domain.split(".")[-2:])

def bro_records(handle):
    seperator = None
    record_type = None
    field_types = None
    num_fields = 0
    for raw_row in handle:
        row = raw_row[:-1] # Strip off line end
        if not seperator and row[0:10] == "#separator":
            seperator = row[11:].decode('unicode_escape')
        if not record_type and row[0:7] == "#fields":
            record_type = namedtuple('BroRecord', [a.replace(".", "_") for a in row[8:].split(seperator)])
        if not field_types and row[0:6] == "#types":
            field_types = row[7:].split(seperator)
            num_fields = len(field_types)
        elif row[0] != "#":
            row_values = [a if a != "-" else "" for a in row.split(seperator)]
            mapped_values = []
            for i in range(num_fields):
                current_type = field_types[i]
                if current_type == "time":
                    current_value = float(row_values[i])
                elif current_type == "count":
                    current_value = int(row_values[i])
                else:
                    current_value = row_values[i]
                mapped_values.append(current_value)
            yield record_type._make(mapped_values)

def referrer_chains(path, time=.5, chain_length=3, domains=True):
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

    Return:
        A dictionary of referrer chains extracted from data.
    """
    logging.info(" * {0}: Begining parsing".format(path))

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
                logging.debug(" - {0}: found referrer chain, but didn't have distinct domains".format(path))
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

                if len(redirects[combined_root_referrers]) > 1:
                    logging.debug(" - {0}: possible detection at {1} -> {2} -> {3}".format(path, root_referrer_url, intermediate_referrer_url, bad_site_url))
    print redirects
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
        for combined_url, (domains, first_url, second_url, third_level_urls) in referrer_chains[k]:
            if len(third_level_urls) >= min_chain_nodes:
                output_h.write(first_url + "\n")
                output_h.write("\t -> " + second_url + "\n")
                for url in third_level_urls:
                    output_h.write("\t\t -> " + url + "\n")
                output_h.write("---\n\n")

class BroRecordWindow(object):
    """Keep track of a sliding window of BroRecord objects, and don't keep more
    than a given amount (defined by a time range) in memory at a time"""

    def __init__(self, time=.5, steps=1):
        # A collection of BroRecords that all occurred less than the given
        # amount of time before the most recent one (in order oldest to newest)
        self._collection = []

        # Window size of bro records to keep in memory
        self._time = time

        # The number of hops, backwards in the window, we want to trace
        # referrers
        self._steps = steps

    def size(self):
        return len(self._collection)

    def prune(self):
        """Remove all BroRecords that occured more than self.time before the
        most recent BroRecord in the collection.

        Return:
            An int count of the number of objects removed from the collection
        """

        # Simple case that if we have no stored BroRecords, there can't be
        # any to remove
        if len(self._collection) == 0:
            return 0

        removed_count = 0
        most_recent_time = self._collection[-1].ts
        window_low_bound = self._time * self._steps

        while len(self._collection) > 1 and self._collection[0].ts + window_low_bound < most_recent_time:
            self._collection = self._collection[1:]
            removed_count += 1

        return removed_count

    def append(self, record):
        """Adds a BroRecord to the current collection of bro records, and then
        cleans to watched collection to remove old records (records before the)
        the sliding time window.

        Args:
            record -- A BroRecord, created by the bro_records function

        Return:
            The number of records that were removed from the window during garbage collection.
        """
        self._collection.append(record)

        # Most of the time the given record will be later than the last
        # record added (since we keep the collection sorted).  In this common
        # case, just add the new record to the end of the collection.
        # Otherwise, add the record and sort the whole thing
        self._collection.append(record)
        if record.ts > self._collection[-2].ts:
            self._collection.sort(key=lambda x: x.ts)

        return self.prune()

    def referrer(self, record, step=None):
        """Checks to see if the current collection contains a record that could
        be the referrer for the given record.  This is done by checking to
        see if there are any records in the collection that
            a) have a host+uri pair that match the passed records referrer
            b) have a requesting ip address that matches the passed records
               ip address

        Args:
            record -- A BroRecord named tuple

        Keyword Args:
            step -- Not intended to be set by a caller.  Used when trying to do
                    recursive traces through the window of referrers

        Return:
            Either a list of BroRecords, starting with the given record and
            ending with one that links to the given record in self._steps
            number of steps, or None if no such record / chain exists
        """
        if step == None:
            step = self._steps

        for r in self._collection:
            r_path = r.host + r.uri
            referrer_path = record.referrer
            if referrer_path[0:7] == "http://":
                referrer_path = referrer_path[7:]
            elif referrer_path[0:8] == "https://":
                referrer_path = referrer_path[8:]
            if record is not r and record.id_orig_h == r.id_orig_h and referrer_path == r_path:
                if step == 1:
                    return [r]
                else:
                    parent_items = self.referrer(r, step=step - 1)
                    if parent_items:
                        parent_items.append(r)
                        return parent_items
                    else:
                        return None
        return None
