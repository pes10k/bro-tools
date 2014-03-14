"""Classes and iterators useful for processing collections of bro data"""

from collections import namedtuple

def bro_chains(handle, time=.5, record_filter=None):
    """A generator function that yields completed BroRecordChain objects.

    Args:
        handle -- a file handle like object to read lines of bro data off of.

    Keyword Args:
        record_filter -- an optional function that, if provided, should take two
                         arguments of bro records, and should provide True if
                         they should be included in the same chain or not.  Note
                         that this is in addition to the filtering / matching
                         already performed by the BroRecordChain.add_record
                         function
        time          -- the maximum amount of time that can elapse between two
                         records and still have them be considered in the same
                         chain

    Return:
        An iterator returns BroRecordChain objects
    """
    chains = []
    latest_record_time = 0
    for r in bro_records(handle):

        latest_record_time = max(latest_record_time, r.ts)
        early_record_cutoff = latest_record_time - time

        # First see if there are any chains that this record can be attached
        # to in the current collection
        altered_chain = None
        first_good_chain_index = None
        index = 0
        for c in chains:
            if not altered_chain and c.add_record(r, record_filter):
                altered_chain = c

            # In order to avoid having to double walk the set of chains,
            # we check here to find the first chain in the set that happened
            # recently enough to not need to evicted
            if first_good_chain_index is None and c.tail().ts > early_record_cutoff:
                first_good_chain_index = index

            index += 1
            if altered_chain and first_good_chain_index is not None:
                break

        # If we couldn't attach the current record to an existing chain,
        # create a new chain with this record as the root
        if not altered_chain:
            chains.append(BroRecordChain(r))
        # Otherwise, move the updated chain to the end of the chain list,
        # so that the last updated record is always the first in the
        # list of chains
        else:
            chains.remove(altered_chain)
            chains.append(altered_chain)

        # Now, return any completed chains that have been found, which
        # just means any any chains that have their last element more than
        # the given cut off time ago.
        if first_good_chain_index == None:
            for c in chains:
                yield c
            chains = []
        else:
            for c in chains[:first_good_chain_index]:
                yield c
            chains = chains[first_good_chain_index:]

    # Once we've finished processing all bro records in the set,
    # there will likely still be some chains that haven't been completed.
    # Return them all here
    for c in chains:
        yield c


def bro_records(handle):
    """A generator function for iterating over a a collection of bro records.
    The iterator returns BroRecord objects (named tuples) for each record
    in the given file

    Args:
        handle -- a file handle like object to read lines of bro data off of.

    Return:
        An iterator returning BroRecord objects
    """
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


class BroRecordChain(object):
    """Keeps track of a chain of BroRecord items, based on ip, referrer and
    timestamp"""

    def __init__(self, record):
        self.ip = record.id_orig_h
        self.tail_url = record.host + record.uri
        self.records = [record]

    def __str__(self):
        output = ""
        count = 0
        for r in self.records:
            output += (" -> " * count) + r.host + r.host + "\n"
            count += 1
        return output

    def head(self):
        return self.records[0]

    def tail(self):
        return self.records[-1]

    def len(self):
        return len(self.records)

    def domain_position(self, domain):
        """Returns the position of a given domain (example.org) in the current
        record chain.

        Args:
            domain -- an internet domain in the format "example.org"

        Return:
            The integer position of the bro record describing a request to
            the given domain in this chain, or None if no such record exists.
        """
        index = 0
        for r in self.records:
            if r.host == domain:
                return index
            index += 1
        return None

    def add_record(self, record, record_filter=None):
        """Attempts to add a given BroRecord to the current referrer chain.
        This method checks to see if it makes sense to add the given record
        to the referrer chain (by checking the ip of the requester, whether
        the referrer of the given record matches the domain + path of the
        last record in this chain, and if the record comes after the
        last the record in the chain)

        Args:
            record -- a BroRecord element to try and add to a referrer chain

        Keyword Args:
            record_filter -- an optional function that, if provided, should take
                             two arguments of bro records, and should provide
                             True if the record should be included

        Return:
            True if the given record was added to the chain, otherwise False
        """
        if record.id_orig_h != self.id:
            return False

        tail_record = self.tail()

        if record.ts < tail_record.ts:
            return False

        referrer_url = record.referrer
        if referrer_url[0:7] == "http://":
            referrer_url = referrer_url[7:]
        elif referrer_url[0:8] == "https://":
            referrer_url = referrer_url[8:]

        if self.tail_url != referrer_url:
            return False

        if record_filter and not record_filter(tail_record, record):
            return False

        self.tail_url = referrer_url
        self.records.append(record)
        return True


class BroRecordWindow(object):
    """Keep track of a sliding window of BroRecord objects, and don't keep more
    than a given amount (defined by a time range) in memory at a time"""

    def __init__(self, time=.5):
        # A collection of BroRecords that all occurred less than the given
        # amount of time before the most recent one (in order oldest to newest)
        self._collection = []

        # Window size of bro records to keep in memory
        self._time = time

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
        window_low_bound = self._time

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
