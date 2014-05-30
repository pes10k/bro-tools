"""Functions and classes that attempt to represent and order bro records
into linear chains, with a single record leading to the next.  Records
are tied together through IP address, data and referrer header values.
Note that for most cases this is probably not very useful, since browsing
patterns make requests better represented by DAGs (ie one resource
requesting multiple others)."""

from urlparse import urlparse
from .records import bro_records

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

        short_content_type = r.content_type[:9]
        if short_content_type not in ('text/plai', 'text/html') or r.status_code == "301":
            continue

        # A growing timestamp of the most recent record found yet.  Since
        # records are required to be ordered (handled elsewhere), we know
        # that the current record being examined will always be the most recent
        # one / latest one
        latest_record_time = r.ts

        # Since we don't allow a step greater than the passed time parameter
        # between any two records in a chain, we know we're done considering
        # any chains who have their most recent record being more than `time`
        # ago
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

            if altered_chain and first_good_chain_index is not None:
                break

            index += 1

        # If we couldn't attach the current record to an existing chain,
        # create a new chain with this record as the root
        if altered_chain is None:
            chains.append(BroRecordChain(r))
            first_good_chain_index = len(chains) - 1
        # Otherwise, move the updated chain to the end of the chain list,
        # so that the last updated record is always the first in the
        # list of chains
        else:
            chains.remove(altered_chain)
            chains.append(altered_chain)

        # Now, return any completed chains that have been found, which
        # just means any chains that have their last element more than
        # the given cut off time ago.
        if first_good_chain_index is None:
            for completed_c in chains:
                yield c
            chains = []
        else:
            for completed_c in chains[:first_good_chain_index]:
                yield completed_c
            chains = chains[first_good_chain_index:]

    # Once we've finished processing all bro records in the set,
    # there will likely still be some chains that haven't been completed.
    # Return them all here
    for remaining_chain in chains:
        yield remaining_chain

class BroRecordChain(object):
    """Keeps track of a chain of BroRecord items, based on ip, referrer and
    timestamp"""

    def __init__(self, r):
        url = r.host + r.uri
        self.ip = r.id_orig_h
        self.tail_url = url

        # We want to treat the referrer of the first record in the
        # chain if we can capture it.  Otherwise, if the first record
        # has no referrer, we star the chain with the url of the
        # first requested page
        if r.referrer:
            self._pre_url = r.referrer
            parts = urlparse(r.referrer)
            self._pre_host = parts.netloc
        else:
            self._pre_url = None
            self._pre_host = None

        self.records = [r]

    def __str__(self):
        output = "ip: {0}\n".format(self.ip)
        count = 0
        if self._pre_url:
            output += self._pre_url + "\n"
            count += 1

        for r in self.records:
            output += ("    " * count) + (" -> " if count else "") + r.host + r.uri + " ({0})\n".format(r.ts)
            count += 1
        return output

    def __iter__(self):
        return iter(self.records)

    def domains(self):
        """Returns a list of the distinct domains represented in the current
        redirection chain.  Each domain will only appear in the list once,
        in the order they appeared in the redirection chain (earliest to
        latest).

        Return:
            A list of zero or more unique domains in the redirection chain
        """
        hosts = []
        for r in self:
            if r.host not in hosts:
                hosts.append(r.host)
        return hosts

    def head_host(self):
        if self._pre_url:
            return self._pre_host
        else:
            return self.records[0].host

    def tail(self):
        return self.records[-1]

    def len(self):
        num_records = len(self.records)
        if self._pre_url:
            num_records += 1
        return num_records

    def add_record(self, record, record_filter=None, ignore_self_refs=True):
        """Attempts to add a given BroRecord to the current referrer chain.
        This method checks to see if it makes sense to add the given record
        to the referrer chain (by checking the ip of the requester, whether
        the referrer of the given record matches the domain + path of the
        last record in this chain, and if the record comes after the
        last the record in the chain)

        Args:
            record -- a BroRecord element to try and add to a referrer chain

        Keyword Args:
            record_filter    -- an optional function that, if provided, should take
                                two arguments of bro records, and should provide
                                True if the record should be included
            ignore_self_refs -- sometimes pages will record references to
                                themselves (to do things like setting a cookie
                                serverside).  Setting this flag to True will
                                not include these self references.

        Return:
            True if the given record was added to the chain, otherwise False
        """
        if record.id_orig_h != self.ip:
            return False

        tail_record = self.tail()

        if record.ts < tail_record.ts:
            return False

        referrer_url = record.referrer
        if ignore_self_refs and self.tail_url == record.host + record.uri:
            return False

        if self.tail_url != referrer_url:
            return False

        if record_filter and not record_filter(tail_record, record):
            return False

        self.tail_url = record.host + record.uri
        self.records.append(record)
        return True
