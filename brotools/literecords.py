"""Iterator and class for reading a large number of bro records off disk
using as little memory as possible, which can then be transformed into
a full fledged record graph when needed.  The goal here is to make
building the graphs as-light-weight as possible, memory wise, at the expense
of disk reads."""

from hashlib import sha1
from .records import BroRecord
from .graphs import BroRecordGraph
import datetime
import os

def bro_records(handle, record_filter=None):
    """A generator function for iterating over a a collection of bro records.
    The iterator returns BroRecord objects (named tuples) for each record
    in the given file

    Args:
        handle -- a file handle like object to read lines of bro data off of.

    Keyword Args:
        record_filter -- an optional function that, if provided, should take two
                         arguments of bro records, and should provide True if
                         they should be included in the same chain or not.

    Return:
        An iterator returning triples of values:
            BroRecordLite
            ip
            user_agent
    """
    seperator = None
    file_name = handle.name
    offset = 0
    for raw_row in handle:
        row = raw_row[:-1] # Strip off line end
        if not seperator and row[0:10] == "#separator":
            seperator = row[11:].decode('unicode_escape')
        elif row[0] != "#":
            try:
                timestamp, ip, host, uri, referrer, user_agent = parse_log_entry(row, seperator)
                r = BroRecordLite(file_name, offset, timestamp, ip, host, uri, referrer)
            except Exception, e:
                print "Bad line entry"
                print "File: {0}".format(handle.name)
                print "Values: {0}".format(row.split(seperator))
                raise e

            if record_filter and not record_filter(r):
                continue
            yield r, ip, user_agent
        offset = handle.tell()


def parse_log_entry(line, seperator="\t"):
    values = [a if a != "-" else "" for a in line.split(seperator)]
    timestamp = float(values[0])
    ip = values[1]
    host = values[4]
    uri = values[5]
    referrer = values[6]
    user_agent = values[7]
    if referrer[0:7] == "http://":
        referrer = referrer[7:]
    elif referrer[0:8] == "https://":
        referrer = referrer[8:]
    return timestamp, ip, host, uri, referrer, user_agent


def hash_record_values(ip, url):
    values = (ip, url)
    h = sha1()
    h.update("|".join(values))
    return h.digest()


class BroRecordLite(object):

    def __init__(self, file_name, offset, timestamp, ip, host, uri, referrer):
        self.ts = timestamp
        self.record_id = hash_record_values(ip, "{}{}".format(host, uri))
        self.referrer_id = hash_record_values(ip, referrer) if referrer else None
        self.file_name = file_name
        self.offset = offset

    def __str__(self):
        date = datetime.datetime.fromtimestamp(int(self.ts))
        human_date = date.strftime('%Y-%m-%d %H:%M:%S')

        lines = []
        lines.append("Time: {0}".format(human_date))
        lines.append("File: {0}".format(self.file_name))
        lines.append("Offset: {0}".format(self.offset))
        lines.append("Request: {0}".format(self.record_id))
        lines.append("Referrer: {0}".format(self.referrer_id))
        return "\n".join(lines)

    def is_referrer_of(self, r):
        """Returns a boolean response of whether it looks like the current
        BroRecord object is the referrer of the passed BroRecord.  This
        check is True if 1) the IPs match, 2) the requested url of the current
        object is equal to the referrer of the given object, and 3) if the
        passed BroRecord has a timestamp later than the current object.

        Args:
            r -- A LiteBroRecord

        Return:
            True if it looks like the current object could be the referrer
            of the passed object, and otherwise False.
        """
        return ((self.referrer_id == r.request_id) and
                (r.ts <= self.ts))

    def to_record(self, path=".", seperator="\t"):
        """Returns a BroRecord object representing the same underlying
        request that generated this lite bro record.

        Keyword Args:
            path      -- the directory path to read the containing bro record
                         from
            seperator -- the seperator used in this bro record file to split
                         between fields

        Returns:
            A BroRecord object
        """
        file_path = os.path.join(path, self.file_name)
        with open(file_path, 'r') as h:
            h.seek(self.offset)
            line = h.readline()
            record = BroRecord(line, seperator)
        return record


def graphs(handle, time=.5, record_filter=None):
    """A generator function yields BroRecordGraph objects that represent
    pages visited in a browsing session.

    Args:
        handle -- a file handle like object to read lines of bro data off of.

    Keyword Args:
        time          -- the maximum amount of time that can have passed in
                         a browsing session before the graph is closed and
                         yielded
        record_filter -- an optional function that, if provided, should take two
                         arguments of bro records, and should provide True if
                         they should be included in the same chain or not.  Note
                         that this is in addition to the filtering / matching
                         already performed by the BroRecordChain.add_record
                         function

    Return:
        An iterator returns BroRecordGraph objects
    """
    # To avoid needing to iterate over all the graphs, keys in this collection
    # are a simple concatination of IP and user agent, and the values
    # are all the currently active graphs being tracked for that client
    all_client_graphs = {}
    for brl, ip, ua in bro_records(handle, record_filter=record_filter):
        hash_key = ip + "|" + ua

        # By default, assume that we've seen a request by this client
        # before, so start looking for a graph we can add this record to
        # to in the list of all graphs currently tracked for the client
        try:
            graphs = all_client_graphs[hash_key]
            found_graph_for_record = False
            for g in graphs:
                # First make sure that our graphs are not too old.  If they
                # are, yield them and then remove them from our considered
                # set
                if (brl.ts - g.latest_ts) > time:
                    yield g.to_graph()
                    graphs.remove(g)
                    continue

                # If the current graph is not too old to represent a valid
                # browsing session then, see if it is valid for the given
                # bro record.  If so, then we don't need to consider any other
                # graphs on this iteration
                if g.add_node(brl, ip, ua):
                    found_graph_for_record = True
                    break

            # Last, if we haven't found a graph to add the current record to,
            # create a new graph and add the record to it
            if not found_graph_for_record:
                graphs.append(BroRecordGraph(brl))

        # If we've never seen any requests for this client, then
        # there is no way the request could be part of any graph we're tracking,
        # so create a new collection of graphs to search
        except KeyError:
            all_client_graphs[hash_key] = [BroRecordGraphLite(brl, ip, ua)]

    # Last, if we've considered every bro record in the collection, we need to
    # yield the remaining graphs to the caller, to make sure they see
    # ever relevant record
    for graphs in all_client_graphs.values():
        for g in graphs:
            yield g.to_graph()


class BroRecordGraphLite(object):

    def __init__(self, brl, ip, user_agent):
        self.ip = ip
        self.user_agent = user_agent

        self.latest_ts = brl.ts
        self._nodes_by_referrer = {}
        self._nodes_by_referrer[brl.referrer_id] = [brl]

    def to_graph(self, path=".", seperator="\t"):
        all_nodes = []
        for key, nodes in self._nodes_by_referrer.items():
            all_nodes += nodes
        all_nodes = sorted(all_nodes, key=lambda x: x.ts)
        head = all_nodes[0]
        other_nodes = all_nodes[1:]
        graph = BroRecordGraph(head.to_record())
        for n in other_nodes:
            graph.add_node(n.to_record())
        return graph

    def referrer_record(self, brl, ip, user_agent):
        """Returns the BroRecord that could be the referrer of the given
        record, if one exists, and otherwise returns None.  If there
        are multiple BroRecords in this graph that could be the referrer of
        the given record, the most recent candidate is returned.

        Args:
            brl        -- a BroRecordLite object
            ip         -- the ip address associated with the BroRecordLite
                          request
            user_agent -- the user agent that made the request in the underlying
                          bro record request

        Returns:
            The most recent candidate BroRecordLite that could be the referrer
            of the passed BroRecordLite, or None if there are no possible
            matches.
        """
        # We can special case situations where the IP addresses don't match,
        # in order to save ourselves having to walk the entire line of nodes
        # again in a clear miss situation
        if ip != self.ip:
            return None

        # Similarly, we can special case situations where user agents
        # don't match.  Since all records in a single graph will have
        # the same user agent, we can quick reject any records that have
        # a user agent other than the first user agent seen in the graph.
        if user_agent != self.user_agent:
            return None

        try:
            for n in self._nodes_by_referrer[brl.referrer_id]:
                if n.ts < brl.ts:
                    return n
        except KeyError:
            return None

    def add_node(self, brl, ip, user_agent):
        """Attempts to add the given BroRecordLite as a child (successor) of its
        referrer in the graph.

        Args:
            brl        -- a BroRecordLite object
            ip         -- the ip address associated with the BroRecordLite
                          request
            user_agent -- the user agent that made the request in the underlying
                          bro record request

        Returns:
            True if a referrer of the BroRecordLite could be found and the given
            record was added as its child / successor.  Otherwise, False is
            returned, indicating no changes were made."""
        referrer_node = self.referrer_record(brl, ip, user_agent)
        if not referrer_node:
            return False

        self.latest_ts = max(brl.ts, self.latest_ts)

        try:
            self._nodes_by_referrer[brl.request_id].append(brl)
        except KeyError:
            self._nodes_by_referrer[brl.request_id] = [brl]

        return True
