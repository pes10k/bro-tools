"""Classes and functions useful for parsing and representing collections
of BroRecords as a DAG, with each node's successor being the page that
lead to a given page, and its children being the pages visted next."""

import networkx as nx
from .records import bro_records
from .chains import BroRecordChain
import logging

def merge_graphs(handle, time=10, state=False):
    """Takes an iterator of BroRecordGraph objects, and yields back
    BroRecordGraphs, with the records merged together as possible.

    Args:
        handle -- An iterator that returns BroRecordGraph objects

    Keyword Args:
        time  -- the maximum amount of time that can have passed in
                 a browsing session before the graph is closed and yielded
        state -- if set, the iterator also yields back the state of the
                 generator.  Only really useful for debugging

    Return:
        Yields pairs of values.  The first value is aBroRecordGraph, and
        the second value is a boolean description of whether the graph has been
        changed (ie if it has absorbed another graph)
    """
    log = logging.getLogger("brorecords")

    state = {
        "# changed": 0,
        "# unchanged": 0,
        "merges": 0,
        "count": 0,
        "graphs_for_client": {},
        # Graphs sorted by latest child record timestamp, earliest value first
        "graphs_by_date": [],
        # Keep track of which graphs were altered by having child graphs
        # merged into them
        "changed": []
    }

    def _yield_values(graph, path):
        if graph in state['changed']:
            state['# changed'] += 1
        else:
            state['# unchanged'] += 1
        if state:
            return path, graph, (graph in state['changed']), state
        else:
            return path, graph, (graph in state['changed'])

    def graph_hash(graph):
        return graph.ip + "|" + graph.user_agent

    def add_to_state(candidate_graph, path):
        hash_key = graph_hash(candidate_graph)
        record = (candidate_graph, path)

        # Special case for considering the first graph.  If this is the cae,
        # then we know there is no sorting or other change needed,
        # and that this will be the first graph for this client.  We can
        # easy out then
        if len(state['graphs_by_date']) == 0:
            state['graphs_for_client'][hash_key] = [record]
            state['graphs_by_date'].append(record)

        try:
            state['graphs_for_client'][hash_key].append(record)
        except KeyError:
            state['graphs_for_client'][hash_key] = [record]

        state['graphs_by_date'].append(record)
        state['graphs_by_date'].sort(key=lambda x: x[0].latest_ts)

        # Now we need to figure out where to insert the new record into
        # the existing sorted collection of graphs.  We do this by just
        # walking through the collection until we find one with a timestamp
        # after us and inserting the graph there.  Index is the index we might
        # insert the graph into
        # match = None
        # for index in range(len(state['graphs_by_date'])):
        #     if graph.latest_ts > candidate_graph.latest_ts:
        #         match = True
        #         break

        # if not match:
        #     index = len(state['graphs_by_date'])

        # state['graphs_by_date'].insert(index, candidate_graph)

    def remove_from_state(graph, path):
        hash_key = graph_hash(graph)
        record = (graph, path)

        if graph in state['changed']:
            state['changed'].remove(graph)

        if record in state['graphs_by_date']:
            state['graphs_by_date'].remove(record)

        try:
            state['graphs_for_client'][hash_key].remove(record)
        except KeyError:
            pass

    def prune_state(most_recent_graph, path):
        latest_valid_time = most_recent_graph.latest_ts - time
        removed = []

        # Look for any graphs in the collection that are too old to still be
        # considered.  If there are any, eject them.  Since the graphs
        # are by oldest to newest, we can stop looking through the collection
        # the moment we find a valid one
        for prev_graph, prev_path in state['graphs_by_date']:
            if prev_graph.latest_ts >= latest_valid_time:
                break
            removed.append((prev_graph, prev_path))
            remove_from_state(prev_graph, prev_path)
        return removed

    for path, graph in handle:

        hash_key = graph_hash(graph)
        if hash_key not in state['graphs_for_client']:
            state['graphs_for_client'][hash_key] = []

        state['count'] += 1

        for old_graph, old_path in prune_state(graph, path):
            yield _yield_values(old_graph, old_path)

        client_graphs = state['graphs_for_client'][hash_key]
        graph_is_merged = False
        for existing_graph, existing_path in client_graphs:
            if (graph.latest_ts - existing_graph.latest_ts <= time and
                existing_graph.add_graph(graph)):
                state['merges'] += 1
                # If we succeed in merging the new graph into an existing
                # graph, we need to read it to our state collections,
                # to make sure it is sorted correctly
                remove_from_state(existing_graph, existing_path)
                add_to_state(existing_graph, existing_path)
                state['changed'].append(existing_graph)
                log.info(" * Found merge: {0}".format(graph._root.url))
                log.info("     Changed graphs: {0}".format(len(state['changed'])))
                graph_is_merged = True
                break

        if not graph_is_merged:
            add_to_state(graph, path)

    for prev_graph, prev_path in state['graphs_by_date']:
        yield _yield_values(prev_graph, prev_path)

def graphs(handle, time=10, record_filter=None):
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
    for r in bro_records(handle, record_filter=record_filter):
        hash_key = r.id_orig_h + "|" + r.user_agent

        # By default, assume that we've seen a request by this client
        # before, so start looking for a graph we can add this record to
        # to in the list of all graphs currently tracked for the client
        try:
            graphs = all_client_graphs[hash_key]
            found_graph_for_record = False
            dirty_graphs = []
            for g in graphs:
                # First make sure that our graphs are not too old.  If they
                # are, yield them and then remove them from our considered
                # set
                if (r.ts - g.latest_ts) > time:
                    yield g
                    dirty_graphs.append(g)
                    continue

                # If the current graph is not too old to represent a valid
                # browsing session then, see if it is valid for the given
                # bro record.  If so, then we don't need to consider any other
                # graphs on this iteration
                if g.add_node(r):
                    found_graph_for_record = True
                    break

            # Last, if we haven't found a graph to add the current record to,
            # create a new graph and add the record to it
            if not found_graph_for_record:
                graphs.append(BroRecordGraph(r))

            for dg in dirty_graphs:
                graphs.remove(dg)

        # If we've never seen any requests for this client, then
        # there is no way the request could be part of any graph we're tracking,
        # so create a new collection of graphs to search
        except KeyError:
            all_client_graphs[hash_key] = [BroRecordGraph(r)]

    # Last, if we've considered every bro record in the collection, we need to
    # yield the remaining graphs to the caller, to make sure they see
    # ever relevant record
    for graphs in all_client_graphs.values():
        for g in graphs:
            yield g


class BroRecordGraph(object):

    def __init__(self, br):
        self._g = nx.DiGraph()
        self.ip = br.id_orig_h
        self.user_agent = br.user_agent

        # The root element of the graph can either be the referrer of the given
        # bro record, if it exists, or otherwise the record itself.
        self._g.add_node(br)
        self._root = br

        # Keep track of what range of time this graph represents
        self.earliest_ts = br.ts
        self.latest_ts = br.ts

        # Since we expect that we'll see nodes in a sorted order (ie
        # each examined node will be the lastest-one-seen-yet)
        # we can make date comparisons of nodes faster by
        # keeping a seperate set of references to them, from earliest to
        # latest
        self._nodes_sorted = [br]

        # To make searching for referrers faster, we also keep a referrence
        # to each node by its url.  Here, each record's url is the key
        # and the corresponding value is a list of all records requesting
        # that url
        self._nodes_by_url = {}
        self._nodes_by_url[br.url] = [br]

        # Finally, also keep a reference to all nodes by host, where keys
        # are domains, and the values are a list of all records in the
        # graph to that domain
        self._nodes_by_host = {}
        self._nodes_by_host[br.host] = [br]

    def __str__(self):
        return self.summary()

    def __len__(self):
        return len(self._nodes_sorted)

    def summary(self, detailed=True):
        """Returns a string description of the current graph.

        Keyword Args:
            detailed -- boolean, if true, the returned summary includes the
                        client's IP and the date of the inital request

        Returns:
            A string, describing the requests contained in this graph, and
            optionally information about the client and time the initial
            request was made.
        """
        def _print_sub_tree(node, parent=None, level=0):
            response = ("  " * level)
            if parent:
                dif = node.ts - parent.ts
                response += "|-" + str(round(dif, 2)) + "-> "
            response += node.url + "\n"

            children = self.children_of_node(node)
            for c in children:
                response += _print_sub_tree(c, parent=node, level=(level + 1))
            return response

        if detailed:
            output = self.ip + "\n" + self._root.date_str + "\n"
            if self._root.name:
                output += self._root.name + "\n"
            output += "-----\n"
        else:
            output = ""
        return output + _print_sub_tree(self._root)

    def referrer_record(self, candidate_record):
        """Returns the BroRecord that could be the referrer of the given
        record, if one exists, and otherwise returns None.  If there
        are multiple BroRecords in this graph that could be the referrer of
        the given record, the most recent candidate is returned.

        Args:
            candidate_record -- a BroRecord object

        Returns:
            The most recent candidate BroRecord that could be the referrer of
            the passed BroRecord, or None if there are no possible matches.
        """
        # We can special case situations where the IP addresses don't match,
        # in order to save ourselves having to walk the entire line of nodes
        # again in a clear miss situation
        if candidate_record.id_orig_h != self.ip:
            return None

        # Similarly, we can special case situations where user agents
        # don't match.  Since all records in a single graph will have
        # the same user agent, we can quick reject any records that have
        # a user agent other than the first user agent seen in the graph.
        if candidate_record.user_agent != self.user_agent:
            return None

        try:
            for n in self._nodes_by_url[candidate_record.referrer]:
                if n.ts < candidate_record.ts:
                    return n
        except KeyError:
            return None

    def add_node(self, br):
        """Attempts to add the given BroRecord as a child (successor) of its
        referrer in the graph.

        Args:
            br -- a BroRecord object

        Returns:
            True if a referrer of the the BroRecord could be found and the given
            record was added as its child / successor.  Otherwise, False is
            returned, indicating no changes were made."""
        referrer_node = self.referrer_record(br)
        if not referrer_node:
            return False

        time_difference = br.ts - referrer_node.ts
        self._g.add_weighted_edges_from([(referrer_node, br, time_difference)])
        self.latest_ts = max(br.ts, self.latest_ts)
        self._nodes_sorted.append(br)

        if br.url not in self._nodes_by_url:
            self._nodes_by_url[br.url] = []
        self._nodes_by_url[br.url].append(br)

        if br.host not in self._nodes_by_host:
            self._nodes_by_host[br.host] = []
        self._nodes_by_host[br.host].append(br)

        return True

    def add_graph(self, child_graph):
        """Attempts to merge in a child group into the current graph.
        This is done by seeing if the head of the child graph can find any
        referrer in the parent graph.

        Args:
            child_graph -- a BroRecordGraph instance

        Return:
            True if the child graph could be added to / merged into the current
            graph, otherwise False.
        """
        child_head = child_graph._root
        referrer_node = self.referrer_record(child_head)
        if not referrer_node:
            return False

        for n in child_graph.nodes():
            self.add_node(n)

        self._nodes_sorted.sort(key=lambda x: x.ts)

        return True

    def nodes(self):
        """Returns an list of BroRecords, from oldest to newest, that are in
        the graph.

        Return:
            A list of zero or more BroRecords
        """
        return self._nodes_sorted

    def hosts(self):
        """Returns a list of all of the hosts represented in the graph.

        Return:
            A list of zero or more strings
        """
        return self._nodes_by_host.keys()

    def nodes_for_host(self, host):
        """Returns a list of all nodes in the graph that are requests to
        a given host.

        Args:
            host -- a string describing a host / domain

        Return:
            A list of zero or more BroRecords all requesting the given
            host.
        """
        try:
            return self._nodes_by_host[host]
        except KeyError:
            return []

    def nodes_for_hosts(self, *args):
        """Returns a list of all nodes in the graph that are requests to
        any of the given hosts.

        Args:
            args -- one or more host names, as strings

        Return:
            A list of zero or more BroRecords, that were made to one of the
            given hosts.
        """
        nodes = []
        for host in args:
            nodes_for_host = self.nodes_for_host(host)
            if nodes_for_host:
                nodes += nodes_for_host
        return nodes

    def leaves(self):
        """Returns a iterator of BroRecords, each of which are leaves in t
        graph (meaining record nodes that are there referrer for no other node).

        Returns:
            An iterator of BroRecord nodes"""
        g = self._g
        return (n for n in g.nodes_iter() if not g.successors(n))

    def node_domains(self):
        """Returns a dict representing a mapping from domain to a list of all
        nodes in the collection that are requests against that domain.

        Return:
            A dict with keys being domains (as strings), and values being
            lists of one or more BroRecord objects
        """
        mapping = {}
        for n in self._g.nodes_iter():
            try:
                mapping[n.host].append(n)
            except KeyError:
                mapping[n.host] = [n]
        return mapping

    def nodes_for_domain(self, domain):
        """Returns a list of nodes in the collection where the requested host
        matches the provided domain.

        Args:
            domain -- a valid domain, such as example.org

        Return:
            A list of zero or more nodes in the current collection that
            represent requests to the given domain
        """
        g = self._g
        return [n for n in g.nodes_iter() if n.host == domain]

    def graph(self):
        """Returns the underlying graph representation for the BroRecords

        Returns:
            The underlying graph representation, a networkx.DiGraph object
        """
        return self._g

    def remaining_child_time(self, br):
        """Returns the amount of time that the browsing session - captured
        by this graph - continued under the given node.  This is the same
        thing, and computed as, the max of times between the given node
        and all nodes below it.

        Args:
            br -- a BroRecord

        Return:
            A float, describing a number of seconds, or None if the given
            node is not in the graph.
        """
        g = self._g

        if not g.has_node(br):
            return None

        def _time_below(node, parent=None):
            if parent:
                cur_time = node.ts - parent.ts
            else:
                cur_time = 0
            cs = self.children_of_node(node)

            if len(cs) == 0:
                return cur_time

            try:
                max_time = max([_time_below(n, parent=node) for n in cs])
                return cur_time + max_time
            except RuntimeError:
                msg = ("Infinite recursive loop in `remaining_child_time`\n" +
                      "\n" +
                      "Node:\n" +
                      str(node) + "\n\n" +
                      "Graph:\n" +
                      str(self))
                raise(Exception(msg))

        return _time_below(self._root)

    def max_child_depth(self, br):
        """Returns the count of the longest path from the given node to a leaf
        under the node.  If the given node has no children, the returned value
        will be 0.  If the given node is not in the graph, None is returned.

        Args:
            br -- a BroRecord

        Returns:
            None if the given record is not in the graph, and otherwise returns
            an integer.
        """
        g = self._g

        if not g.has_node(br):
            return None

        def _max_depth(node, count=0):
            children = g.successors(node)
            if len(children) == 0:
                return count
            # Occasionally there is a recursion depth error here,
            # which is baffling at the moment.  So, for now, just escape
            # our way out of it
            try:
                return max([_max_depth(c, (count + 1)) for c in children])
            except RuntimeError:
                return count + 1

        return _max_depth(br)

    def children_of_node(self, br):
        """Returns a list of BroRecord objects that were directed to from
        the record represented by the given BroRecord.

        Args:
            br -- a BroRecord

        Return:
            A list of zero or more BroRecords, or None if the given BroRecord
            is not in the current graph.
        """
        g = self._g
        if not g.has_node(br):
            return None
        return g.successors(br)

    def parent_of_node(self, br):
        """Returns a BroRecord object that is the referrer of the given record
        in the graph, if available.

        Args:
            br -- a BroRecord

        Return:
            Either a BroRecord if the passed BroRecord is in the graph and
            has a parent, or None if the given BroRecord either isn't in
            the graph or has no parent.
        """
        g = self._g
        if not g.has_node(br):
            return None
        parents = g.predecessors(br)
        if len(parents) != 1:
            return None
        else:
            return parents[0]

    def chain_from_node(self, br):
        """Returns a BroRecordChain object, describing the chain of requests
        that lead to the given BroRecord.

        Args:
            br -- a BroRecord

        Return:
            None if the given BroRecord is not in the give BroRecordGraph,
            otherwise a BroRecordChain object describing how the record br
            was arrived at from the root of the graph / DAG.
        """
        g = self._g
        if not g.has_node(br):
            return None

        path = [br]
        node = br
        while True:
            parents = g.predecessors(node)
            if not len(parents):
                break
            node = parents[0]
            path.append(node)

        chain = BroRecordChain(path[-1])
        for r in path[1::-1]:
            chain.add_record(r)
        return chain
