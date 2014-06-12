"""Code for collecting and parsing collections of bro records as windows
of browsing sessions that a single client participate in during a given
time period."""


class BroRecordClient(object):

    def __init__(self, graph):
        """Initilizer requires a BroRecordGraph to determin information
        about
