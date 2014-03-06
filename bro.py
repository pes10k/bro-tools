from collections import namedtuple

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


class BroRecordWindow(object):
    """Keep track of a sliding window of BroRecord objects, and don't keep more
    than a given amount (defined by a time range) in memory at a time"""

    def __init__(self, time=.5):
        # A collection of BroRecords that all occurred less than the given
        # amount of time before the most recent one (in order oldest to newest)
        self._collection = []

        # Window size of bro records to keep in memory
        self._time = time

    def prune(self):
        """Remove all BroRecords that occured more than self.time before the
        most recent BroRecord in the collection.

        Returns:
            An int count of the number of objects removed from the collection
        """

        # Simple case that if we have no stored BroRecords, there can't be
        # any to remove
        if len(self._collection) == 0:
            return 0

        removed_count = 0
        most_recent_time = self._collection[-1].ts

        while len(self._collection) > 1 and self._collection[0].ts + self._time < most_recent_time:
            self._collection = self._collection[1:]
            removed_count += 1

        return removed_count

    def append(self, record):
        """Adds a BroRecord to the current collection of bro records, and then
        cleans to watched collection to remove old records (records before the)
        the sliding time window.

        Args:
            record -- A BroRecord, created by the bro_records function
        """
        self._collection.append(record)
        self.prune()

    def referrer(self, record):
        """Checks to see if the current collection contains a record that could
        be the referrer for the given record.  This is done by checking to
        see if there are any records in the collection that
            a) have a host+uri pair that match the passed records referrer
            b) have a requesting ip address that matches the passed records
               ip address

        Args:
            record -- A BroRecord named tuple

        Return:
            Either a BroRecord that could be the log record that directed
            the provided record, or None if no such record exists
        """
        for r in self._collection:
            r_path = r.host + r.uri
            if record.id_orig_h == r.id_orig_h and record.referrer == r_path:
                return r
        return None
