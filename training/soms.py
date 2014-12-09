import sqltypes
from contrib.som import MiniSom
import numpy

class DumbSOM(object):

    def add_value(self, value):
        self.weights[self.winner(value)] += 1

    def normalize(self):
        self.weights = numpy.divide(self.weights, numpy.sum(self.weights))


class BoolSOM(DumbSOM):

    def __init__(self):
        self.weights = numpy.zeros(2)

    def winner(self, value):
        return 1 if value else 0


class RangeSOM(DumbSOM):

    def __init__(self):
        ranges = self.__class__.ranges
        self.weights = numpy.zeros(len(ranges) + 1)

    def winner(self, value):
        index = 0
        for max_value in self.__class__.ranges:
            if value <= max_value:
                break
            index += 1
        return index


class YearsRegisteredSOM(RangeSOM):

    ranges = [
        1,
        2,
        4,
        8,
        16,
        32,
    ]


class TimeFromReferrerSOM(RangeSOM):

    ranges = [5, 10, 30]


class TimeAfterSetSOM(RangeSOM):
    
    ranges = [5, 10, 30]


class TagCountSOM(RangeSOM):

    ranges = [1, 5, 10]


class PageRankSOM(RangeSOM):

    ranges = [
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9
    ]


class DomainRegistrationYearSOM(RangeSOM):

    ranges = [
        1993,
        1994,
        1995,
        1996,
        1997,
        1998,
        1999,
        2000,
        2001,
        2002,
        2003,
        2004,
        2005,
        2006,
        2007,
        2008,
        2009,
        2010,
        2011,
        2012,
        2013,
        2014
    ]


class GraphSizeSOM(RangeSOM):

    ranges = [
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        2048,
        4096
    ]


class AlexiaRankSOM(RangeSOM):

    ranges = [
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024,
        2048,
        4096,
        8192,
        16384,
        32768,
        65536,
        131072,
        262144,
        524288,
        1048576,
        2097152,
        4194304,
        8388608,
        16777216,
        33554432
    ]
