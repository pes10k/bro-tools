import sqltypes
from contrib.som import MiniSom
import numpy

class BoolSOM(MiniSom):

    def __init__(self):
        super(BoolSOM, self).__init__(2, 1, 2, sigma=0.3)

    @classmethod
    def vector_for_value(cls, value):
        vector_size = 2
        vector = numpy.zeros(vector_size)
        vector[1 if value else 0] = 1
        return vector


class RangeSOM(MiniSom):

    def __init__(self):
        ranges = self.__class__.ranges
        super(RangeSOM, self).__init__(len(ranges) + 1, 1, len(ranges) + 1, sigma=0.3)

    @classmethod
    def vector_for_value(cls, value):
        vector_size = len(cls.ranges)
        index = 0
        for max_value in cls.ranges:
            if value <= max_value:
                break
            index += 1
        vector = numpy.zeros(vector_size + 1)
        vector[index] = 1
        return vector


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

    ranges = [
        1,
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
    ]


class TimeAfterSetSOM(RangeSOM):

    ranges = [
        1,
        2,
        4,
        8,
        16,
        32,
        64,
        128,
        256,
        512,
        1024
    ]


class TagCountSOM(RangeSOM):

    ranges = [
        5,
        10,
        15,
        20,
        25,
        30,
        35,
        40,
        45,
        51,
    ]


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
