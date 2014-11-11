"""
Sqlalchemy classes for SQL records describing training related
attributes of cookie setting amazon records.
"""

import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy import ForeignKey, Enum
from sqlalchemy.orm import relationship

Base = declarative_base()


class Graph(Base):
    """Defines SQL rows that describe collected graphs of BroRecords."""

    __tablename__ = "graphs"

    # Unique identifer for a bro graph, which will be the `BroGraph.hash()`
    # value.
    id = Column(String, primary_key=True)

    # The name of the file this graph is stored in on disk, used for debugging
    # and testing purposes mostly.
    file = Column(String)

    # The name of the Amazon tracking tag used in this record.  Ie which
    # affiliate marketer got the credit for this cookie set.
    tag = Column(String, index=True)

    # Reference to the host / domain that sent the request to Amazon that
    # ended up setting the cookie.  This can be NULL if no redirect
    # information is available
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=True)
    domain = relationship("Domain")

    # The amount of time that passed between the referrer page being loaded
    # and the request to Amazon, from that referrer page, being loaded.
    # Can be null if the amazon request is the root of the graph (ie there
    # is no referrer information).
    time_from_referrer = Column(Float, nullable=True)

    # The number of seconds between the cookie-setting request to Amazon
    # and the max distance root of the sub-tree (ie how much time was
    # spent browsing from the Amazon setting request).
    time_to_root = Column(Float, default=0)

    # A human decided label for this graph, whether the graph represents
    # a "valid", "malicious", or "uncertain" cookie stuff event.
    label = Column(String)

    # Timestamp for when the record was created
    created = Column(DateTime, default=datetime.datetime.now)


class Domain(Base):
    """Defines a SQL record type for storing information about a domain
    that referrers people to Amazon affiliate marketing cookie setting
    pages.
    """

    __tablename__ = "domains"

    # Unique identifier for the domain, used for references from the
    # records table.
    id = Column(Integer, primary_key=True)

    # The name of a domain, such as "example.org".
    domain = Column(String, primary_key=True)

    # Whether the domain has a PKI certificate.
    has_cert = Column(Boolean)

    # Whether the given cert is valid or not.  If the domain does not have
    # a cert, this will be null.
    is_cert_valid = Column(Boolean, nullable=True)

    # How long the given cert was valid for, in years.  If there is no cert,
    # this will be null.
    cert_ttl = Column(Integer, nullable=True)

    # The Google PageRank score for this domain.
    page_rank = Column(Integer)

    # Timestamp for when the record was created
    created = Column(DateTime, default=datetime.datetime.now)
