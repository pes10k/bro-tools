"""
Sqlalchemy classes for SQL records describing training related
attributes of cookie setting amazon records.
"""

import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy import ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound
import features

Base = declarative_base()


class CookieSet(Base):
    """Defines SQL rows that describe collected graphs of BroRecords."""

    __tablename__ = "cookiesets"

    # Unique identifer for a bro graph, which will be the `BroGraph.hash()`
    # value.
    id = Column(String, primary_key=True)

    # The name of the file this graph is stored in on disk, used for debugging
    # and testing purposes mostly.
    file = Column(String)

    # The URL to amazon that set the request
    url = Column(String)

    # The time the request was made on
    request_time = Column(DateTime)

    # The name of the Amazon tracking tag used in this record.  Ie which
    # affiliate marketer got the credit for this cookie set.
    tag = Column(String, index=True)

    referrer_id = Column(Integer, ForeignKey('referrers.id'), nullable=True)
    referrer = relationship("Referrer")

    # The amount of time that passed between the referrer page being loaded
    # and the request to Amazon, from that referrer page, being loaded.
    # Can be null if the amazon request is the root of the graph (ie there
    # is no referrer information).
    time_from_referrer = Column(Float, nullable=True)

    # The number of seconds between the cookie-setting request to Amazon
    # and the max distance root of the sub-tree (ie how much time was
    # spent browsing from the Amazon setting request).
    time_after_set = Column(Float, nullable=True)

    # A human decided label for this graph, whether the graph represents
    # a "valid", "stuff", or "uncertain" cookie stuff event.
    label = Column(String)

    # Timestamp for when the record was created
    created = Column(DateTime, default=datetime.datetime.now)


class Referrer(Base):
    """Defines a SQL record type for storing information about a
    referrer page, that directed a client to an Amazon cookie setting
    page.
    """

    __tablename__ = "referrers"

    id = Column(Integer, primary_key=True)

    # Reference to the host / domain that sent the request to Amazon that
    # ended up setting the cookie.  This can be NULL if no redirect
    # information is available
    domain_id = Column(Integer, ForeignKey('domains.id'), nullable=True)
    domain = relationship("Domain")

    url = Column(String, unique=True)

    # Whether the given page is reachable and returns some non-HTTP
    # error code
    is_reachable = Column(Boolean)

    # The Google PageRank score for this url.
    page_rank = Column(Integer, nullable=True)

    # The position of the url in the Alexa Traffic Rankings.
    # Null if the site is not listed
    alexa_rank = Column(Integer, nullable=True)

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
    domain = Column(String, unique=True)

    # Weather there is any server responding to the given domain
    # (ie if the domain is registered)
    is_registered = Column(Boolean)

    # The number of years the domain was registered for.
    years_registered = Column(Integer, nullable=True)

    # The date the domain was registered on
    registration_date = Column(DateTime, nullable=True)

    # Whether the domain talks SSL using a non-expired x509 cert
    # (note that we're not doing OCSP checking...)
    is_ssl = Column(Boolean, nullable=True)

    # Timestamp for when the record was created
    created = Column(DateTime, default=datetime.datetime.now)


def get_set(graph, session):
    query = session.query(CookieSet).filter(CookieSet.id == graph.hash())
    try:
        return query.one()
    except NoResultFound, e:
        return None


def get_referrer_id(referrer, session):
    found_referrer = get_referrer(referrer, sesssion)
    if found_referrer:
        return found_referrer.id
    return save_referrer(referrer, session)


def get_referrer(referrer, session):
    query = session.query(Domain).filter(Referrer.url == referrer.url())
    try:
        return query.one()
    except NoResultFound, e:
        return None


def save_referrer(referrer, session):
    ref_host = referrer.host

    referrer_host = get_domain(ref_host)
    if referrer_host:
        referrer_id = referrer_host.id
    else:
        referrer_id = save_domain(referrer_host, session)

    ref_url = referrer.url()
    is_ref_reachable = features.is_url_live(ref_url)
    if is_ref_reachable:
        ref_page_rank = training.features.page_rank(ref_url)
        ref_alexia = training.features.alexia(ref_url)
    else:
        ref_page_rank = None
        ref_alexia = None

    new_referrer = Referrer(domain_id=None, url=ref_url,
                            is_reachable=is_ref_reachable,
                            page_rank=ref_page_rank,
                            alexa_rank=ref_alexia)


def get_domain(domain, session):
    query = session.query(Domain).filter(Domain.domain == domain)
    try:
        return query.one()
    except NoResultFound, e:
        return None


def save_domain(domain, session):
    whois_rec = features.whois_for_domain(ref_host)
    is_domain_registered = whois_rec is not False
    if is_domain_registered:
        domain_reg_years = features.years_for_domain(whois_rec)
        domain_reg_date = whois_rec.creation_date[0]
        domain_is_ssl = features.fetch_cert(ref_host) is not None
    else:
        domain_reg_years = None
        domain_reg_date = None
        domain_is_ssl = None

    new_domain = Domain(domain=domain, is_registered=is_domain_registered,
                        years_registered=domain_reg_years,
                        registration_date=domain_reg_date,
                        is_ssl=domain_is_ssl)
    session.add(new_domain)
    return new_domain.id
