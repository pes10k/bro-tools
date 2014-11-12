"""Tools for looking for instances of amazon cookie stuffing in individual
and collections of BroRecords."""

import re
from .affiliate import AffiliateHistory, FULL_DOMAIN

AMZ_TOKEN_PATTERN = re.compile('session-token=([^\;]+)')


def session_token(record):
    """Returns the amazon session token cookie in the given request, if one
    exists.

    Args:
        record -- a BroRecord instances

    Return:
        Either the amazon session token, as a string, or None if there
        was no token cookie provided in the response.
    """
    if not record.cookies:
        return None

    match = AMZ_TOKEN_PATTERN.search(record.cookies)
    if not match:
        return None

    return match.group(1)


class AmazonAffiliateHistory(AffiliateHistory):
    """Subclass of Affiliate History used for tracking client interactions
    with amazon affiliates.
    """

    @classmethod
    def session_id(cls, record):
        return session_token(record)

    @classmethod
    def checkout_urls(cls):
        return ('handle-buy-box',)

    @classmethod
    def referrer_tag(cls, record):
        return 'tag'

    @classmethod
    def cookie_set_pattern(cls):
        try:
            return cls._cookie_set_pattern
        except AttributeError:
            pattern = re.compile(r'/(?:.*dp/.*)?[&?]tag=')
            cls._cookie_set_pattern = pattern
            return cls._cookie_set_pattern

    @classmethod
    def domains(cls):
        return (("amazon.com", FULL_DOMAIN), ("www.amazon.com", FULL_DOMAIN))

    @classmethod
    def name(cls):
        return "Amazon Affiliate"
