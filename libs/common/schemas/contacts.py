"""Organisation contacts schema — 22-field record.

Kafka topic: orgs.validated.v1 (embedded in org message envelope)
Conflict key: place_id
"""

import msgspec


class Contact(msgspec.Struct, kw_only=True, omit_defaults=True, gc=False):
    """Contact record extracted from org additional_info by exporters."""

    place_id: str
    phones: list[str] | None = None
    mails: list[str] | None = None
    web_link: str | None = None
    socials: list[str] | None = None
    facebook: str | None = None
    instagram: str | None = None
    twitter: str | None = None
    linkedin: str | None = None
    youtube: str | None = None
    tiktok: str | None = None
    telegram: str | None = None
    whatsapp: str | None = None
    viber: str | None = None
    line: str | None = None
    wechat: str | None = None
    skype: str | None = None
    pinterest: str | None = None
    tripadvisor: str | None = None
    booking: str | None = None
    yelp_url: str | None = None
    additional: dict | None = None
