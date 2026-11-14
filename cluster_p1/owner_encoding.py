"""Owner representation only; this predicate grants no identity or permission.

Keep legacy bytes unchanged. Accept the existing user-owner encoding verbatim;
never normalize, truncate, strip prefixes or rehash it for another identifier.
"""
import re


def valid_owner(value):
    return type(value)is str and (
        re.fullmatch(r'[A-Za-z0-9_-]{1,64}',value) is not None
        or re.fullmatch(r'usr_[0-9a-f]{64}',value) is not None)
