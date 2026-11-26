"""Ids Aegra creates for threads, runs, crons and assistants."""

import uuid


def new_id() -> str:
    """Return a new UUIDv7 string (needs Python 3.14).

    Its first 48 bits are the creation time in Unix milliseconds, so new rows land at the right
    edge of their primary-key index instead of on a random page.
    """
    return str(uuid.uuid7())
