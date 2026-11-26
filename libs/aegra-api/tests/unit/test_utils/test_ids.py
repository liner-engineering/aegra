"""Ids Aegra creates are time-ordered UUIDv7 strings."""

import time
import uuid
from pathlib import Path

from aegra_api.utils.ids import new_id

SOURCE = Path(__file__).resolve().parents[3] / "src" / "aegra_api"


def test_new_id_is_a_uuid7_string() -> None:
    value = new_id()

    parsed = uuid.UUID(value)
    assert str(parsed) == value
    assert parsed.version == 7
    assert parsed.variant == uuid.RFC_4122


def test_new_ids_sort_in_the_order_they_were_made() -> None:
    made = [new_id() for _ in range(2_000)]

    assert made == sorted(made)
    assert len(set(made)) == len(made)


def test_new_id_starts_with_its_creation_time() -> None:
    before = time.time_ns() // 1_000_000
    millis = uuid.UUID(new_id()).int >> 80
    after = time.time_ns() // 1_000_000

    assert before <= millis <= after


def test_aegra_makes_no_random_uuid4_ids() -> None:
    """A rebase onto a new upstream release must not bring a ``str(uuid4())`` id back.

    Random tokens that are not row ids (``uuid4().hex`` delivery ids) stay random.
    """
    offenders = [
        f"{path.relative_to(SOURCE)}:{number}"
        for path in SOURCE.rglob("*.py")
        for number, line in enumerate(path.read_text().splitlines(), start=1)
        if "str(uuid4())" in line or "str(uuid.uuid4())" in line or "uuid.uuid4())" in line
    ]

    assert offenders == []
