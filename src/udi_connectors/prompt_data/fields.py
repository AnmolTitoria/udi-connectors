"""Turns a free-text prompt like "150 rows with name, email, signup_date,
amount and is_active" into a concrete field list + row count, then generates
fake rows for it with Faker. Purely local heuristics — no external API calls,
no cost, no network access.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable

import pyarrow as pa
from faker import Faker

DEFAULT_FIELDS = ["id", "name", "email", "created_at"]

_ROW_COUNT_RE = re.compile(
    r"(\d[\d,]*)\s+(?:\w+\s+)?(?:rows?|records?|entries|items|lines)", re.IGNORECASE
)

# Text that introduces the field list, e.g. "... with columns: a, b, c" or
# "containing name, email". Whatever follows is split into field names.
_FIELD_LIST_RE = re.compile(
    r"(?:columns?|fields?|containing|with|having)\s*:?\s*(.+)$",
    re.IGNORECASE,
)

_SPLIT_RE = re.compile(r",|;|\band\b|&|\n", re.IGNORECASE)


@dataclass
class FieldSpec:
    name: str
    arrow_type: pa.DataType
    generate: Callable[[Faker, int], object]


def _slugify(raw: str) -> str:
    cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", raw.strip()).strip("_").lower()
    return cleaned or "value"


def parse_prompt(prompt: str, default_row_count: int) -> tuple[list[str], int]:
    """Returns (field_names, row_count). Falls back to DEFAULT_FIELDS when
    the prompt doesn't name any fields, and to default_row_count when it
    doesn't mention a count — so any input, however loose, still produces a
    usable dataset."""
    row_count = default_row_count
    count_match = _ROW_COUNT_RE.search(prompt)
    # Cut the row-count phrase out before hunting for fields, so e.g. "50
    # records: id, name" doesn't glue "50_records" onto the first field —
    # there's no explicit list-introducing keyword before that colon for
    # _FIELD_LIST_RE to anchor on.
    remainder = prompt
    if count_match:
        row_count = int(count_match.group(1).replace(",", ""))
        remainder = prompt[: count_match.start()] + prompt[count_match.end() :]
    remainder = remainder.strip().lstrip(":,;.- ").strip()

    list_match = _FIELD_LIST_RE.search(remainder)
    explicit_list = list_match is not None
    candidate = list_match.group(1) if list_match else remainder

    field_names: list[str] = []
    for chunk in _SPLIT_RE.split(candidate):
        name = _slugify(chunk)
        # Drop stray tokens ("with", numbers-only fragments left over after
        # the row-count phrase, empty splits) rather than turning them into
        # a garbage column.
        if not name or name.isdigit() or name in ("with", "and", "rows", "row", "records", "record"):
            continue
        if name not in field_names:
            field_names.append(name)

    # Without an explicit "with/columns/fields/..." keyword, a single
    # leftover chunk is a stray sentence ("just give me some data"), not a
    # field name — only trust an unmarked guess when it actually looks like
    # a list (multiple comma/and-separated chunks).
    if not explicit_list and len(field_names) <= 1:
        field_names = []

    if not field_names:
        field_names = list(DEFAULT_FIELDS)

    return field_names, row_count


def _matches(name: str, *keywords: str) -> bool:
    return any(kw in name for kw in keywords)


def resolve_field(name: str) -> FieldSpec:
    """Maps a field name to a Faker-backed generator by keyword heuristics —
    checked most-specific first so e.g. "first_name" doesn't fall through to
    the generic "name" branch. Each generator takes (faker, row_index) so an
    id-like field can produce a sequential value instead of a random one."""
    n = name.lower()

    if n == "id" or n.endswith("_id"):
        return FieldSpec(name, pa.int64(), lambda f, i: i)
    if _matches(n, "uuid", "guid"):
        return FieldSpec(name, pa.string(), lambda f, i: f.uuid4())
    if _matches(n, "email"):
        return FieldSpec(name, pa.string(), lambda f, i: f.email())
    if _matches(n, "first_name", "firstname"):
        return FieldSpec(name, pa.string(), lambda f, i: f.first_name())
    if _matches(n, "last_name", "lastname", "surname"):
        return FieldSpec(name, pa.string(), lambda f, i: f.last_name())
    if _matches(n, "name"):
        return FieldSpec(name, pa.string(), lambda f, i: f.name())
    if _matches(n, "phone"):
        return FieldSpec(name, pa.string(), lambda f, i: f.phone_number())
    if _matches(n, "address"):
        return FieldSpec(name, pa.string(), lambda f, i: f.address().replace("\n", ", "))
    if _matches(n, "city"):
        return FieldSpec(name, pa.string(), lambda f, i: f.city())
    if _matches(n, "state", "province"):
        return FieldSpec(name, pa.string(), lambda f, i: f.state())
    if _matches(n, "country"):
        return FieldSpec(name, pa.string(), lambda f, i: f.country())
    if _matches(n, "zip", "postal"):
        return FieldSpec(name, pa.string(), lambda f, i: f.postcode())
    if _matches(n, "company", "employer", "organization", "organisation"):
        return FieldSpec(name, pa.string(), lambda f, i: f.company())
    if _matches(n, "job", "title", "role", "position"):
        return FieldSpec(name, pa.string(), lambda f, i: f.job())
    if _matches(n, "url", "website", "link"):
        return FieldSpec(name, pa.string(), lambda f, i: f.url())
    if _matches(n, "date", "created", "updated", "timestamp", "dob", "birth"):
        return FieldSpec(name, pa.string(), lambda f, i: f.date_time_this_decade().isoformat())
    if _matches(n, "price", "amount", "salary", "cost", "revenue", "total", "balance"):
        return FieldSpec(name, pa.float64(), lambda f, i: float(f.pydecimal(left_digits=4, right_digits=2, positive=True)))
    if _matches(n, "age"):
        return FieldSpec(name, pa.int64(), lambda f, i: f.random_int(min=18, max=90))
    if _matches(n, "quantity", "qty", "count", "stock", "inventory"):
        return FieldSpec(name, pa.int64(), lambda f, i: f.random_int(min=1, max=1000))
    if n.startswith(("is_", "has_")) or _matches(n, "active", "enabled", "verified", "flag"):
        return FieldSpec(name, pa.bool_(), lambda f, i: f.boolean())
    if _matches(n, "description", "notes", "comment", "bio", "summary", "text"):
        return FieldSpec(name, pa.string(), lambda f, i: f.sentence())
    if _matches(n, "category", "type", "status", "tag", "label"):
        return FieldSpec(name, pa.string(), lambda f, i: f.word())

    return FieldSpec(name, pa.string(), lambda f, i: f.word())


def build_field_specs(field_names: list[str]) -> list[FieldSpec]:
    return [resolve_field(name) for name in field_names]


def generate_rows(specs: list[FieldSpec], start_index: int, count: int, faker: Faker) -> dict[str, list]:
    columns: dict[str, list] = {spec.name: [] for spec in specs}
    for i in range(start_index, start_index + count):
        for spec in specs:
            columns[spec.name].append(spec.generate(faker, i))
    return columns
