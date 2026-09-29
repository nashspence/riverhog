"""Exact lexical sidecar candidates from accepted core provenance locator facts."""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance_contracts import (
    SOURCE_NAMING_VIEW_SCHEME,
    require_canonical_uuid_urn,
)

Status = Literal["usable", "no-locator", "insufficient", "unsupported", "ambiguous"]
Rule = Literal["full-leaf", "stem"]


class UnsupportedLocator(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class LocatorEvidence:
    subject_id: str
    locator: Mapping[str, Any]
    context_endpoint: Mapping[str, Any]
    context_identifiers: Sequence[Mapping[str, Any]]
    context_support: Mapping[str, Any]
    locator_support: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class SourceStatus:
    subject_id: str
    status: Status
    support: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class FilenameCandidate:
    primary_id: str
    sidecar_id: str
    rule: Rule
    support: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True, slots=True)
class _ParsedName:
    syntax: str
    root: bytes
    parent: tuple[bytes, ...]
    leaf: bytes
    width: int


@dataclass(frozen=True, slots=True)
class _Selected:
    view_id: str | None
    context_identity: bytes
    name: _ParsedName
    support: tuple[Mapping[str, Any], ...]


def _name_bytes(name: Mapping[str, Any], syntax: str) -> bytes:
    if name.get("kind") == "text":
        text = name.get("text")
        if not isinstance(text, str):
            raise UnsupportedLocator("filesystem name text is invalid")
        return text.encode("utf-8" if syntax == "posix" else "utf-16le", "surrogatepass")
    if name.get("kind") != "bytes":
        raise UnsupportedLocator("filesystem name representation is unsupported")
    encoding = name.get("encoding")
    if encoding not in (("utf-8", "posix-bytes") if syntax == "posix" else ("utf-16le",)):
        raise UnsupportedLocator("filesystem name encoding is unsupported")
    data = name.get("bytes")
    if not isinstance(data, Mapping) or not isinstance(data.get("data"), str):
        raise UnsupportedLocator("filesystem name bytes are invalid")
    try:
        raw = base64.b64decode(data["data"], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise UnsupportedLocator("filesystem name base64 is invalid") from exc
    if base64.b64encode(raw).decode("ascii") != data["data"] or data.get("byte_length") != str(
        len(raw)
    ):
        raise UnsupportedLocator("filesystem name byte commitment differs")
    return raw


def split_locator(locator: Mapping[str, Any]) -> _ParsedName:
    """Preserve exact root and component units; never normalize aliases or case."""

    if locator.get("kind") != "filesystem_path" or locator.get("form") != "absolute":
        raise UnsupportedLocator("locator is not an absolute filesystem path")
    syntax = locator.get("syntax")
    if syntax not in {"posix", "windows"} or not isinstance(locator.get("name"), Mapping):
        raise UnsupportedLocator("filesystem path syntax is unsupported")
    raw = _name_bytes(locator["name"], syntax)
    if syntax == "posix":
        if not raw.startswith(b"/") or b"\0" in raw:
            raise UnsupportedLocator("absolute POSIX spelling is invalid")
        root = b"//" if raw.startswith(b"//") and not raw.startswith(b"///") else b"/"
        parts = tuple(raw[len(root) :].split(b"/"))
        width = 1
    else:
        if len(raw) % 2:
            raise UnsupportedLocator("Windows UTF-16 spelling has an odd byte count")
        text = raw.decode("utf-16le", "surrogatepass")
        if "\0" in text or "/" in text:
            raise UnsupportedLocator("Windows spelling is unsupported")
        ordinary = re.match(r"^([A-Za-z]:\\)(.*)$", text)
        unc = re.match(r"^(\\\\(?![?.]\\)[^\\:]+\\[^\\:]+\\)(.*)$", text)
        extended = re.match(
            r"^(\\\\\?\\(?:[A-Za-z]:\\|UNC\\[^\\:]+\\[^\\:]+\\|Volume\{[0-9A-Fa-f-]+\}\\))(.*)$",
            text,
        )
        match = extended or ordinary or unc
        if match is None:
            raise UnsupportedLocator("Windows root is unsupported")
        root = match[1].encode("utf-16le", "surrogatepass")
        strings = match[2].split("\\")
        if any(":" in part for part in strings):
            raise UnsupportedLocator("Windows alternate streams are unsupported")
        parts = tuple(part.encode("utf-16le", "surrogatepass") for part in strings)
        width = 2
    dot = b"." if width == 1 else b".\0"
    if any(not part or part in (dot, dot * 2) for part in parts):
        raise UnsupportedLocator("empty or unresolved path component")
    return _ParsedName(syntax, root, parts[:-1], parts[-1], width)


def _view_id(identifiers: Sequence[Mapping[str, Any]]) -> str | None:
    matches = [row for row in identifiers if row.get("scheme") == SOURCE_NAMING_VIEW_SCHEME]
    if not matches:
        return None
    values: set[str] = set()
    for row in matches:
        value = row.get("value")
        if (
            row.get("scope") != "global"
            or not isinstance(value, Mapping)
            or value.get("kind") != "text"
            or not isinstance(value.get("text"), str)
        ):
            raise UnsupportedLocator("source naming view identifier is malformed")
        try:
            values.add(require_canonical_uuid_urn(value["text"], "source naming view"))
        except (TypeError, ValueError) as exc:
            raise UnsupportedLocator("source naming view identifier is malformed") from exc
    if len(values) != 1:
        raise ValueError("source naming view identifiers conflict")
    return next(iter(values))


def _unique_support(rows: Sequence[Mapping[str, Any]]) -> tuple[Mapping[str, Any], ...]:
    by_identity = {canonical_json_bytes(row): row for row in rows}
    return tuple(by_identity[key] for key in sorted(by_identity))


def _select(
    subject_id: str, locators: Sequence[LocatorEvidence]
) -> tuple[SourceStatus, _Selected | None]:
    support = _unique_support(
        tuple(item for row in locators for item in (row.context_support, row.locator_support))
    )
    if not locators:
        return SourceStatus(subject_id, "no-locator", ()), None
    parsed: list[_Selected] = []
    unsupported = False
    conflict = False
    for row in locators:
        if row.subject_id != subject_id:
            raise ValueError("locator subject differs from its declared partition")
        try:
            view_id = _view_id(row.context_identifiers)
            name = split_locator(row.locator)
        except UnsupportedLocator:
            unsupported = True
            continue
        except ValueError:
            conflict = True
            continue
        parsed.append(
            _Selected(
                view_id,
                canonical_json_bytes(row.context_endpoint),
                name,
                _unique_support((row.context_support, row.locator_support)),
            )
        )
    if conflict:
        return SourceStatus(subject_id, "ambiguous", support), None
    if not parsed:
        return SourceStatus(subject_id, "unsupported", support), None
    if unsupported:
        return SourceStatus(subject_id, "unsupported", support), None
    if {item.view_id is None for item in parsed} == {True, False}:
        return SourceStatus(subject_id, "insufficient", support), None
    scoped = parsed[0].view_id is not None
    keys = {
        (
            item.view_id if scoped else item.context_identity,
            item.name.syntax,
            item.name.root,
            item.name.parent,
            item.name.leaf,
        )
        for item in parsed
    }
    if len(keys) != 1:
        return SourceStatus(subject_id, "ambiguous", support), None
    selected = parsed[0]
    return (
        SourceStatus(subject_id, "usable" if scoped else "insufficient", support),
        _Selected(
            selected.view_id,
            selected.context_identity,
            selected.name,
            _unique_support(tuple(item for row in parsed for item in row.support)),
        ),
    )


def _stem(leaf: bytes, width: int) -> bytes:
    units = [leaf[index : index + width] for index in range(0, len(leaf), width)]
    dot = b"." if width == 1 else b".\0"
    at = max((index for index, unit in enumerate(units) if unit == dot), default=-1)
    return leaf[: at * width] if 0 < at < len(units) - 1 else leaf


def _comparable(left: _Selected, right: _Selected) -> bool:
    if left.name.syntax != right.name.syntax:
        return False
    if left.name.root != right.name.root or left.name.parent != right.name.parent:
        return False
    if left.view_id is not None and right.view_id is not None and left.view_id != right.view_id:
        return False
    if left.context_identity == right.context_identity:
        return True
    return left.view_id is not None and left.view_id == right.view_id


def compare_filenames(
    facts: Mapping[str, Sequence[LocatorEvidence]],
    *,
    primary_ids: Sequence[str],
    sidecar_ids: Sequence[str],
    sidecar_suffix: str,
) -> tuple[tuple[SourceStatus, ...], tuple[FilenameCandidate, ...]]:
    """Report all candidates and complete/insufficient source comparison status."""

    primary = tuple(primary_ids)
    sidecar = tuple(sidecar_ids)
    if (
        len(set(primary)) != len(primary)
        or len(set(sidecar)) != len(sidecar)
        or set(primary) & set(sidecar)
        or set(primary) | set(sidecar) != set(facts)
    ):
        raise ValueError("filename candidate partitions must be exact and disjoint")
    if re.fullmatch(r"\.[A-Za-z0-9_-]{1,32}", sidecar_suffix) is None:
        raise ValueError("filename sidecar suffix must be explicit bounded ASCII")
    statuses: list[SourceStatus] = []
    selected: dict[str, _Selected] = {}
    for subject_id in sorted(facts):
        status, choice = _select(subject_id, facts[subject_id])
        statuses.append(status)
        if choice is not None:
            selected[subject_id] = choice
    candidates: list[FilenameCandidate] = []
    for sidecar_id in sidecar:
        sidecar_name = selected.get(sidecar_id)
        if sidecar_name is None:
            continue
        suffix = sidecar_suffix.encode("utf-8" if sidecar_name.name.width == 1 else "utf-16le")
        leaf = sidecar_name.name.leaf
        if not leaf.endswith(suffix) or len(leaf) == len(suffix):
            continue
        base = leaf[: -len(suffix)]
        for primary_id in primary:
            primary_name = selected.get(primary_id)
            if primary_name is None or not _comparable(sidecar_name, primary_name):
                continue
            if base == primary_name.name.leaf:
                rule: Rule = "full-leaf"
            elif base == _stem(primary_name.name.leaf, primary_name.name.width):
                rule = "stem"
            else:
                continue
            candidates.append(
                FilenameCandidate(
                    primary_id,
                    sidecar_id,
                    rule,
                    _unique_support((*primary_name.support, *sidecar_name.support)),
                )
            )
    return tuple(statuses), tuple(
        sorted(candidates, key=lambda row: (row.sidecar_id, row.primary_id, row.rule))
    )
