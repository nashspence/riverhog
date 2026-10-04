"""Compile exact Markdown source bytes against native, source-owned obligations."""

from __future__ import annotations

import copy
import hashlib
import posixpath
import re
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any, cast
from urllib.parse import unquote, urlsplit

import yaml
from markdown_it import MarkdownIt
from markdown_it.token import Token
from markdown_it.tree import SyntaxTreeNode
from release_documentation_lib import markdown_text

from .documentation_requirements import REQUIREMENTS_FORMAT, target_key
from .model import ContractAtlasError, canonical_bytes

SOURCE = "riverhog-release-documentation-document/v1"
HEX = re.compile(r"[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
Invalid = ContractAtlasError


def need(ok: bool, reason: str) -> None:
    if not ok:
        raise Invalid(reason)


def report_bytes(value: Any) -> bytes:
    return canonical_bytes(value)


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def text(value: Any, *, line: bool = False) -> str:
    need(isinstance(value, str) and bool(value.strip()), "empty/non-text prose")
    need(
        not any(
            (ord(c) < 32 and c not in "\n\r\t")
            or 127 <= ord(c) <= 159
            or 0x202A <= ord(c) <= 0x202E
            or 0x2066 <= ord(c) <= 0x2069
            for c in value
        ),
        "control sequence in prose",
    )
    need(not line or not any(c in value for c in "\n\r\t"), "summary must be one line")
    return cast(str, value)


def path_name(value: Any) -> str:
    need(
        isinstance(value, str)
        and bool(value)
        and "\\" not in value
        and not any(ord(c) < 32 for c in value),
        "invalid corpus path",
    )
    p = PurePosixPath(value)
    need(
        not p.is_absolute() and all(part not in {"", ".", ".."} for part in value.split("/")),
        "unsafe corpus path",
    )
    need(not any(part.casefold() == ".git" for part in p.parts), "Git path in corpus")
    return cast(str, value)


def target(value: Any) -> str:
    need(isinstance(value, dict), "documentation target must be a mapping")
    return target_key(value)


def front_matter(
    payload: bytes, *, locations: dict[str, int] | None = None
) -> tuple[dict[str, Any], str]:
    """Strict string/list/map YAML; no implicit booleans, tags, aliases or merge keys."""
    need(isinstance(payload, bytes), "document is not bytes")
    try:
        source = payload.decode("utf-8")
    except UnicodeError as exc:
        raise Invalid("document is not UTF-8") from exc
    lines = source.splitlines(keepends=True)
    need(bool(lines) and lines[0].rstrip("\r\n") == "---", "Markdown needs leading front matter")
    end = next((i for i in range(1, len(lines)) if lines[i].rstrip("\r\n") == "---"), None)
    need(end is not None, "front matter lacks closing delimiter")
    assert end is not None
    header = "".join(lines[1:end])
    need(len(header.encode()) <= 65536, "front-matter operational budget exceeded")
    try:
        for token in yaml.scan(header, Loader=yaml.BaseLoader):
            need(
                not isinstance(
                    token,
                    (
                        yaml.tokens.AliasToken,
                        yaml.tokens.AnchorToken,
                        yaml.tokens.TagToken,
                        yaml.tokens.DirectiveToken,
                    ),
                ),
                f"line {token.start_mark.line + 2}: field front matter: "
                "YAML aliases, anchors, tags and directives are forbidden",
            )
        node = yaml.compose(header, Loader=yaml.BaseLoader)
    except (yaml.YAMLError, RecursionError) as exc:
        mark = getattr(exc, "problem_mark", None)
        line = 2 if mark is None else mark.line + 2
        raise Invalid(f"line {line}: field front matter: invalid bounded YAML") from exc

    marks = {} if locations is None else locations

    def convert(item: Any, depth: int = 0, field: str = "") -> Any:
        marks.setdefault(field, item.start_mark.line + 2 if item is not None else 2)
        need(depth <= 16, "front-matter nesting budget exceeded")
        if isinstance(item, yaml.ScalarNode):
            return item.value
        if isinstance(item, yaml.SequenceNode):
            return [
                convert(child, depth + 1, f"{field}[{i}]") for i, child in enumerate(item.value)
            ]
        need(isinstance(item, yaml.MappingNode), "front matter must use string/list/map values")
        result = {}
        for key_node, value_node in item.value:
            need(isinstance(key_node, yaml.ScalarNode), "YAML keys must be strings")
            key = key_node.value
            child_field = field + "." + key if field else key
            need(
                key != "<<" and key not in result,
                f"line {key_node.start_mark.line + 2}: field {child_field}: "
                "duplicate or merge YAML key",
            )
            marks[child_field] = key_node.start_mark.line + 2
            result[key] = convert(value_node, depth + 1, child_field)
        return result

    metadata = convert(node)
    if isinstance(metadata, dict):
        unknown = set(metadata) - {"format", "id", "kind", "title", "subjects"}
        if unknown:
            field = sorted(unknown)[0]
            raise Invalid(f"line {marks[field]}: field {field}: unknown front-matter field")
    need(isinstance(metadata, dict), "line 2: field front matter: expected a mapping")
    for field in ("format", "id", "kind", "title", "subjects"):
        need(field in metadata, f"line 2: field {field}: required front-matter field is absent")

    def check_field(ok: bool, field: str, reason: str) -> None:
        need(ok, f"line {marks[field]}: field {field}: {reason}")

    check_field(metadata["format"] == SOURCE, "format", "unsupported document format")
    check_field(
        isinstance(metadata["kind"], str) and metadata["kind"] in {"reference", "guide"},
        "kind",
        "expected reference or guide",
    )
    check_field(
        isinstance(metadata["id"], str)
        and re.fullmatch(r"[a-z][a-z0-9-]*", metadata["id"]) is not None,
        "id",
        "expected a stable lowercase document ID",
    )
    check_field(
        isinstance(metadata["subjects"], list) and bool(metadata["subjects"]),
        "subjects",
        "expected a nonempty list of exact subjects",
    )
    try:
        text(metadata["title"], line=True)
    except Invalid as exc:
        raise Invalid(f"line {marks['title']}: field title: {exc}") from exc
    marks["body"] = end + 2
    return metadata, "".join(lines[end + 1 :])


def obligations(requirements: dict[str, Any]) -> dict[str, dict[str, Any]]:
    need(
        requirements.get("format") == REQUIREMENTS_FORMAT
        and isinstance(requirements.get("subjects"), dict),
        "unknown native documentation requirements",
    )
    rows = cast(dict[str, dict[str, Any]], requirements["subjects"])
    for key, row in rows.items():
        need(
            target(row["target"]) == key and row["rule"] in {"authored", "structural", "reference"},
            "duplicate or unclassified native obligation",
        )
        if row["rule"] == "reference":
            need(target(row["canonical"]) in rows, "unresolved source canonical relationship")
    return rows


def validate_asset(name: str, payload: bytes) -> None:
    """PNG only: bounded raster bytes, no SVG, active content or trailing payload."""
    import struct
    import zlib

    need(
        name.endswith(".png") and payload.startswith(b"\x89PNG\r\n\x1a\n"),
        "permitted assets are PNG raster images",
    )
    need(len(payload) <= 8_000_000, "asset operational byte budget exceeded")
    offset, width, height, ended, first = 8, 0, 0, False, True
    while offset + 12 <= len(payload):
        length = struct.unpack(">I", payload[offset : offset + 4])[0]
        kind = payload[offset + 4 : offset + 8]
        stop = offset + 8 + length
        need(stop + 4 <= len(payload), "truncated PNG asset")
        data = payload[offset + 8 : stop]
        crc = struct.unpack(">I", payload[stop : stop + 4])[0]
        need(zlib.crc32(kind + data) == crc, "PNG asset checksum differs")
        if first:
            need(kind == b"IHDR" and length == 13, "PNG asset lacks its image header")
            width, height = struct.unpack(">II", data[:8])
            need(
                width > 0 and height > 0 and width * height <= 32_000_000,
                "asset operational pixel budget exceeded",
            )
            first = False
        if kind == b"IEND":
            need(length == 0 and stop + 4 == len(payload), "PNG asset has trailing bytes")
            ended = True
            break
        offset = stop + 4
    need(ended, "PNG asset has no complete terminator")


def native_markdown(tokens: list[Token]) -> str:
    """Deterministic CommonMark projection; links come from validated token attributes."""

    def inline(token: Token) -> str:
        parts, links = [], []
        for child in token.children or []:
            if child.type == "text":
                parts.append(markdown_text(child.content))
            elif child.type == "code_inline":
                fence = "`" * max(
                    1,
                    max((len(run) + 1 for run in re.findall(r"`+", child.content)), default=1),
                )
                padding = " " if child.content.strip() else ""
                parts.append(fence + padding + child.content + padding + fence)
            elif child.type == "softbreak":
                parts.append("\n")
            elif child.type == "hardbreak":
                parts.append("  \n")
            elif child.type in {"em_open", "em_close", "strong_open", "strong_close"}:
                parts.append(child.markup)
            elif child.type == "link_open":
                parts.append("[")
                links.append((child.attrGet("href"), child.attrGet("title")))
            elif child.type == "link_close":
                href, title = links.pop()
                suffix = (
                    ' "' + str(title).replace("\\", "\\\\").replace('"', '\\"') + '"'
                    if title is not None
                    else ""
                )
                parts.append("](<" + str(href) + ">" + suffix + ")")
            elif child.type == "image":
                alternative = markdown_text(child.content)
                parts.append("![" + alternative + "](<" + str(child.attrGet("src")) + ">)")
        return "".join(parts)

    def blocks(nodes: list[SyntaxTreeNode], *, tight: bool = False) -> str:
        parts = []
        for node in nodes:
            if node.type in {"paragraph", "heading"}:
                token = node.to_tokens()[1]
                text = inline(token)
                parts.append(
                    ("#" * int(node.tag[1:]) + " " if node.type == "heading" else "") + text
                )
            elif node.type in {"bullet_list", "ordered_list"}:
                items = []
                start = int(node.attrs.get("start", 1))
                for index, item in enumerate(node.children):
                    marker = f"{start + index}. " if node.type == "ordered_list" else "- "
                    lines = blocks(
                        item.children,
                        tight=any(
                            child.type == "paragraph" and child.hidden for child in item.children
                        ),
                    ).splitlines()
                    items.append(
                        marker
                        + (lines[0] if lines else "")
                        + "\n"
                        + "\n".join(" " * len(marker) + line if line else "" for line in lines[1:])
                    )
                parts.append("\n".join(items))
            elif node.type == "blockquote":
                parts.append("\n".join("> " + line for line in blocks(node.children).splitlines()))
            elif node.type == "hr":
                parts.append("---")
            elif node.type in {"fence", "code_block"}:
                marker = "~" if "`" in node.info else "`"
                fence = marker * max(
                    3,
                    max(
                        (len(run) + 1 for run in re.findall(re.escape(marker) + "+", node.content)),
                        default=3,
                    ),
                )
                content = node.content + ("" if node.content.endswith("\n") else "\n")
                parts.append(fence + node.info + "\n" + content + fence)
            else:
                raise Invalid("unsupported native CommonMark block: " + node.type)
        return ("\n" if tight else "\n\n").join(parts)

    return blocks(SyntaxTreeNode(tokens).children).strip() + "\n"


def document_route(identifier: str) -> str:
    return "d-" + digest(identifier.encode())[:24] + ".html"


def selected_content(compiled: Mapping[str, Any], entry: Mapping[str, Any]) -> dict[str, Any]:
    """An exact selected section is stored once per document, never copied into every binding."""
    return (
        compiled["pages"][entry["source_path"]]["sections"][entry["body"]]
        if "body" in entry
        else {}
    )


def plain_tokens(tokens: list[Token]) -> str:
    """Readable terminal prose retains working links and literal fenced examples."""
    paragraphs = []
    for token in tokens:
        if token.type in {"fence", "code_block"}:
            paragraphs.append(token.content.rstrip())
        elif token.type == "inline":
            words, links = [], []
            for child in token.children or []:
                if child.type in {"text", "code_inline"}:
                    words.append(child.content)
                elif child.type in {"softbreak", "hardbreak"}:
                    words.append("\n")
                elif child.type == "link_open":
                    links.append(str(child.attrGet("href")))
                elif child.type == "link_close":
                    words.append(" <" + links.pop() + ">")
                elif child.type == "image":
                    words.append(child.content + " <" + str(child.attrGet("src")) + ">")
            paragraphs.append("".join(words))
    return "\n\n".join(paragraphs)


def _render_pages(
    bodies: Mapping[str, str],
    rows: dict[str, Any],
    routes: Mapping[str, str] | None,
    documents: dict[str, Any],
    assets: dict[str, Any],
) -> dict[str, Any]:
    class CheckedMarkdown(MarkdownIt):
        def validateLink(self, url: str) -> bool:
            return True

    # Recognize unsafe syntax, then REJECT it; never return raw HTML tokens.
    md = CheckedMarkdown("commonmark", {"html": True})
    pages, anchors, links = {}, {}, []
    for name, source in sorted(bodies.items()):
        text(source) if source.strip() else None
        tokens = md.parse(source)
        headings = {}
        for i, token in enumerate(tokens):
            need(token.type not in {"html_block", "html_inline"}, "raw HTML is not authored markup")
            if token.type == "heading_open":
                slug = (
                    re.sub(r"[^\w-]+", "-", tokens[i + 1].content.casefold()).strip("-")
                    or "section"
                )
                need(slug not in headings, "duplicate/colliding heading IDs")
                headings[slug] = (i, int(token.tag[1:]))
                token.attrSet("id", "doc-" + slug)
            for child in token.children or []:
                need(child.type != "html_inline", "raw HTML is not authored markup")
                if child.type == "text":
                    need(
                        "{{" not in child.content
                        and "{%" not in child.content
                        and not child.content.startswith(("::: ", "!include ")),
                        "templates and executable directives are forbidden",
                    )
                if child.type not in {"link_open", "image"}:
                    continue
                href = str(child.attrGet("src" if child.type == "image" else "href") or "")
                need(not any(ord(c) < 32 for c in href) and "\\" not in href, "unsafe link bytes")
                parts = urlsplit(href)
                if parts.scheme == "contract":
                    need(child.type == "link_open", "contract links cannot be images")
                    element = unquote(parts.path)
                    need(
                        not parts.netloc
                        and not parts.query
                        and not parts.fragment
                        and target({"element_id": element, "pointer": ""}) in rows,
                        "unresolved contract link",
                    )
                    need(
                        routes is not None and element in routes,
                        "source renderer route is required",
                    )
                    assert routes is not None
                    child.attrSet("href", routes[element])
                    continue
                if parts.scheme or parts.netloc:
                    need(child.type == "link_open", "remote images are forbidden")
                    need(
                        (parts.scheme == "https" and bool(parts.netloc))
                        or (parts.scheme == "mailto" and bool(parts.path)),
                        "unsafe link scheme",
                    )
                    continue
                need(not parts.query, "local documentation links have no query")
                dest = (
                    posixpath.normpath(posixpath.join(posixpath.dirname(name), unquote(parts.path)))
                    if parts.path
                    else name
                )
                path_name(dest)
                need(dest in bodies or dest in assets, "unresolved local documentation link")
                if child.type == "image":
                    need(
                        dest in assets and not parts.fragment,
                        "image must identify a captured asset",
                    )
                fragment = unquote(parts.fragment)
                links.append((dest, fragment))
                attribute = "src" if child.type == "image" else "href"
                route = (
                    document_route(documents[dest]["id"])
                    if dest in bodies
                    else "documentation-assets/" + dest
                )
                child.attrSet(attribute, route + ("#doc-" + fragment if fragment else ""))
        anchors[name] = set(headings)

        def render(
            start: int, stop: int, *, tokens: list[Token] = tokens, source: str = source
        ) -> dict[str, Any]:
            selected = tokens[start:stop]
            lines = source.splitlines(keepends=True)
            first = selected[0].map[0] if selected and selected[0].map else 0
            next_map = tokens[stop].map if stop < len(tokens) else None
            last = next_map[0] if next_map is not None else len(lines)
            return {
                "markdown": native_markdown(selected),
                "source_markdown": "".join(lines[first:last]),
                "html": md.renderer.render(selected, md.options, {}),
                "plain": plain_tokens(selected),
            }

        sections = {"#": render(0, len(tokens))}
        for slug, (start, level) in headings.items():
            stop = next(
                (
                    i
                    for i in range(start + 1, len(tokens))
                    if tokens[i].type == "heading_open" and int(tokens[i].tag[1:]) <= level
                ),
                len(tokens),
            )
            sections["#" + slug] = render(start, stop)
        pages[name] = {**sections["#"], "sections": sections}
    for dest, fragment in links:
        need(not fragment or fragment in anchors.get(dest, set()), "unresolved Markdown fragment")
    return pages


def compile_corpus(
    files: Mapping[str, bytes],
    requirements: dict[str, Any],
    *,
    final: bool = False,
    routes: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Compile all front-matter Markdown. Complete coverage is NOT human approval."""
    captured = dict(files)
    need(
        len(captured) <= 10000
        and all(isinstance(v, bytes) for v in captured.values())
        and sum(map(len, captured.values())) <= 32_000_000,
        "documentation corpus operational budget exceeded",
    )
    rows = obligations(requirements)
    documents, bodies, seen_paths, seen_ids, entries, guides, assets = (
        {},
        {},
        set(),
        set(),
        {},
        [],
        {},
    )
    for name, payload in sorted(captured.items()):
        path_name(name)
        need(name.endswith((".md", ".png")), "corpus has an index/review stamp or unsupported file")
        need(name.casefold() not in seen_paths, "case-colliding corpus paths")
        seen_paths.add(name.casefold())
        if name.endswith(".png"):
            validate_asset(name, payload)
            assets[name] = {"sha256": digest(payload), "size": len(payload)}
            continue
        try:
            locations: dict[str, int] = {}
            document, body = front_matter(payload, locations=locations)
        except Invalid as exc:
            raise Invalid(f"{name}: {exc}") from exc
        need(
            document["id"] not in seen_ids,
            f"{name}: line {locations['id']}: field id: duplicate document ID",
        )
        seen_ids.add(document["id"])
        documents[name], bodies[name] = document, body
        if document["kind"] == "guide":
            keys = []
            for index, item in enumerate(document["subjects"]):
                field = f"subjects[{index}]"
                try:
                    keys.append(target(item))
                    need(keys[-1] in rows, "unresolved guide subject")
                except Invalid as exc:
                    raise Invalid(f"{name}: line {locations[field]}: field {field}: {exc}") from exc
            need(
                len(keys) == len(set(keys)) and set(keys) <= set(rows),
                "unresolved/duplicate guide subjects",
            )
            guides.append(
                {
                    "id": document["id"],
                    "title": document["title"],
                    "body": name,
                    "subjects": copy.deepcopy(document["subjects"]),
                }
            )
            continue
        for index, entry in enumerate(document["subjects"]):
            field = f"subjects[{index}]"
            diagnostic = f"{name}: line {locations[field]}: field {field}: "
            need(
                isinstance(entry, dict)
                and set(entry) <= {"target", "summary", "body"}
                and {"target", "summary"} <= set(entry),
                diagnostic + "unknown editorial fields",
            )
            try:
                key = target(entry["target"])
            except Invalid as exc:
                raise Invalid(diagnostic + "target: " + str(exc)) from exc
            need(
                key in rows and key not in entries and rows[key]["rule"] == "authored",
                diagnostic + "target: unknown, duplicate, unowned or non-authorable target",
            )
            need(isinstance(entry["summary"], str), diagnostic + "summary must be a string")
            if entry["summary"].strip():
                try:
                    text(entry["summary"], line=True)
                except Invalid as exc:
                    summary_field = field + ".summary"
                    raise Invalid(
                        f"{name}: line {locations[summary_field]}: field {summary_field}: {exc}"
                    ) from exc
            if "body" in entry:
                need(
                    isinstance(entry["body"], str) and entry["body"].startswith("#"),
                    diagnostic + "body selects this document or one heading, not another file",
                )
            entries[key] = {
                **copy.deepcopy(entry),
                "document_id": document["id"],
                "source_path": name,
                "_body_line": locations.get(field + ".body", locations[field]),
                "_field": field + ".body",
            }
    pages = _render_pages(bodies, rows, routes, documents, assets)
    for entry in entries.values():
        if "body" in entry:
            need(
                entry["body"] in pages[entry["source_path"]]["sections"],
                f"{entry['source_path']}: line {entry['_body_line']}: "
                f"field {entry['_field']}: unresolved body fragment",
            )
        entry.pop("_body_line")
        entry.pop("_field")

    def resolve(key: str, stack: tuple[str, ...] = ()) -> dict[str, Any] | None:
        need(key not in stack, "cyclic canonical documentation relationship")
        row = rows[key]
        if row["rule"] == "structural":
            return None
        if row["rule"] == "reference":
            donor = target(row["canonical"])
            need(rows[donor]["rule"] != "structural", "reference targets a structural facet")
            return resolve(donor, (*stack, key))
        entry = cast(dict[str, Any] | None, entries.get(key))
        return entry if entry is not None and entry["summary"].strip() else None

    missing, resolved = [], {}
    for key, row in sorted(rows.items()):
        entry = resolve(key)
        if row["rule"] != "structural" and (
            entry is None or (row["detail"] and "body" not in entry)
        ):
            missing.append(row["target"])
        elif entry is not None:
            resolved[key] = entry
    ledger = {
        name: {"sha256": digest(data), "size": len(data)} for name, data in sorted(captured.items())
    }
    if final:
        need(not missing, "release has unresolved documentation requirements")
    return {
        "format": "riverhog-documentation-compiled/v1",
        "resolved": resolved,
        "pages": pages,
        "documents": {
            name: {key: value for key, value in metadata.items() if key != "subjects"}
            for name, metadata in documents.items()
        },
        "guides": guides,
        "assets": assets,
        "source_files": ledger,
        "inputs": {
            "closure_sha256": requirements["closure_sha256"],
            "requirements_sha256": digest(report_bytes(requirements)),
            "corpus_sha256": digest(report_bytes(ledger)),
        },
        "coverage": {
            "total": len(rows),
            "missing": missing,
            "structural": sum(r["rule"] == "structural" for r in rows.values()),
        },
    }
