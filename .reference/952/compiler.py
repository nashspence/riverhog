"""#952 reference: compile a captured prose corpus against SOURCE-owned obligations.

Not a production discovery engine or release signer. The producer supplies the full
obligation ledger. Report encoding below is reference-local, NOT Riverhog's JCS codec.
Front matter is the sole authored binding. No network, templates, or approval stamping.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import inspect
import json
import posixpath
import re
import subprocess

import yaml
from pathlib import PurePosixPath
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt

SOURCE = "riverhog-release-documentation-document/v1"
HEX = re.compile(r"[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
INTERFACES = frozenset({
    "artifact-verification", "cli", "compatibility-guarantees", "configuration",
    "configuration-environment", "durable-state", "extent", "http-operations",
    "http-schemas", "http-service-declaration", "http-security-schemes",
    "installation-roots", "process-protocol", "process-protocol-operations",
    "process-protocol-schemas", "publication-locations", "publication-policies",
    "python", "python-distributions", "release-artifacts", "runtime-images",
    "schema", "versioning-tags",
})


class Invalid(ValueError):
    pass


def need(ok: bool, reason: str) -> None:
    if not ok:
        raise Invalid(reason)


def report_bytes(value: Any) -> bytes:
    """Deterministic REFERENCE report encoding; integration must use native JCS."""
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                       separators=(",", ":")) + "\n").encode()


def digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def read_json(payload: bytes) -> Any:
    def pairs(items):
        result = {}
        for key, value in items:
            need(key not in result, "duplicate JSON member")
            result[key] = value
        return result
    def constant(value):
        raise Invalid("nonfinite JSON number")
    try:
        return json.loads(payload.decode("utf-8"), object_pairs_hook=pairs,
                          parse_constant=constant)
    except (UnicodeError, ValueError) as exc:
        raise Invalid(f"invalid JSON: {exc}") from exc


def text(value: Any, *, line: bool = False) -> str:
    need(isinstance(value, str) and bool(value.strip()), "empty/non-text prose")
    need(not any((ord(c) < 32 and c not in "\n\r\t") or ord(c) == 127
                 or 0x202A <= ord(c) <= 0x202E or 0x2066 <= ord(c) <= 0x2069
                 for c in value), "control sequence in prose")
    need(not line or not any(c in value for c in "\n\r\t"), "summary must be one line")
    return value


def path_name(value: Any) -> str:
    need(isinstance(value, str) and bool(value) and "\\" not in value
         and not any(ord(c) < 32 for c in value), "invalid corpus path")
    p = PurePosixPath(value)
    need(not p.is_absolute() and all(part not in {"", ".", ".."}
                                   for part in value.split("/")), "unsafe corpus path")
    need(not any(part.casefold() == ".git" for part in p.parts), "Git path in corpus")
    return value


def target(value: Any) -> str:
    """A root, an object-key pointer, or a typed native named member; never a slug."""
    need(isinstance(value, dict) and isinstance(value.get("element_id"), str)
         and bool(value["element_id"]), "missing existing element identity")
    if set(value) == {"element_id", "pointer"}:
        pointer = value["pointer"]
        need(isinstance(pointer, str) and (not pointer or pointer.startswith("/")), "bad pointer")
        need(re.search(r"~(?![01])", pointer) is None, "bad pointer escaping")
    else:
        need(set(value) == {"element_id", "member"}, "unknown target fields")
        member = value["member"]
        need(isinstance(member, dict) and set(member) == {"kind", "key"}
             and member["kind"] in {"cli-parameter", "python-parameter", "python-result"}
             and isinstance(member["key"], str) and bool(member["key"]), "bad named member")
    return report_bytes(value).decode().strip()


def front_matter(payload: bytes) -> tuple[dict, str]:
    """Strict string/list/map YAML; no implicit booleans, tags, aliases or merge keys."""
    need(isinstance(payload, bytes), "document is not bytes")
    try:
        source = payload.decode("utf-8")
    except UnicodeError as exc:
        raise Invalid("document is not UTF-8") from exc
    lines = source.splitlines(keepends=True)
    need(lines and lines[0].rstrip("\r\n") == "---", "Markdown needs leading front matter")
    end = next((i for i in range(1, len(lines)) if lines[i].rstrip("\r\n") == "---"), None)
    need(end is not None, "front matter lacks closing delimiter")
    header = "".join(lines[1:end])
    need(len(header.encode()) <= 65536, "front-matter operational budget exceeded")
    try:
        for token in yaml.scan(header, Loader=yaml.BaseLoader):
            need(not isinstance(token, (yaml.tokens.AliasToken, yaml.tokens.AnchorToken,
                                         yaml.tokens.TagToken, yaml.tokens.DirectiveToken)),
                 "YAML aliases, anchors, tags and directives are forbidden")
        node = yaml.compose(header, Loader=yaml.BaseLoader)
    except (yaml.YAMLError, RecursionError) as exc:
        raise Invalid("invalid bounded YAML front matter") from exc
    def convert(item, depth=0):
        need(depth <= 16, "front-matter nesting budget exceeded")
        if isinstance(item, yaml.ScalarNode):
            return item.value
        if isinstance(item, yaml.SequenceNode):
            return [convert(child, depth + 1) for child in item.value]
        need(isinstance(item, yaml.MappingNode), "front matter must use string/list/map values")
        result = {}
        for key_node, value_node in item.value:
            need(isinstance(key_node, yaml.ScalarNode), "YAML keys must be strings")
            key = key_node.value
            need(key != "<<" and key not in result, "duplicate or merge YAML key")
            result[key] = convert(value_node, depth + 1)
        return result
    metadata = convert(node)
    need(isinstance(metadata, dict) and set(metadata) == {"format", "id", "kind", "title", "subjects"}
         and metadata["format"] == SOURCE and isinstance(metadata["kind"], str)
         and metadata["kind"] in {"reference", "guide"} and isinstance(metadata["id"], str)
         and re.fullmatch(r"[a-z][a-z0-9-]*", metadata["id"]) is not None
         and isinstance(metadata["subjects"], list) and bool(metadata["subjects"]),
         "invalid document metadata")
    text(metadata["title"], line=True)
    return metadata, "".join(lines[end + 1:])


def capture_git(repository: str, commit: str, version: str) -> dict[str, bytes]:
    """Capture the complete version subtree once; this runnable profile permits Markdown only."""
    need(bool(SHA.fullmatch(commit)), "exact documentation commit required")
    need(re.fullmatch(r"v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version) is not None,
         "exact final version subtree required")
    def git(*args):
        return subprocess.check_output(["git", "-C", repository, *args], stderr=subprocess.PIPE)
    files = {}
    for row in git("ls-tree", "-rz", commit, "--", version + "/").split(b"\0"):
        if not row:
            continue
        metadata, raw = row.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        name = raw.decode()
        need(name.startswith(version + "/") and mode == "100644" and kind == "blob",
             "corpus contains a non-regular source")
        relative = path_name(name[len(version) + 1:])
        need(relative.endswith(".md"), "reference capture permits Markdown only, not indexes/review stamps")
        files[relative] = git("cat-file", "blob", oid)
    need(bool(files), "selected subtree contains no documentation")
    return files


def obligations(requirements: dict) -> dict[str, dict]:
    """The source producer, not the documentation corpus, establishes completeness."""
    need(set(requirements) == {"format", "closure_sha256", "policy_sha256", "complete", "subjects"}
         and requirements["format"] == "riverhog-doc-requirements-reference/v1"
         and HEX.fullmatch(requirements["closure_sha256"]) is not None
         and HEX.fullmatch(requirements["policy_sha256"]) is not None
         and requirements["complete"] is True, "incomplete/unknown requirement ledger")
    need(isinstance(requirements["subjects"], list), "subject ledger must be a list")
    rows = {}
    for row in requirements["subjects"]:
        need(set(row) <= {"target", "interface", "authority", "rule", "detail", "canonical"}
             and {"target", "interface", "authority", "rule", "detail"} <= set(row), "bad obligation")
        key = target(row["target"])
        need(key not in rows and row["interface"] in INTERFACES
             and row["rule"] in {"authored", "structural", "reference"}
             and type(row["detail"]) is bool and isinstance(row["authority"], str),
             "duplicate/unclassified obligation")
        need(("canonical" in row) == (row["rule"] == "reference"), "bad canonical obligation")
        rows[key] = row
    for row in rows.values():
        if row["rule"] == "reference":
            need(target(row["canonical"]) in rows, "unresolved source canonical relationship")
    return rows


def _render_pages(bodies: Mapping[str, str], rows: dict, routes: Mapping[str, str] | None) -> dict:
    md = MarkdownIt("commonmark", {"html": True})
    # Recognize unsafe syntax, then REJECT it; never return raw HTML tokens.
    md.validateLink = lambda value: True
    pages, anchors, links = {}, {}, []
    for name, source in sorted(bodies.items()):
        source = text(source)
        tokens = md.parse(source)
        headings = {}
        for i, token in enumerate(tokens):
            need(token.type not in {"html_block", "html_inline"}, "raw HTML is not authored markup")
            if token.type == "heading_open":
                slug = re.sub(r"[^\w-]+", "-", tokens[i + 1].content.casefold()).strip("-") or "section"
                need(slug not in headings, "duplicate/colliding heading IDs")
                headings[slug] = (i, int(token.tag[1:]))
                token.attrSet("id", "doc-" + slug)
            for child in token.children or []:
                need(child.type not in {"html_inline", "image"}, "HTML/images unsupported by reference profile")
                if child.type != "link_open":
                    continue
                href = child.attrGet("href") or ""
                need(not any(ord(c) < 32 for c in href) and "\\" not in href, "unsafe link bytes")
                parts = urlsplit(href)
                if parts.scheme == "contract":
                    element = unquote(parts.path)
                    need(not parts.netloc and not parts.query and not parts.fragment
                         and target({"element_id": element, "pointer": ""}) in rows,
                         "unresolved contract link")
                    need(routes is not None and element in routes, "source renderer route is required")
                    child.attrSet("href", routes[element])
                    continue
                if parts.scheme or parts.netloc:
                    need((parts.scheme == "https" and bool(parts.netloc))
                         or (parts.scheme == "mailto" and bool(parts.path)), "unsafe link scheme")
                    continue
                need(not parts.query, "local documentation links have no query")
                dest = posixpath.normpath(posixpath.join(posixpath.dirname(name), unquote(parts.path))) if parts.path else name
                path_name(dest)
                need(dest in bodies, "unresolved local Markdown link")
                fragment = unquote(parts.fragment)
                links.append((dest, fragment))
                child.attrSet("href", (parts.path[:-3] + ".html" if parts.path else "")
                              + ("#doc-" + fragment if fragment else ""))
        anchors[name] = set(headings)
        def render(start, stop):
            selected = tokens[start:stop]
            plain = []
            for t in selected:
                if t.type in {"fence", "code_block"}:
                    plain.append(t.content.rstrip())
                if t.type == "inline":
                    plain.append("".join(c.content if c.type in {"text", "code_inline"}
                                         else "\n" if c.type in {"softbreak", "hardbreak"} else ""
                                         for c in t.children or []))
            lines = source.splitlines(keepends=True)
            first = selected[0].map[0] if selected and selected[0].map else 0
            last = tokens[stop].map[0] if stop < len(tokens) and tokens[stop].map else len(lines)
            return {"markdown": "".join(lines[first:last]),
                    "html": md.renderer.render(selected, md.options, {}), "plain": "\n\n".join(plain)}
        sections = {"#": render(0, len(tokens))}
        for slug, (start, level) in headings.items():
            stop = next((i for i in range(start + 1, len(tokens))
                         if tokens[i].type == "heading_open" and int(tokens[i].tag[1:]) <= level), len(tokens))
            sections["#" + slug] = render(start, stop)
        pages[name] = {**sections["#"], "sections": sections}
    for dest, fragment in links:
        need(not fragment or fragment in anchors[dest], "unresolved Markdown fragment")
    return pages


def compile_corpus(files: Mapping[str, bytes], requirements: dict, *, final: bool = False,
                   routes: Mapping[str, str] | None = None) -> dict:
    """Compile all front-matter Markdown. Complete coverage is NOT human approval."""
    captured = dict(files)
    need(len(captured) <= 10000 and all(isinstance(v, bytes) for v in captured.values())
         and sum(map(len, captured.values())) <= 32_000_000, "reference corpus operational budget exceeded")
    rows = obligations(requirements)
    documents, bodies, seen_paths, seen_ids, entries, guides = {}, {}, set(), set(), {}, []
    for name, payload in sorted(captured.items()):
        path_name(name)
        need(name.endswith(".md"), "corpus has an index/review stamp or unsupported file")
        need(name.casefold() not in seen_paths, "case-colliding corpus paths")
        seen_paths.add(name.casefold())
        document, body = front_matter(payload)
        need(document["id"] not in seen_ids, "duplicate document ID")
        seen_ids.add(document["id"])
        documents[name], bodies[name] = document, body
        if document["kind"] == "guide":
            keys = [target(item) for item in document["subjects"]]
            need(len(keys) == len(set(keys)) and set(keys) <= set(rows), "unresolved/duplicate guide subjects")
            guides.append({"id": document["id"], "title": document["title"], "body": name,
                           "subjects": copy.deepcopy(document["subjects"])})
            continue
        for entry in document["subjects"]:
            need(isinstance(entry, dict) and set(entry) <= {"target", "summary", "body"}
                 and {"target", "summary"} <= set(entry), "unknown editorial fields")
            key = target(entry["target"])
            need(key in rows and key not in entries and rows[key]["rule"] == "authored",
                 "unknown, duplicate, unowned or non-authorable target")
            text(entry["summary"], line=True)
            if "body" in entry:
                need(isinstance(entry["body"], str) and entry["body"].startswith("#"),
                     "body selects this document or one heading, not another file")
            entries[key] = {**copy.deepcopy(entry), "document_id": document["id"], "source_path": name}
    pages = _render_pages(bodies, rows, routes)
    for entry in entries.values():
        if "body" in entry:
            need(entry["body"] in pages[entry["source_path"]]["sections"], "unresolved body fragment")
    def resolve(key, stack=()):
        need(key not in stack, "cyclic canonical documentation relationship")
        row = rows[key]
        if row["rule"] == "structural":
            return None
        if row["rule"] == "reference":
            donor = target(row["canonical"])
            need(rows[donor]["rule"] != "structural", "reference targets a structural facet")
            return resolve(donor, (*stack, key))
        return entries.get(key)
    missing, resolved = [], {}
    for key, row in sorted(rows.items()):
        entry = resolve(key)
        if row["rule"] != "structural" and (entry is None or (row["detail"] and "body" not in entry)):
            missing.append(row["target"])
        elif entry is not None:
            content = pages[entry["source_path"]]["sections"][entry["body"]] if "body" in entry else {}
            resolved[key] = {**entry, "content": content}
    ledger = {name: digest(data) for name, data in sorted(captured.items())}
    if final:
        need(not missing, "release has unresolved documentation requirements")
    return {"format": "riverhog-documentation-compiled-reference/v2", "resolved": resolved,
            "pages": pages, "documents": documents, "guides": guides, "source_files": ledger,
            "inputs": {"closure_sha256": requirements["closure_sha256"],
                       "requirements_sha256": digest(report_bytes(requirements)),
                       "corpus_sha256": digest(report_bytes(ledger))},
            "coverage": {"total": len(rows), "missing": missing,
                         "structural": sum(r["rule"] == "structural" for r in rows.values())}}


def documentation_plan(files: Mapping[str, bytes], requirements: dict) -> list[dict]:
    """Generated author assistance, not a second authored registry or approval."""
    result = compile_corpus(files, requirements)
    missing = {target(t) for t in result["coverage"]["missing"]}
    return [{"target": row["target"], "authority": row["authority"], "interface": row["interface"],
             "status": "missing" if key in missing else "structural" if row["rule"] == "structural" else "covered"}
            for key, row in sorted(obligations(requirements).items())]


def apply_argparse(parser: argparse.ArgumentParser, command: dict, parameters: Mapping[str, dict]) -> None:
    """Adapter seam: source supplies native object/destination mappings, not docs."""
    actions = {a.dest: a for a in parser._actions if not isinstance(a, argparse._SubParsersAction)}
    need(set(parameters) <= set(actions), "unknown native parser parameter")
    need(all(actions[name].help is not argparse.SUPPRESS for name in parameters), "cannot expose suppressed action")
    # Validate before mutating. Escape literal percent so argparse never treats authored prose as a format program.
    description = text(command["summary"], line=True)
    helps = {name: text(entry["summary"], line=True).replace("%", "%%") for name, entry in parameters.items()}
    parser.description = description
    for name, prose in helps.items():
        actions[name].help = prose


def annotate_openapi(document: dict, slots: Mapping[tuple[str, str], dict]) -> dict:
    """Operation-only prototype. Never a generic JSON patch or recursive prose stripper."""
    result = copy.deepcopy(document)
    for (path, method), entry in slots.items():
        need(method in {"get", "put", "post", "delete", "options", "head", "patch", "trace"}
             and path in result.get("paths", {}) and method in result["paths"][path], "unknown native operation")
        operation = result["paths"][path][method]
        operation["summary"] = text(entry["summary"], line=True)
        if entry.get("content", {}).get("markdown"):
            operation["description"] = entry["content"]["markdown"]
    return result


def annotate_python(function, entry: dict, *, owned_module: str):
    """Owned Python function only: no wrappers, imports from author input, or changed signatures."""
    need(inspect.isfunction(function) and function.__module__ == owned_module, "not an owned Python function")
    function.__doc__ = text(entry["summary"], line=True) + "\n\n" + entry.get("content", {}).get("plain", "")
    return function


def stage_project_metadata(project: dict, entry: dict) -> dict:
    """A NEW prepared-source table, before the backend reads it; not a wheel patch."""
    need(not ({"description", "readme"} & set(project.get("dynamic", []))), "dynamic metadata needs its own adapter")
    result = copy.deepcopy(project)
    result["description"] = text(entry["summary"], line=True)
    result["readme"] = {"text": entry.get("content", {}).get("markdown", entry["summary"]), "content-type": "text/markdown"}
    return result
