"""#952 reference: compile a captured prose corpus against SOURCE-owned obligations.

Not a production discovery engine or release signer. The producer supplies the full
obligation ledger. Report encoding below is reference-local, NOT Riverhog's JCS codec.
No network, template execution, source mutation, or implicit review approval occurs.
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
from pathlib import PurePosixPath
from typing import Any, Mapping
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt

SOURCE = "riverhog-release-documentation-source/v2"
REVIEW = "riverhog-documentation-review-reference/v1"
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


def capture_git(repository: str, commit: str, version: str) -> dict[str, bytes]:
    """Capture regular JSON/Markdown files from an exact object, not a live checkout."""
    need(bool(SHA.fullmatch(commit)), "exact documentation commit required")
    need(re.fullmatch(r"v1\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version) is not None,
         "exact final version subtree required")
    def git(*args):
        return subprocess.check_output(["git", "-C", repository, *args], stderr=subprocess.PIPE)
    rows = git("ls-tree", "-rz", commit, "--", version + "/").split(b"\0")
    files = {}
    for row in rows:
        if not row:
            continue
        metadata, raw = row.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        name = raw.decode()
        need(name.startswith(version + "/") and mode == "100644" and kind == "blob",
             "corpus contains a non-regular source")
        relative = path_name(name[len(version) + 1:])
        need(relative.endswith((".json", ".md")), "reference capture supports JSON/Markdown only")
        files[relative] = git("cat-file", "blob", oid)
    need("documentation.json" in files, "missing version documentation index")
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


def compile_corpus(files: Mapping[str, bytes], requirements: dict, *, final: bool = False, routes: Mapping[str, str] | None = None) -> dict:
    """Preview lists missing obligations; final also requires a matching review fence."""
    captured = dict(files)
    need(0 < len(captured) <= 10000 and sum(map(len, captured.values())) <= 32_000_000,
         "reference corpus operational budget exceeded")
    names = set()
    for name, payload in captured.items():
        path_name(name)
        need(name.casefold() not in names and isinstance(payload, bytes), "case collision/bad bytes")
        names.add(name.casefold())
    rows = obligations(requirements)
    index = read_json(captured.get("documentation.json", b""))
    need(isinstance(index, dict) and set(index) == {"format", "entries", "guides"}
         and index["format"] == SOURCE and isinstance(index["entries"], list)
         and isinstance(index["guides"], list), "unknown index schema")
    entries, used, pages = {}, {"documentation.json"}, set()
    for entry in index["entries"]:
        need(isinstance(entry, dict) and set(entry) <= {"target", "summary", "body"}
             and {"target", "summary"} <= set(entry), "unknown editorial fields")
        key = target(entry["target"])
        need(key in rows and key not in entries and rows[key]["rule"] == "authored",
             "unknown, duplicate, unowned or non-authorable target")
        text(entry["summary"], line=True)
        if "body" in entry:
            name = path_name(entry["body"])
            need(name.endswith(".md") and name in captured, "missing Markdown body")
            pages.add(name)
        entries[key] = copy.deepcopy(entry)
    guide_ids = set()
    for guide in index["guides"]:
        need(isinstance(guide, dict) and set(guide) == {"id", "title", "body", "subjects"}, "bad guide")
        need(isinstance(guide["id"], str) and re.fullmatch(r"[a-z][a-z0-9-]*", guide["id"])
             and guide["id"] not in guide_ids, "duplicate/invalid guide ID")
        guide_ids.add(guide["id"])
        text(guide["title"], line=True)
        keys = [target(t) for t in guide["subjects"]]
        need(bool(keys) and len(keys) == len(set(keys)) and set(keys) <= set(rows), "unresolved guide subjects")
        name = path_name(guide["body"])
        need(name.endswith(".md") and name in captured, "missing guide Markdown")
        pages.add(name)

    md = MarkdownIt("commonmark", {"html": True})
    # Parse unsafe schemes as links so our policy rejects rather than silently dropping them.
    md.validateLink = lambda value: True
    parsed, headings, links = {}, {}, []
    queue = sorted(pages)
    while queue:
        name = queue.pop(0)
        if name in parsed:
            continue
        source = text(captured[name].decode("utf-8"))
        tokens = md.parse(source)
        anchors = set()
        plain = []
        for i, token in enumerate(tokens):
            need(token.type not in {"html_block", "html_inline"}, "raw HTML is not authored markup")
            if token.type == "heading_open":
                slug = re.sub(r"[^\w-]+", "-", tokens[i + 1].content.casefold()).strip("-") or "section"
                need(slug not in anchors, "duplicate/colliding heading IDs")
                anchors.add(slug)
                token.attrSet("id", slug)
            if token.type in {"fence", "code_block"}:
                plain.append(token.content.rstrip())
            if token.type == "inline":
                inline = []
                for child in token.children or []:
                    need(child.type not in {"html_inline", "image"}, "HTML/images unsupported by reference profile")
                    if child.type in {"text", "code_inline"}:
                        inline.append(child.content)
                    elif child.type in {"softbreak", "hardbreak"}:
                        inline.append("\n")
                    if child.type == "link_open":
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
                        need(dest.endswith(".md") and dest in captured, "unresolved local Markdown link")
                        links.append((dest, unquote(parts.fragment)))
                        if parts.path:
                            child.attrSet("href", parts.path[:-3] + ".html" + ("#" + parts.fragment if parts.fragment else ""))
                        if dest not in parsed:
                            queue.append(dest)
                plain.append("".join(inline))
        parsed[name] = {"markdown": source, "html": md.renderer.render(tokens, md.options, {}),
                        "plain": "\n\n".join(plain)}
        headings[name] = anchors
        used.add(name)
    for dest, fragment in links:
        need(not fragment or fragment in headings[dest], "unresolved Markdown fragment")
    need(set(captured) - used <= {"review.json"}, "unindexed/orphan corpus file")

    missing, resolved = [], {}
    def resolve(key, stack=()):
        need(key not in stack, "cyclic canonical documentation relationship")
        row = rows[key]
        if row["rule"] == "structural":
            return None
        if row["rule"] == "reference":
            donor = resolve(target(row["canonical"]), (*stack, key))
            need(rows[target(row["canonical"])] ["rule"] != "structural", "reference targets a structural facet")
            return donor
        return entries.get(key)
    for key, row in rows.items():
        entry = resolve(key)
        if row["rule"] != "structural" and (entry is None or (row["detail"] and "body" not in entry)):
            missing.append(row["target"])
        elif entry is not None:
            resolved[key] = {**entry, "content": parsed.get(entry.get("body"), {})}
    ledger = {name: digest(payload) for name, payload in sorted(captured.items()) if name != "review.json"}
    expected = {"format": REVIEW, "closure_sha256": requirements["closure_sha256"],
                "requirements_sha256": digest(report_bytes(requirements)),
                "corpus_sha256": digest(report_bytes(ledger))}
    review_matches = "review.json" in captured and read_json(captured["review.json"]) == expected
    if final:
        need(not missing, "release has unresolved documentation requirements")
        need(review_matches, "missing/stale review fence; ordinary builds never approve")
    return {"format": "riverhog-documentation-compiled-reference/v1", "resolved": resolved,
            "pages": parsed, "guides": index["guides"], "files": ledger,
            "source_files": {name: digest(data) for name, data in sorted(captured.items())},
            "coverage": {"total": len(rows), "missing": missing,
                         "structural": sum(r["rule"] == "structural" for r in rows.values())},
            "review_expected": expected, "review_matches": review_matches}


def proposed_review(compiled: dict) -> bytes:
    """Explicit review aid, never evidence of human approval or a release signature."""
    need(not compiled["coverage"]["missing"], "cannot propose a complete-review fence with missing docs")
    return report_bytes(compiled["review_expected"])


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
