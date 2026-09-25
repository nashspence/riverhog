"""One static Contract Render with optional bound audit and documentation modes."""

from __future__ import annotations

import hashlib
import html
import json
import posixpath
import re
from collections import defaultdict
from collections.abc import Mapping, Sequence
from html.parser import HTMLParser
from pathlib import PurePosixPath
from typing import cast
from urllib.parse import quote, urlsplit

from .model import INTERFACE_REGISTRY, ContractAtlasError, canonical_bytes, canonical_sha256
from .records import validate_audit_record, validate_closure

DIRECTORY = "riverhog-v1"
_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_STYLES = """
*{box-sizing:border-box}html{background:#fff;color:#242424;font:16px/1.55 system-ui,sans-serif}
body{margin:0}main{max-width:1120px;margin:auto;padding:24px 28px 64px}
header{border-bottom:1px solid #ccc;padding-bottom:14px}header p{margin:.4em 0}
h1{font-size:1.75rem;overflow-wrap:anywhere}h2{font-size:1.3rem;margin-top:1.5em}h3{font-size:1.08rem}
a{color:#174e72;text-underline-offset:3px}
a:focus-visible,input:focus-visible,summary:focus-visible{
  outline:3px solid #174e72;outline-offset:3px}
table{border-collapse:collapse;width:100%;margin:12px 0 20px;table-layout:fixed}
th,td{border-bottom:1px solid #ddd;text-align:left;vertical-align:top;
  padding:7px 10px;overflow-wrap:anywhere}
th{background:#f5f5f5;font-size:.9rem}th:first-child{width:36%}
table table{font-size:.9rem;margin:2px 0 8px}table table th:first-child{width:30%}
code{font:.9em/1.5 ui-monospace,monospace;white-space:pre-wrap;overflow-wrap:anywhere}
.literal-prose{font:inherit}.kind,.meta{color:#555;font-size:.82rem}.kind{display:block}
.member td:first-child{padding-left:30px}.mode{display:none;margin-right:1em}
html[data-js=yes] .mode{display:inline-block}
.audit,.documentation,.audit-cue,.docs-cue{display:none}
html[data-audit=on] .audit,html[data-audit=on] .audit-cue{display:block}
html[data-docs=on] .documentation,html[data-docs=on] .docs-cue{display:block}
.audit,.documentation{border-left:3px solid #89969f;padding:4px 18px;margin:28px 0}
.documentation{border-left-color:#6e8c71}details{margin:8px 0}summary{cursor:pointer}
footer{border-top:1px solid #ddd;margin-top:32px;padding-top:16px;font-size:.83rem}
@media(max-width:700px){main{padding:18px 12px 40px}html{font-size:15px}
  th,td{padding:7px 5px}h1{font-size:1.5rem}}
@media print{.mode{display:none!important}main{max-width:none;padding:0}}
"""
_MODES = """'use strict';
const root=document.documentElement;
const audit=document.getElementById('audit-mode');
const docs=document.getElementById('docs-mode');
root.dataset.js='yes';
function apply(){const url=new URL(location.href);
  root.dataset.audit=url.searchParams.get('audit')==='1'&&audit&&!audit.disabled?'on':'off';
  root.dataset.docs=url.searchParams.get('docs')==='1'&&docs&&!docs.disabled?'on':'off';
  if(audit)audit.checked=root.dataset.audit==='on';
  if(docs)docs.checked=root.dataset.docs==='on';}
function changed(){const url=new URL(location.href);
  for(const [key,control] of [['audit',audit],['docs',docs]])
    if(control&&control.checked)url.searchParams.set(key,'1');else url.searchParams.delete(key);
  history.pushState(null,'',url);apply();}
if(audit)audit.addEventListener('change',changed);
if(docs)docs.addEventListener('change',changed);
addEventListener('popstate',apply);
document.addEventListener('click',event=>{
  const link=event.target.closest('a[href]');if(!link)return;
  const url=new URL(link.href,location.href);
  if(url.origin!==location.origin||!url.pathname.endsWith('.html'))return;
  for(const key of ['audit','docs'])if(root.dataset[key]==='on')url.searchParams.set(key,'1');
  else url.searchParams.delete(key);
  link.href=url.href;});
apply();
const sources=[...document.querySelectorAll('code[data-source-path]')];
if(sources.length)fetch(new URL('../build-manifest.json',location.href))
  .then(response=>response.ok?response.json():null)
  .then(manifest=>{
    if(!manifest||manifest.format!=='riverhog-contract-preview-build/v1'||
       !/^[0-9a-f]{40}$/.test(manifest.source_sha))return;
    for(const source of sources){
      const path=source.dataset.sourcePath.split('/').map(encodeURIComponent).join('/');
      const line=source.dataset.sourceLine;
      const link=document.createElement('a');
      link.href='https://github.com/nashspence/riverhog/blob/'+manifest.source_sha+'/'+path+
        (line?'#L'+line:'');
      link.dataset.sourceLink='exact-commit';
      source.replaceWith(link);link.append(source);
    }
  }).catch(()=>{});
"""


def _esc(value: object) -> str:
    return html.escape(str(value), quote=True).replace("\r", "&#13;")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:24]


def _element_file(identity: str) -> str:
    return f"e-{_hash(identity)}.html"


def _inventory_file(authority: str, interface: str) -> str:
    return f"i-{_hash(authority + chr(0) + interface)}.html"


def _link(target: str, label: str) -> str:
    return f'<a href="{_esc(target)}">{_esc(label)}</a>'


def _parts(pointer: str) -> list[str]:
    if not pointer.startswith("/") or re.search(r"~(?![01])", pointer):
        raise ContractAtlasError(f"invalid contract pointer: {pointer}")
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer[1:].split("/")]


def _token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _at(value: object, pointer: str) -> object:
    current = value
    for part in _parts(pointer):
        try:
            current = (
                current[int(part)]
                if isinstance(current, list)
                else cast(Mapping[str, object], current)[part]
            )
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise ContractAtlasError(f"unresolved contract pointer: {pointer}") from exc
    return current


def _literal(value: object, pointer: str, *, prose: bool = False) -> str:
    if isinstance(value, str):
        control = any(ord(char) < 32 for char in value)
        visible = json.dumps(value, ensure_ascii=False) if control else value
        kind = "string (JSON spelling for control characters)" if control else "string"
    else:
        visible = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        kind = (
            "null"
            if value is None
            else "boolean"
            if isinstance(value, bool)
            else "object"
            if isinstance(value, dict)
            else "array"
            if isinstance(value, list)
            else "number"
        )
    css = ' class="literal-prose"' if prose else ""
    return (
        f'<code{css} data-value-pointer="{_esc(pointer)}">{_esc(visible)}</code>'
        f'<span class="kind">{kind}</span>'
    )


def _value_html(
    value: object,
    pointer: str,
    *,
    prose: bool = False,
    references: Mapping[str, str] | None = None,
) -> str:
    anchor = f' id="v-{_hash(pointer)}"'
    if not isinstance(value, (dict, list)) or not value:
        definition = (
            " " + _link(references[pointer], "Definition")
            if references is not None and pointer in references
            else ""
        )
        return f"<span{anchor}>{_literal(value, pointer, prose=prose)}{definition}</span>"
    items = sorted(value.items()) if isinstance(value, dict) else list(enumerate(value))
    rows = [
        '<tr><th scope="row"><code>'
        + _esc(key)
        + "</code></th><td>"
        + _value_html(child, f"{pointer}/{_token(str(key))}", references=references)
        + "</td></tr>"
        for key, child in items
    ]
    return f"<table{anchor}><tbody>{''.join(rows)}</tbody></table>"


def _reference_links(
    closure: Mapping[str, object],
    element: Mapping[str, object],
    owners: Mapping[str, str],
) -> dict[str, str]:
    """Resolve every local schema reference to an exact declared subject and anchor."""

    links: dict[str, str] = {}

    def visit(value: object, pointer: str, http_application: str | None = None) -> None:
        if isinstance(value, Mapping):
            if (
                isinstance(value.get("kind"), str)
                and value["kind"]
                in {
                    "http-operation-response",
                    "openapi-schema",
                }
                and isinstance(value.get("application"), str)
            ):
                http_application = str(value["application"])
            for key, child in value.items():
                child_pointer = f"{pointer}/{_token(str(key))}"
                if key == "$ref":
                    if not isinstance(child, str) or not child.startswith("#/"):
                        raise ContractAtlasError(
                            f"unsupported normative reference: {child_pointer}"
                        )
                    candidates: list[str] = []
                    base = pointer
                    while base.startswith("/external_contract/"):
                        target = base + child[1:]
                        try:
                            _at(closure, target)
                        except ContractAtlasError:
                            pass
                        else:
                            candidates.append(target)
                        base = base.rsplit("/", 1)[0]
                    if http_application is not None:
                        target = (
                            "/external_contract/http_openapi/"
                            + _token(http_application)
                            + child[1:]
                        )
                        try:
                            _at(closure, target)
                        except ContractAtlasError:
                            pass
                        else:
                            candidates.append(target)
                    if len(candidates) != 1:
                        raise ContractAtlasError(
                            f"normative reference has {len(candidates)} possible targets: "
                            f"{child_pointer}"
                        )
                    target = candidates[0]
                    candidate = target
                    while candidate not in owners and "/" in candidate:
                        candidate = candidate.rsplit("/", 1)[0]
                    if candidate not in owners:
                        raise ContractAtlasError(
                            f"normative reference has no declared subject: {child_pointer}"
                        )
                    links[child_pointer] = _element_file(owners[candidate]) + "#v-" + _hash(target)
                else:
                    visit(child, child_pointer, http_application)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                visit(child, f"{pointer}/{index}", http_application)

    for pointer in cast(Sequence[str], element["pointers"]):
        visit(_at(closure, pointer), pointer)
    return links


def contract_body(
    closure: Mapping[str, object],
    element: Mapping[str, object],
    *,
    owners: Mapping[str, str] | None = None,
) -> str:
    """Render normative values using no audit or documentation input."""

    if owners is None:
        owners = {
            pointer: str(item["id"])
            for item in cast(Sequence[Mapping[str, object]], closure["elements"])
            for pointer in cast(Sequence[str], item["pointers"])
        }
    references = _reference_links(closure, element, owners)
    sections: list[str] = []
    for pointer in cast(Sequence[str], element["pointers"]):
        value = _at(closure, pointer)
        sections.append(
            f"<section><h3><code>{_esc(pointer)}</code></h3>"
            + _value_html(
                value,
                pointer,
                prose=element["interface"] == "compatibility-guarantees",
                references=references,
            )
            + "</section>"
        )
    return (
        f'<section id="contract" data-contract="{_esc(element["id"])}">'
        "<h2>Contract</h2>" + "".join(sections) + "</section>"
    )


def _documentation(
    closure: Mapping[str, object], document: Mapping[str, object] | None
) -> tuple[dict[str, str], list[Mapping[str, object]]]:
    if document is None:
        return {}, []
    if (
        set(document)
        != {
            "format",
            "closure_sha256",
            "release_scope",
            "build_scope",
            "source_revision",
            "explanations",
            "guides",
        }
        or document["format"] != "riverhog-contract-documentation-record/v1"
    ):
        raise ContractAtlasError("documentation record has an unknown or incomplete format")
    if document["closure_sha256"] != canonical_sha256(closure) or any(
        not isinstance(document[key], str) or not document[key]
        for key in ("release_scope", "build_scope", "source_revision")
    ):
        raise ContractAtlasError("documentation record has a stale or incomplete scope")
    ids = {str(item["id"]) for item in cast(Sequence[Mapping[str, object]], closure["elements"])}
    explanations: dict[str, str] = {}
    for item in cast(Sequence[Mapping[str, object]], document["explanations"]):
        if set(item) != {"element_id", "text"} or item["element_id"] not in ids:
            raise ContractAtlasError("documentation explanation has an unresolved subject")
        identity, explanation = str(item["element_id"]), str(item["text"])
        if identity in explanations or not explanation:
            raise ContractAtlasError("documentation explanation is duplicate or empty")
        explanations[identity] = explanation
    guides = cast(Sequence[Mapping[str, object]], document["guides"])
    guide_ids: set[str] = set()
    for guide in guides:
        if set(guide) != {"id", "title", "text", "subjects"}:
            raise ContractAtlasError("documentation guide has unreviewed fields")
        identity = str(guide["id"])
        if not identity or identity in guide_ids or not guide["title"] or not guide["text"]:
            raise ContractAtlasError("documentation guide identity or text is invalid")
        guide_ids.add(identity)
        subjects = cast(Sequence[str], guide["subjects"])
        if not subjects or len(subjects) != len(set(subjects)) or not set(subjects) <= ids:
            raise ContractAtlasError("documentation guide has stale subjects")
    return explanations, list(guides)


def _source_html(source: Mapping[str, object], source_revision: str | None) -> str:
    location = source.get("source")
    if not isinstance(location, Mapping) or not isinstance(location.get("path"), str):
        return f"<code>{_esc(source['id'])}</code>"
    path = str(location["path"])
    posix = PurePosixPath(path)
    if posix.is_absolute() or ".." in posix.parts or "\\" in path:
        raise ContractAtlasError("audit source path is unsafe")
    label = path + (f"::{location['symbol']}" if location.get("symbol") else "")
    if source_revision is None:
        line = location.get("line")
        line_attribute = f' data-source-line="{line}"' if isinstance(line, int) and line > 0 else ""
        return f'<code data-source-path="{_esc(path)}"{line_attribute}>{_esc(label)}</code>'
    href = (
        "https://github.com/nashspence/riverhog/blob/"
        + source_revision
        + "/"
        + quote(path, safe="/")
    )
    if isinstance(location.get("line"), int) and location["line"] > 0:
        href += f"#L{location['line']}"
    return _link(href, label)


def _audit_panel(
    element: Mapping[str, object],
    overlay: Mapping[str, object] | None,
    audit: Mapping[str, object] | None,
    source_revision: str | None,
) -> str:
    if overlay is None or audit is None:
        return ""
    sources = {
        str(item["id"]): item for item in cast(Sequence[Mapping[str, object]], audit["sources"])
    }
    analysis = cast(Mapping[str, object], audit["extent_analysis"])
    decisions = {
        str(item["id"]): item
        for item in cast(Sequence[Mapping[str, object]], analysis["decisions"])
    }
    source_ids = cast(Sequence[str], overlay["source_authority_ids"])
    source_items = "".join(
        f"<li>{_source_html(sources[identity], source_revision)}</li>" for identity in source_ids
    )
    decision_ids = cast(Sequence[str], overlay["extent_decision_ids"])
    decision_rows = "".join(
        f"<li><code>{_esc(identity)}</code>"
        + _value_html(decisions[identity], "/audit/decisions/" + _token(identity))
        + "</li>"
        for identity in decision_ids
    )
    routes = "".join(
        f"<li><code>{_esc(route)}</code></li>"
        for route in cast(Sequence[str], overlay["qualification_routes"])
    )
    body = (
        '<aside class="audit" id="audit"><h2>Audit context · noncontractual</h2>'
        + "<h3>Source associations</h3><ul>"
        + source_items
        + "</ul><h3>Candidate validation routes, not execution results</h3><ul>"
        + routes
        + "</ul>"
    )
    if decision_rows:
        body += (
            "<h3>Generated extent analysis</h3><p>These records index source constraints; "
            "they do not add limits or progression promises.</p><ul>" + decision_rows + "</ul>"
        )
    return body + "</aside>"


def _shell(
    title: str,
    body: str,
    closure_sha256: str,
    *,
    audit: bool,
    documentation: bool,
    breadcrumbs: str = "",
) -> bytes:
    controls = (
        '<label class="mode"><input id="audit-mode" type="checkbox"'
        + ("" if audit else " disabled")
        + "> Audit Mode</label>"
        + '<label class="mode"><input id="docs-mode" type="checkbox"'
        + ("" if documentation else " disabled")
        + "> Documentation Mode</label>"
    )
    status = (
        '<p class="meta">Generated v1 candidate; no freeze or qualification '
        "acceptance is claimed.</p>"
        + ("" if audit else '<p class="meta">Audit Record not supplied.</p>')
    )
    footer = (
        "<footer>"
        + _link("../riverhog-v1.json", "Exact Contract Closure")
        + (" · " + _link("../riverhog-v1-audit.json", "Bound Audit Record") if audit else "")
        + " · "
        + _link("manifest.json", "Artifact manifest")
        + f"<p>Closure SHA-256: <code>{closure_sha256}</code></p></footer>"
    )
    page = (
        '<!doctype html><html lang="en" data-audit="off" data-docs="off">'
        '<head><meta charset="utf-8"><meta name="viewport" '
        'content="width=device-width,initial-scale=1">'
        f"<title>{_esc(title)} · Riverhog Contract Render</title>"
        '<link rel="stylesheet" href="style.css"><script src="modes.js" defer></script></head>'
        "<body><main><header>"
        + breadcrumbs
        + status
        + controls
        + "<noscript><p>Contract-only reading remains available without JavaScript.</p></noscript>"
        + "</header>"
        + f"<h1>{_esc(title)}</h1>"
        + body
        + footer
        + "</main></body></html>"
    )
    # Checked candidate pages are reviewed in Git. Keep each HTML tag on its
    # own line so a changed contract value produces a bounded source diff.
    return page.replace("><", ">\n<").replace(">\n</code>", "></code>").encode("utf-8")


def render_contract(
    closure: Mapping[str, object],
    audit: Mapping[str, object] | None = None,
    documentation: Mapping[str, object] | None = None,
    *,
    source_revision: str | None = None,
) -> dict[str, bytes]:
    """Render the entire Closure with independent optional bound augmentations."""

    validate_closure(closure)
    if audit is not None:
        validate_audit_record(closure, audit)
    if source_revision is not None and not _REVISION.fullmatch(source_revision):
        raise ContractAtlasError("source revision must be an exact commit SHA")
    explanations, guides = _documentation(closure, documentation)
    closure_sha256 = canonical_sha256(closure)
    elements = cast(Sequence[Mapping[str, object]], closure["elements"])
    overlays = (
        {
            str(item["id"]): item
            for item in cast(Sequence[Mapping[str, object]], audit["element_overlays"])
        }
        if audit is not None
        else {}
    )
    owners = {
        pointer: str(element["id"])
        for element in elements
        for pointer in cast(Sequence[str], element["pointers"])
    }
    groups: dict[tuple[str, str], list[Mapping[str, object]]] = defaultdict(list)
    for element in elements:
        interface = str(element["interface"])
        if interface not in INTERFACE_REGISTRY:
            raise ContractAtlasError(f"contract element has no interface renderer: {interface}")
        groups[(str(element["authority"]), interface)].append(element)
    files: dict[str, bytes] = {"style.css": _STYLES.encode(), "modes.js": _MODES.encode()}
    root_rows: list[str] = []
    for (authority, interface), members in sorted(groups.items()):
        inventory_path = _inventory_file(authority, interface)
        label = INTERFACE_REGISTRY[interface].label
        root_rows.append(
            "<tr><td><code>"
            + _esc(authority)
            + "</code></td><td>"
            + _link(inventory_path, label)
            + f" · {len(members)}</td></tr>"
        )
        rows: list[str] = []
        ordered = sorted(members, key=lambda item: (str(item["title"]).casefold(), str(item["id"])))
        if interface == "python":
            exports: dict[str, Mapping[str, object]] = {}
            children: dict[str, list[Mapping[str, object]]] = defaultdict(list)
            for element in ordered:
                value = cast(
                    Mapping[str, object], _at(closure, cast(Sequence[str], element["pointers"])[0])
                )
                identity = cast(Sequence[str], element["pointers"])[0].rsplit("/", 1)[-1]
                identity = identity.replace("~1", "/").replace("~0", "~")
                if value["unit"] == "member":
                    children[str(value["owner"])].append(element)
                else:
                    exports[identity] = element
            if set(children) - set(exports):
                raise ContractAtlasError("Python member has no declared parent in its inventory")
            ordered = []
            for identity, element in sorted(exports.items()):
                ordered.append(element)
                ordered.extend(
                    sorted(children.get(identity, []), key=lambda item: str(item["title"]))
                )
        for element in ordered:
            identity = str(element["id"])
            record_value = _at(closure, cast(Sequence[str], element["pointers"])[0])
            is_member = (
                interface == "python"
                and isinstance(record_value, Mapping)
                and record_value.get("unit") == "member"
            )
            cue = (
                '<span class="audit-cue"> · audit context available</span>'
                if overlays.get(identity, {}).get("extent_decision_ids")
                else ""
            )
            name = _link(_element_file(identity), str(element["title"])) + cue
            if interface == "python":
                kind = cast(
                    Mapping[str, object], cast(Mapping[str, object], record_value)["contract"]
                )["kind"]
                fact = _literal(
                    kind, cast(Sequence[str], element["pointers"])[0] + "/contract/kind"
                )
            elif interface == "compatibility-guarantees":
                fact = _literal(
                    record_value, cast(Sequence[str], element["pointers"])[0], prose=True
                )
            else:
                fact = "<code>" + _esc(cast(Sequence[str], element["pointers"])[0]) + "</code>"
            rows.append(
                f'<tr data-element="{_esc(identity)}"{(' class="member"' if is_member else "")}>'
                + "<td>"
                + name
                + "</td><td>"
                + fact
                + "</td></tr>"
            )
            doc_body = ""
            if identity in explanations:
                doc_body = (
                    '<aside class="documentation" id="documentation">'
                    "<h2>Documentation · noncontractual</h2>"
                    + "<p>"
                    + _esc(explanations[identity])
                    + "</p></aside>"
                )
            breadcrumbs = (
                "<p>"
                + _link("index.html", "All authorities")
                + " / "
                + _link(inventory_path, f"{authority} · {label}")
                + "</p>"
            )
            files[_element_file(identity)] = _shell(
                str(element["title"]),
                contract_body(closure, element, owners=owners)
                + _audit_panel(element, overlays.get(identity), audit, source_revision)
                + doc_body,
                closure_sha256,
                audit=audit is not None,
                documentation=documentation is not None,
                breadcrumbs=breadcrumbs,
            )
        heading = (
            "Declared kind"
            if interface == "python"
            else "Owned promise"
            if interface == "compatibility-guarantees"
            else "Contract pointer"
        )
        table = (
            "<table><thead><tr><th>Element</th><th>"
            + _esc(heading)
            + "</th></tr></thead><tbody>"
            + "".join(rows)
            + "</tbody></table>"
        )
        files[inventory_path] = _shell(
            f"{authority} · {label}",
            table,
            closure_sha256,
            audit=audit is not None,
            documentation=documentation is not None,
            breadcrumbs="<p>" + _link("index.html", "All authorities") + "</p>",
        )
    guide_links: list[str] = []
    for guide in guides:
        identity = str(guide["id"])
        path = f"g-{_hash(identity)}.html"
        guide_links.append(f"<li>{_link(path, str(guide['title']))}</li>")
        subjects = "".join(
            f"<li>{_link(_element_file(str(subject)), str(subject))}</li>"
            for subject in cast(Sequence[str], guide["subjects"])
        )
        files[path] = _shell(
            str(guide["title"]),
            '<aside class="documentation"><h2>Guide · noncontractual</h2><p>'
            + _esc(guide["text"])
            + "</p><h3>Exact contract subjects</h3><ul>"
            + subjects
            + "</ul></aside>",
            closure_sha256,
            audit=audit is not None,
            documentation=True,
            breadcrumbs="<p>" + _link("index.html", "All authorities") + "</p>",
        )
    body = (
        f"<p>{len(elements)} declared contract elements in the current candidate. "
        "Membership and values come from the exact Closure.</p>"
        "<table><thead><tr><th>Authority</th><th>Interface · elements</th></tr></thead><tbody>"
        + "".join(root_rows)
        + "</tbody></table>"
    )
    if audit is not None:
        body += (
            '<aside class="audit"><h2>Audit record · noncontractual</h2>'
            "<p>Discovery and witness associations are candidate accounting, "
            "not executed results.</p>"
            + _value_html(
                cast(Mapping[str, object], audit["discovery"])["anomalies"],
                "/audit/discovery/anomalies",
            )
            + "</aside>"
        )
    if guides:
        body += (
            '<aside class="documentation"><h2>Guides</h2><ul>'
            + "".join(guide_links)
            + "</ul></aside>"
        )
    files["index.html"] = _shell(
        "Riverhog v1 Contract Render",
        body,
        closure_sha256,
        audit=audit is not None,
        documentation=documentation is not None,
    )
    manifest = {
        "format": "riverhog-contract-render-manifest/v1",
        "closure_sha256": closure_sha256,
        "audit_sha256": canonical_sha256(audit) if audit is not None else None,
        "documentation_sha256": canonical_sha256(documentation)
        if documentation is not None
        else None,
        "source_revision": source_revision,
        "files": {
            path: hashlib.sha256(payload).hexdigest() for path, payload in sorted(files.items())
        },
    }
    files["manifest.json"] = canonical_bytes(manifest)
    return {f"{DIRECTORY}/{path}": payload for path, payload in files.items()}


class _PageLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.anchors: set[str] = set()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        anchor = attributes.get("id")
        if anchor is not None:
            if anchor in self.anchors:
                raise ContractAtlasError(f"contract render repeats an anchor: {anchor}")
            self.anchors.add(anchor)
        if tag in {"a", "link"} and attributes.get("href"):
            self.links.append(str(attributes["href"]))
        if tag == "script" and attributes.get("src"):
            self.links.append(str(attributes["src"]))


def validate_render(files: Mapping[str, bytes]) -> None:
    """Check every generated page, local link, fragment, and asset destination."""

    parsed: dict[str, _PageLinks] = {}
    for path, payload in files.items():
        if not path.endswith(".html"):
            continue
        page = _PageLinks()
        page.feed(payload.decode("utf-8"))
        page.close()
        parsed[path] = page
    allowed_machine = {"riverhog-v1.json", "riverhog-v1-audit.json"}
    for source, page in parsed.items():
        for href in page.links:
            target = urlsplit(href)
            if target.scheme or target.netloc:
                if target.scheme not in {"http", "https"}:
                    raise ContractAtlasError(f"render has unsupported external link: {href}")
                continue
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(source), target.path))
            if resolved in allowed_machine and not target.fragment:
                continue
            if resolved not in files:
                raise ContractAtlasError(f"render has an unresolved local link: {source} -> {href}")
            if target.fragment and (
                resolved not in parsed or target.fragment not in parsed[resolved].anchors
            ):
                raise ContractAtlasError(f"render has an unresolved anchor: {source} -> {href}")
