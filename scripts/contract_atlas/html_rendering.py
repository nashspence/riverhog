"""One static Contract Render with optional bound audit and documentation modes."""

from __future__ import annotations

import hashlib
import html
import posixpath
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from html.parser import HTMLParser
from pathlib import PurePosixPath
from typing import cast
from urllib.parse import quote, urlsplit

from .human_contract import command_path, inventory_fact, render_human
from .model import INTERFACE_REGISTRY, ContractAtlasError, canonical_bytes, canonical_sha256
from .records import AUDIT_PRESENTATION, validate_audit_record, validate_closure

DIRECTORY = "riverhog-v1"
_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_STYLES = """
*{box-sizing:border-box}html{--bg:#fff;--fg:#242424;--link:#174e72;
  --line:#d8dfe3;--rule:#ddd;--muted:#555;--context:#4a4a4a;
  --shape:#343d43;--member:#b7ccd7;--audit:#89969f;--docs:#6e8c71;
  color-scheme:light;background:var(--bg);color:var(--fg);font:16px/1.55 system-ui,sans-serif}
body{margin:0}main{max-width:1120px;margin:auto;padding:24px 28px 64px;overflow-wrap:anywhere}
header{border-bottom:1px solid var(--rule);padding-bottom:14px}header p{margin:.4em 0}
h1{font-size:1.75rem;overflow-wrap:anywhere}h2{font-size:1.3rem;margin-top:1.5em}h3{font-size:1.08rem}
a{color:var(--link);text-underline-offset:3px}
a:focus-visible,input:focus-visible,summary:focus-visible{
  outline:3px solid var(--link);outline-offset:3px}
table{display:block;border-collapse:collapse;width:100%;margin:12px 0 20px}
table tbody{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:8px}
table thead{display:none}
table tr{display:block;min-width:0;border:1px solid var(--line);border-radius:5px;padding:8px 10px}
table tr.module{grid-column:1/-1;border:0;border-radius:0;margin-top:10px;padding:0}
table tr.member{margin-left:14px;width:calc(100% - 14px);border-left:3px solid var(--member)}
table th,table td{display:block;border:0;text-align:left;vertical-align:top;
  padding:2px 0;overflow-wrap:anywhere}
table td:empty{display:none}
table td[data-label]:before{content:attr(data-label) ": ";font-weight:600}
.selection-table td:first-child:before,.inventory-table td[data-label]:before{content:none}
.selection-table td:first-child{font-weight:600}
.record-collection{max-width:100%}
.selection-list{padding-left:24px}.selection-list li{margin:5px 0}
table table{font-size:.9rem;margin:2px 0 8px}
code{font:.9em/1.5 ui-monospace,monospace;white-space:pre-wrap;overflow-wrap:anywhere}
.literal-prose{font:inherit}.kind,.meta{color:var(--muted);font-size:.82rem}.kind{display:block}
.mode{display:none;margin-right:1em}
html[data-js=yes] .mode{display:inline-block}
.audit,.documentation,.audit-cue,.docs-cue{display:none}
html[data-audit=on] .audit{display:block}html[data-audit=on] .audit-cue{display:inline}
html[data-docs=on] .documentation{display:block}html[data-docs=on] .docs-cue{display:inline}
.audit,.documentation{border-left:3px solid var(--audit);padding:4px 18px;margin:28px 0}
.documentation{border-left-color:var(--docs)}details{margin:12px 0}summary{cursor:pointer}
.audit-references,.documentation-references{border:0;padding:0;margin:4px 0}
.references{margin:5px 0}.references summary{font-size:.9rem;color:var(--context)}
.references ul{margin:5px 0 8px}
.audit-marker{font-size:1.1em;margin-left:.25em}.lead{font-size:1.08rem}
.facts{display:grid;grid-template-columns:minmax(140px,220px) minmax(0,1fr);gap:3px 16px}
.facts dt{font-weight:600}.facts dd{margin:0 0 8px}
.schema-section{border-top:1px solid var(--rule);margin-top:18px;padding-top:10px}
.shape{color:var(--shape)}.exact{border-top:1px solid var(--rule);padding-top:10px;margin-top:30px}
.exact summary{font-weight:600}
.authority-filter{display:none;margin:12px 0}
html[data-js=yes] .authority-filter{display:block}
.authority-filter label{display:block;font-weight:600}
.authority-filter input{width:min(100%,24rem);padding:6px 8px;
  border:1px solid var(--line);
  border-radius:5px;background:var(--bg);color:var(--fg);font:inherit}
.authority-filter .meta{display:block}
.authority-card[hidden]{display:none}
.authority-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));
  gap:12px;margin:14px 0}
.authority-card{border:1px solid var(--line);border-radius:5px;padding:12px;min-width:0}
.authority-card h3{margin:0}.authority-card p{margin:4px 0 10px}
.authority-interface{padding:2px 0}
.command-tree,.command-tree ul{list-style:none;padding-left:20px}
.command-tree li{border-left:2px solid var(--line);padding-left:10px;margin:5px 0}
.context{color:var(--context)}.comparison-promise{font:inherit}
footer{border-top:1px solid var(--rule);margin-top:32px;padding-top:16px;font-size:.83rem}
@media(prefers-color-scheme:dark){html{--bg:#171d22;--fg:#e8edf0;--link:#8bd0fb;
  --line:#52616b;--rule:#44525c;--muted:#bac5cb;--context:#c4cdd2;
  --shape:#d4dee3;--member:#7193a4;--audit:#9db4c2;--docs:#9bc5a1;
  color-scheme:dark}}
@media(max-width:700px){main{padding:18px 12px 40px}html{font-size:15px}
  h1{font-size:1.5rem}.facts{grid-template-columns:1fr;gap:0}
  .facts dd{margin-bottom:12px}
  .authority-cards{grid-template-columns:1fr}
  table tbody{grid-template-columns:1fr}
}
@media print{.mode{display:none!important}main{max-width:none;padding:0}}
"""
_MODES = """'use strict';
const root=document.documentElement;
const audit=document.getElementById('audit-mode');
const docs=document.getElementById('docs-mode');
root.dataset.js='yes';
const authorityFilter=document.getElementById('authority-filter');
if(authorityFilter){
  const cards=[...document.querySelectorAll('#authority-cards > .authority-card')];
  const count=document.getElementById('authority-filter-count');
  function filterAuthorities(){
    const query=authorityFilter.value.trim().toLowerCase();
    let shown=0;
    for(const card of cards){
      card.hidden=!card.dataset.authority.toLowerCase().includes(query);
      if(!card.hidden)shown++;
    }
    count.textContent=shown+' of '+cards.length+' authorities';
  }
  authorityFilter.addEventListener('input',filterAuthorities);
  filterAuthorities();
}
function apply(){const url=new URL(location.href);
  const requestedAudit=url.searchParams.get('audit');
  root.dataset.audit=(requestedAudit==='1'||
    (requestedAudit===null&&root.dataset.auditDefault==='on'))&&audit&&!audit.disabled?'on':'off';
  const requestedDocs=url.searchParams.get('docs');
  root.dataset.docs=(requestedDocs==='1'||
    (requestedDocs===null&&root.dataset.docsDefault==='on'))&&docs&&!docs.disabled?'on':'off';
  if(audit)audit.checked=root.dataset.audit==='on';
  if(docs)docs.checked=root.dataset.docs==='on';}
function changed(){const url=new URL(location.href);
  for(const [key,control] of [['audit',audit],['docs',docs]]){
    if(control&&control.checked)url.searchParams.set(key,'1');
    else if((key==='audit'&&root.dataset.auditDefault==='on')||
            (key==='docs'&&root.dataset.docsDefault==='on'))url.searchParams.set(key,'0');
    else url.searchParams.delete(key);
  }
  history.pushState(null,'',url);apply();}
if(audit)audit.addEventListener('change',changed);
if(docs)docs.addEventListener('change',changed);
addEventListener('popstate',apply);
function revealFragment(){
  if(!location.hash)return;
  const target=document.getElementById(decodeURIComponent(location.hash.slice(1)));
  for(let parent=target?.parentElement;parent;parent=parent.parentElement)
    if(parent.tagName==='DETAILS')parent.open=true;
}
addEventListener('hashchange',revealFragment);
document.addEventListener('click',event=>{
  const link=event.target.closest('a[href]');if(!link)return;
  const url=new URL(link.href,location.href);
  if(url.origin!==location.origin||!url.pathname.endsWith('.html'))return;
  for(const key of ['audit','docs'])if(root.dataset[key]==='on')url.searchParams.set(key,'1');
  else url.searchParams.delete(key);
  link.href=url.href;});
apply();
revealFragment();
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


def _selection_identity(
    authority: str, interface: str, title: str, pointer: str, value: object
) -> str:
    """Use the shortest identity derivable within an authority and interface."""

    if interface == "python" and isinstance(value, Mapping):
        if value.get("unit") == "member" and isinstance(value.get("name"), str):
            return str(value["name"])
        prefix = str(value.get("module", "")) + "."
        if title.startswith(prefix):
            return title[len(prefix) :]
    if interface == "http-schemas":
        return _parts(pointer)[-1]
    if interface == "http-security-schemes":
        return _parts(pointer)[-1]
    if authority == "release" and ": " in title:
        return title.split(": ", 1)[1]
    if interface == "durable-state" and isinstance(value, Mapping):
        name = value.get("name")
        if isinstance(name, str):
            return name
    for prefix in (authority + ":configuration:", authority + ": ", "schemas: "):
        if title.startswith(prefix):
            return title[len(prefix) :]
    if interface == "process-protocol-schemas" and ": " in title:
        return title.rsplit(": ", 1)[-1]
    return title


def _authority_file(authority: str) -> str:
    return f"a-{_hash(authority)}.html"


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
        visible = canonical_bytes(value).decode("utf-8") if control else value
        kind = "string (JSON spelling for control characters)" if control else "string"
    else:
        visible = canonical_bytes(value).decode("utf-8")
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
            f"<section><h4><code>{_esc(pointer)}</code></h4>"
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
        "<h2>Contract</h2>"
        + render_human(closure, element, references)
        + '<details class="exact"><summary>Exact machine fields and pointers</summary>'
        + "".join(sections)
        + "</details></section>"
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


def _source_locations_html(source: Mapping[str, object], revision: str | None) -> str:
    locations: list[str] = []
    if isinstance(source.get("source"), Mapping):
        locations.append(_source_html(source, revision))
    for key in ("declarations", "bindings", "fixtures"):
        for location in cast(Sequence[Mapping[str, object]], source.get(key, ())):
            if isinstance(location.get("path"), str):
                locations.append(_source_html({"id": source["id"], "source": location}, revision))
    framework = source.get("framework")
    if isinstance(framework, Mapping):
        locations.append(
            "Framework: "
            + _esc(framework.get("module", ""))
            + "."
            + _esc(framework.get("symbol", ""))
        )
    return "<br>".join(dict.fromkeys(locations)) or f"<code>{_esc(source['id'])}</code>"


def _policy_element_file(pointer: str, owners: Mapping[str, str]) -> str:
    owner = _owning_element(pointer, owners)
    if owner is None:
        raise ContractAtlasError(f"policy definition has no contract element: {pointer}")
    return _element_file(owner)


def _source_anchor(identity: str) -> str:
    return "source-" + _hash(identity)


def _route_anchor(route: str) -> str:
    return "route-" + _hash(route)


def _audit_panel(
    closure: Mapping[str, object],
    element: Mapping[str, object],
    overlay: Mapping[str, object] | None,
    audit: Mapping[str, object] | None,
    source_revision: str | None,
    owners: Mapping[str, str],
    extent_groups: Sequence[str] = (),
) -> str:
    if audit is None or overlay is None:
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
        "<li>"
        + _link("source-authorities.html#" + _source_anchor(identity), identity)
        + " — "
        + _source_locations_html(sources[identity], source_revision)
        + "</li>"
        for identity in source_ids
    )
    decision_ids = cast(Sequence[str], overlay["extent_decision_ids"])
    decision_rows = []
    for identity in decision_ids:
        decision = decisions[identity]
        bounds = {
            key: value
            for key, value in decision.items()
            if key not in {"id", "owner", "source_pointer", "dimension", "unit", "policy", "rule"}
        }
        rule_id = "extent-rule/" + str(decision["rule"])
        rule_pointer = "/external_contract/extents/rules/" + _token(str(decision["rule"]))
        normative_rules = cast(
            Mapping[str, object],
            cast(
                Mapping[str, object],
                cast(Mapping[str, object], closure["external_contract"])["extents"],
            )["rules"],
        )
        rule_display = (
            _link(_policy_element_file(rule_pointer, owners), rule_id)
            if str(decision["rule"]) in normative_rules
            else f"<code>{_esc(decision['rule'])}</code>"
        )
        decision_rows.append(
            (
                f"<code>{_esc(decision['source_pointer'])}</code>",
                rule_display,
                _esc(decision["dimension"])
                + " · "
                + _esc(decision["unit"])
                + " · "
                + _esc(decision["policy"]),
                f"<code>{_esc(canonical_bytes(bounds).decode('utf-8'))}</code>",
            )
        )
    routes = "".join(
        f"<li>{_link('source-commands.html#' + _route_anchor(route), route)}</li>"
        for route in cast(Sequence[str], overlay["qualification_routes"])
    )
    body = '<aside class="audit" id="audit"><h2>Audit context · noncontractual</h2>'
    if extent_groups:
        body += (
            "<h3>Open extent qualification</h3><ul>"
            + "".join(
                f"<li>{_link(_qualification_file(identity), identity)}</li>"
                for identity in extent_groups
            )
            + "</ul>"
        )
    policy_ids = cast(Sequence[str], overlay["policy_ids"])
    if policy_ids:
        policy_definitions = {
            str(record["id"]): str(record["definition_pointer"])
            for records in cast(
                Mapping[str, Sequence[Mapping[str, object]]], audit["policies"]
            ).values()
            for record in records
        }
        body += (
            "<h3>Governing policies</h3><ul>"
            + "".join(
                "<li>"
                + _link(_policy_element_file(policy_definitions[identity], owners), identity)
                + "</li>"
                for identity in policy_ids
            )
            + "</ul>"
        )
    owned_policies = [
        str(record["id"])
        for records in cast(
            Mapping[str, Sequence[Mapping[str, object]]], audit["policies"]
        ).values()
        for record in records
        if str(record["definition_pointer"]) in cast(Sequence[str], element["pointers"])
    ]
    if owned_policies:
        body += (
            "<h3>Indexed applications</h3><ul>"
            + "".join(
                f"<li>{_link('p-' + _hash(identity) + '.html', identity)}</li>"
                for identity in owned_policies
            )
            + "</ul>"
        )
    body += (
        "<h3>Source associations</h3><ul>"
        + source_items
        + "</ul><h3>Qualification routes</h3><ul>"
        + routes
        + "</ul>"
    )
    if decision_rows:
        body += "<h3>Generated extent analysis</h3>" + _table_html(
            ("Exact subject", "Governing rule", "Decision", "Bounds or reason"), decision_rows
        )
    if element["interface"] == "configuration-environment":
        configuration_rows = []
        for identity in source_ids:
            source = sources[identity]
            for declaration in cast(Sequence[Mapping[str, object]], source.get("declarations", ())):
                configuration_rows.append(
                    (
                        "declaration",
                        "—",
                        _source_html({"id": identity, "source": declaration}, source_revision),
                        f"<code>{_esc(declaration.get('pointer', ''))}</code>",
                    )
                )
            for binding in cast(Sequence[Mapping[str, object]], source.get("bindings", ())):
                configuration_rows.append(
                    (
                        "consumer",
                        f"<code>{_esc(binding.get('consumer', ''))}</code>",
                        _source_html({"id": identity, "source": binding}, source_revision),
                        f"<code>{_esc(binding.get('expression', ''))}</code>",
                    )
                )
        if configuration_rows:
            body += "<h3>Configuration authority and bindings</h3>" + _table_html(
                ("Kind", "Consumer", "Source", "Authority or expression"), configuration_rows
            )
    if element["interface"] == "http-operations":
        pointer = cast(Sequence[str], element["pointers"])[0]
        value = _at(closure, pointer)
        if isinstance(value, Mapping) and isinstance(value.get("operationId"), str):
            application = _parts(pointer)[2]
            records = cast(
                Sequence[Mapping[str, object]],
                cast(
                    Mapping[str, object],
                    cast(Mapping[str, object], audit["trace"])["operation_qualification"],
                )["records"],
            )
            matched = [
                record
                for record in records
                if record["application"] == application
                and record["operation_id"] == value["operationId"]
            ]
            if len(matched) > 1:
                raise ContractAtlasError("HTTP operation has ambiguous structural binding")
            if matched:
                body += (
                    "<details><summary>Structural operation bindings</summary>"
                    + _value_html(
                        matched[0],
                        "/audit/operation-qualification/" + _token(str(value["operationId"])),
                    )
                    + "</details>"
                )
    return body + "</aside>"


def _authority_descriptions(closure: Mapping[str, object]) -> dict[str, str]:
    """Use current source-derived declarations, not a presentation-owned taxonomy."""

    boundaries = cast(Mapping[str, object], closure["boundaries"])
    descriptions = dict(cast(Mapping[str, str], boundaries["contract_authorities"]))
    external = cast(Mapping[str, object], closure["external_contract"])
    release = cast(Mapping[str, object], external["release"])
    publication = cast(Mapping[str, object], release["publication"])
    for key in ("distributions", "runtime_images"):
        for name, record in cast(Mapping[str, Mapping[str, object]], publication[key]).items():
            description = record.get("description")
            if isinstance(description, str) and description:
                descriptions[name] = description
    durable = cast(Mapping[str, object], external["durable_state"])
    for owner in cast(Sequence[Mapping[str, object]], durable["owners"]):
        description = descriptions.get(str(owner["distribution"]))
        if description:
            descriptions[str(owner["id"])] = description
    return descriptions


def _authority_relationships(
    closure: Mapping[str, object], authority: str, available: set[str]
) -> str:
    boundaries = cast(Mapping[str, object], closure["boundaries"])
    lines: list[str] = []

    def destination(name: str) -> str:
        return (
            _link(_authority_file(name), name)
            if name in available
            else f"<code>{_esc(name)}</code>"
        )

    for extension in cast(Sequence[Mapping[str, object]], boundaries["entry_point_extensions"]):
        group = str(extension["group"])
        owner = str(extension["owner"])
        providers = {
            str(item["distribution"])
            for item in cast(Sequence[Mapping[str, object]], extension["providers"])
        }
        if authority == owner:
            links = ", ".join(destination(name) for name in sorted(providers))
            lines.append(f"Defines extension group <code>{_esc(group)}</code>; providers: {links}.")
        elif authority in providers:
            lines.append(
                f"Provides extension group <code>{_esc(group)}</code> "
                f"defined by {destination(owner)}."
            )
    for protocol in cast(Sequence[Mapping[str, object]], boundaries["process_extensions"]):
        name = str(protocol["name"])
        owner = str(protocol["contract_owner"])
        providers = {
            str(item["distribution"])
            for item in cast(Sequence[Mapping[str, object]], protocol["providers"])
        }
        if authority == owner:
            links = ", ".join(destination(item) for item in sorted(providers))
            lines.append(f"Defines process protocol <code>{_esc(name)}</code>; providers: {links}.")
        elif authority in providers:
            lines.append(
                f"Implements process protocol <code>{_esc(name)}</code> "
                f"owned by {destination(owner)}."
            )
    return (
        "<h2>Declared relationships</h2><ul>"
        + "".join(f"<li>{item}</li>" for item in lines)
        + "</ul>"
        if lines
        else ""
    )


def _audit_marker(target: str) -> str:
    marker = cast(Mapping[str, str], AUDIT_PRESENTATION["extent_marker"])
    label = f"Open {marker['label'].lower()} qualification"
    return (
        '<span class="audit-cue"> ' + f'<a class="audit-marker" href="{_esc(target)}" '
        f'aria-label="{_esc(label)}" title="{_esc(label)}">{_esc(marker["glyph"])}</a></span>'
    )


def _extent_witness_associations(
    audit: Mapping[str, object] | None,
    owners: Mapping[str, str],
) -> tuple[dict[str, Mapping[str, object]], dict[str, tuple[str, ...]]]:
    if audit is None:
        return {}, {}
    trace = cast(Mapping[str, object], audit["trace"])
    witnesses: dict[str, Mapping[str, object]] = {}
    affected: dict[str, set[str]] = defaultdict(set)
    owned = sorted(owners, key=len, reverse=True)
    for witness in cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"]):
        if witness["association_status"] != "candidate" or witness["result_reference"] is not None:
            continue
        identity = str(witness["id"])
        if identity in witnesses:
            raise ContractAtlasError(f"duplicate extent witness: {identity}")
        witnesses[identity] = witness
        for subject in cast(Sequence[str], witness["subject_pointers"]):
            match = next(
                (
                    pointer
                    for pointer in owned
                    if subject == pointer or subject.startswith(pointer + "/")
                ),
                None,
            )
            if match is None:
                raise ContractAtlasError(f"open extent witness has no declared subject: {subject}")
            affected[owners[match]].add(identity)
    return witnesses, {identity: tuple(sorted(groups)) for identity, groups in affected.items()}


def _qualification_file(identity: str) -> str:
    return f"q-{_hash(identity)}.html"


def _audit_scope(groups: Sequence[str], *, label: str, count: int) -> str:
    if not groups:
        return ""
    links = "".join(
        f"<li>{_link(_qualification_file(identity), identity)}</li>" for identity in sorted(groups)
    )
    return (
        '<aside class="audit" id="audit-scope"><h2>Open extent qualifications in this scope</h2>'
        f"<p>{count} subjects · {_esc(label)}</p><ul>" + links + "</ul></aside>"
    )


def _qualification_page(
    closure: Mapping[str, object],
    witness: Mapping[str, object],
    owners: Mapping[str, str],
    elements: Mapping[str, Mapping[str, object]],
    source_revision: str | None,
) -> str:
    rule_pointer = str(witness["rule_pointer"])
    rule = _at(closure, rule_pointer)
    subject_rows = []
    owned = sorted(owners, key=len, reverse=True)
    for pointer in cast(Sequence[str], witness["subject_pointers"]):
        match = next(
            (item for item in owned if pointer == item or pointer.startswith(item + "/")), None
        )
        if match is None:
            raise ContractAtlasError(f"extent qualification has no contract subject: {pointer}")
        element = elements[owners[match]]
        subject_rows.append(
            "<li>"
            + _link(_element_file(str(element["id"])), str(element["title"]))
            + f" · <code>{_esc(pointer)}</code></li>"
        )
    scope_rows = []
    for scope in cast(Sequence[Mapping[str, object]], witness["test_scopes"]):
        location = scope.get("source")
        source = (
            _source_html({"id": scope["node_id"], "source": location}, source_revision)
            if isinstance(location, Mapping)
            else f"<code>{_esc(scope['node_id'])}</code>"
        )
        scope_rows.append(f"<li>{source} — {_esc(scope['scope'])}</li>")
    test_rows = "".join(
        "<li><code>"
        + _esc(node)
        + "</code> — "
        + _source_html(
            {"id": node, "source": {"path": node.split("::", 1)[0]}},
            source_revision,
        )
        + "</li>"
        for node in cast(Sequence[str], witness["test_node_ids"])
    )
    gate_rows = "".join(
        f"<li>{_link('source-commands.html#' + _route_anchor(route), route)}</li>"
        for route in cast(Sequence[str], witness["gates"])
    )
    rule_link = _link(
        _policy_element_file(rule_pointer, owners),
        str(witness["rule_id"]),
    )
    return (
        '<section id="audit"><h2>Open extent qualification</h2>'
        + f'<p class="lead">{_esc(witness["audit_scope"])}</p>'
        + f"<p>Recorded owner: <code>{_esc(witness['owner'])}</code>. "
        + f"Association: {_esc(witness['association_status'])}; "
        + f"executed result: {_esc(witness['result_reference'] or 'none')}.</p>"
        + f"<p>Governing declared rule: {rule_link}</p>"
        + _value_html(rule, rule_pointer)
        + "<h3>Exact affected subjects</h3><ul>"
        + "".join(subject_rows)
        + "</ul>"
        + (
            "<h3>What candidate checks actually cover</h3><ul>" + "".join(scope_rows) + "</ul>"
            if scope_rows
            else ""
        )
        + "<h3>Candidate commands</h3><ul>"
        + gate_rows
        + "</ul>"
        + "<h3>Candidate test associations</h3><ul>"
        + test_rows
        + "</ul></section>"
    )


def _shell(
    title: str,
    body: str,
    closure_sha256: str,
    *,
    audit: bool,
    documentation: bool,
    breadcrumbs: str = "",
    audit_only: bool = False,
    documentation_only: bool = False,
) -> bytes:
    controls = (
        '<label class="mode"><input id="audit-mode" type="checkbox"> Audit Mode</label>'
        if audit
        else ""
    ) + (
        '<label class="mode"><input id="docs-mode" type="checkbox"> Documentation Mode</label>'
        if documentation
        else ""
    )
    footer = (
        '<footer><details class="artifact-details"><summary>Exact artifacts</summary>'
        + _link("../riverhog-v1.json", "Exact Contract Closure")
        + (" · " + _link("../riverhog-v1-audit.json", "Bound Audit Record") if audit else "")
        + " · "
        + _link("manifest.json", "Artifact manifest")
        + f"<p>Closure SHA-256: <code>{closure_sha256}</code></p></details></footer>"
    )
    page = (
        f'<!doctype html><html lang="en" data-audit="{"on" if audit_only else "off"}" '
        f'data-docs="{"on" if documentation_only else "off"}" '
        f'data-audit-default="{"on" if audit_only else "off"}" '
        f'data-docs-default="{"on" if documentation_only else "off"}">'
        '<head><meta charset="utf-8"><meta name="viewport" '
        'content="width=device-width,initial-scale=1">'
        f"<title>{_esc(title)} · Riverhog Contract Render</title>"
        '<link rel="stylesheet" href="style.css"><script src="modes.js" defer></script></head>'
        "<body><main><header>"
        + breadcrumbs
        + controls
        + "</header>"
        + f"<h1>{_esc(title)}</h1>"
        + body
        + footer
        + "</main></body></html>"
    )
    # Checked candidate pages are reviewed in Git. Keep each HTML tag on its
    # own line so a changed contract value produces a bounded source diff.
    return (
        page.replace("><", ">\n<")
        .replace(">\n</code>", "></code>")
        .replace(">\n</td>", "></td>")
        .encode("utf-8")
    )


def _cli_tree(
    members: Sequence[Mapping[str, object]],
    affected: Mapping[str, tuple[str, ...]],
) -> str:
    by_path: dict[tuple[str, ...], Mapping[str, object]] = {}
    for element in members:
        path = command_path(cast(Sequence[str], element["pointers"])[0])
        if path in by_path:
            raise ContractAtlasError(f"duplicate CLI command path: {' '.join(path)}")
        by_path[path] = element
    for path in by_path:
        if len(path) > 1 and path[:-1] not in by_path:
            raise ContractAtlasError(f"CLI command lacks its parent: {' '.join(path)}")

    def branch(path: tuple[str, ...]) -> str:
        element = by_path[path]
        identity = str(element["id"])
        cue = _audit_marker(_element_file(identity) + "#audit") if affected.get(identity) else ""
        children = sorted(
            candidate
            for candidate in by_path
            if len(candidate) == len(path) + 1 and candidate[:-1] == path
        )
        nested = "<ul>" + "".join(branch(child) for child in children) + "</ul>" if children else ""
        return (
            f'<li data-element="{_esc(identity)}">'
            + _link(_element_file(identity), path[-1])
            + cue
            + nested
            + "</li>"
        )

    roots = sorted(path for path in by_path if len(path) == 1)
    if not roots:
        raise ContractAtlasError("CLI interface has no root command")
    return (
        '<h2>Command tree</h2><ul class="command-tree">'
        + "".join(branch(root) for root in roots)
        + "</ul>"
    )


def _table_html(
    headers: Sequence[str], rows: Sequence[Sequence[str]], *, selection: bool = False
) -> str:
    heading = "".join(f'<th scope="col">{_esc(item)}</th>' for item in headers)
    body = "".join(
        "<tr>"
        + "".join(
            f'<td data-label="{_esc(headers[index])}">{cell}</td>' for index, cell in enumerate(row)
        )
        + "</tr>"
        for row in rows
    )
    table_class = ' class="selection-table"' if selection else ""
    return (
        f'<div class="record-collection"><table{table_class}><thead><tr>'
        + heading
        + "</tr></thead><tbody>"
        + body
        + "</tbody></table></div>"
    )


def _owning_element(pointer: str, owners: Mapping[str, str]) -> str | None:
    matches = [owner for owner in owners if pointer == owner or pointer.startswith(owner + "/")]
    return owners[max(matches, key=len)] if matches else None


def _policy_application_pages(
    audit: Mapping[str, object],
    owners: Mapping[str, str],
    elements: Mapping[str, Mapping[str, object]],
    digest: str,
    documentation: bool,
) -> dict[str, bytes]:
    """Keep audit application indexes; definitions live in their authority elements."""

    applied_elements: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for overlay in cast(Sequence[Mapping[str, object]], audit["element_overlays"]):
        element = elements[str(overlay["id"])]
        for identity in cast(Sequence[str], overlay["policy_ids"]):
            applied_elements[identity].append(element)
    files: dict[str, bytes] = {}
    for records in cast(Mapping[str, Sequence[Mapping[str, object]]], audit["policies"]).values():
        for record in records:
            identity = str(record["id"])
            definition = _policy_element_file(str(record["definition_pointer"]), owners)
            path = f"p-{_hash(identity)}.html"
            applications = sorted(
                applied_elements[identity],
                key=lambda item: (str(item["authority"]), str(item["title"]), str(item["id"])),
            )
            by_authority: dict[str, list[Mapping[str, object]]] = defaultdict(list)
            for item in applications:
                by_authority[str(item["authority"])].append(item)

            def rows_for(items: Sequence[Mapping[str, object]]) -> list[tuple[str, str]]:
                return [
                    (
                        _link(_element_file(str(item["id"])), str(item["title"])),
                        f"<code>{_esc(cast(Sequence[str], item['pointers'])[0])}</code>",
                    )
                    for item in items
                ]

            if len(applications) > 150 and len(by_authority) > 1:
                authority_rows = []
                for authority, items in sorted(by_authority.items()):
                    child = f"p-{_hash(identity)}-a-{_hash(authority)}.html"
                    authority_rows.append((_link(child, authority), str(len(items))))
                    files[child] = _shell(
                        f"{identity}: {authority}",
                        '<aside class="audit">'
                        + _table_html(("Contract element", "Owned pointer"), rows_for(items))
                        + "</aside>",
                        digest,
                        audit=True,
                        documentation=documentation,
                        audit_only=True,
                        breadcrumbs="<p>"
                        + _link("index.html", "All authorities")
                        + " / "
                        + _link(definition, "Policy definition")
                        + " / "
                        + _link(path, "Indexed applications")
                        + "</p>",
                    )
                indexed = _table_html(("Authority", "Indexed elements"), authority_rows)
            else:
                indexed = (
                    _table_html(("Contract element", "Owned pointer"), rows_for(applications))
                    if applications
                    else "<p>No contract elements are indexed here.</p>"
                )
            scope_rows = []
            for pointer in cast(Sequence[str], record.get("applies_to", ())):
                owner = _owning_element(pointer, owners)
                target = (
                    _link(_element_file(owner), str(elements[owner]["title"]))
                    if owner
                    else f"<code>{_esc(pointer)}</code>"
                )
                scope_rows.append((target, f"<code>{_esc(pointer)}</code>"))
            files[path] = _shell(
                f"Indexed applications: {identity}",
                '<aside class="audit"><h2>Indexed contract elements</h2>'
                + indexed
                + (
                    "<h2>Declared scope pointers</h2>"
                    + _table_html(("Contract subject", "Exact scope"), scope_rows)
                    if scope_rows
                    else ""
                )
                + "</aside>",
                digest,
                audit=True,
                documentation=documentation,
                audit_only=True,
                breadcrumbs="<p>"
                + _link("index.html", "All authorities")
                + " / "
                + _link(definition, "Policy definition")
                + "</p>",
            )
    return files


def _audit_reference_pages(
    closure: Mapping[str, object],
    audit: Mapping[str, object],
    owners: Mapping[str, str],
    elements: Mapping[str, Mapping[str, object]],
    witnesses: Mapping[str, Mapping[str, object]],
    affected: Mapping[str, tuple[str, ...]],
    digest: str,
    documentation: bool,
    source_revision: str | None,
) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    trace = cast(Mapping[str, object], audit["trace"])

    def page(path: str, title: str, content: str, *, parent: str = "index.html") -> None:
        files[path] = _shell(
            title,
            '<aside class="audit">' + content + "</aside>",
            digest,
            audit=True,
            documentation=documentation,
            audit_only=True,
            breadcrumbs="<p>"
            + _link("index.html", "All authorities")
            + (" / " + _link(parent, "Audit references") if parent != "index.html" else "")
            + "</p>",
        )

    marker = cast(
        Mapping[str, str],
        cast(Mapping[str, object], audit["presentation"])["extent_marker"],
    )
    page(
        "audit-key.html",
        "Audit key",
        f"<p><strong>{_esc(marker['glyph'])} {_esc(marker['label'])}:</strong> "
        f"{_esc(marker['meaning'])}</p>",
    )
    counts = cast(Mapping[str, object], audit["counts"])
    count_rows = [
        (_esc(key.replace("_", " ")), f"<code>{_esc(value)}</code>")
        for key, value in sorted(counts.items())
        if not isinstance(value, Mapping)
    ]
    discovery = cast(Mapping[str, object], audit["discovery"])
    anomalies = cast(Mapping[str, int], discovery["anomalies"])
    coverage = cast(Mapping[str, int], discovery["projection_coverage"])
    meta_coverage = cast(
        Mapping[str, object], cast(Mapping[str, object], discovery["meta_closure"])["coverage"]
    )

    def measure_rows(values: Mapping[str, object]) -> list[tuple[str, str]]:
        return [
            (_esc(key.replace("_", " ")), f"<code>{_esc(value)}</code>")
            for key, value in sorted(values.items())
            if not isinstance(value, Mapping)
        ]

    breakdowns = "".join(
        "<details><summary>By "
        + _esc(name.replace("by_", "").replace("_", " "))
        + "</summary>"
        + _table_html(("Scope", "Elements"), measure_rows(cast(Mapping[str, object], counts[name])))
        + "</details>"
        for name in ("by_authority", "by_interface", "by_detector", "by_policy")
    )
    page(
        "accounting.html",
        "Accounting checks",
        _table_html(("Measure", "Recorded value"), count_rows)
        + "<h2>Discovery anomalies</h2>"
        + _table_html(("Check", "Count"), measure_rows(anomalies))
        + "<h2>Projection coverage</h2>"
        + _table_html(("Measure", "Count"), measure_rows(coverage))
        + "<h2>Protected channels</h2>"
        + _table_html(("Measure", "Count"), measure_rows(meta_coverage))
        + breakdowns
        + "<p>Inspect individual candidates, detections, dispositions, and resolutions in the "
        + _link("../riverhog-v1-audit.json", "exact bound Audit Record")
        + ".</p>",
    )
    source_rows = []
    fixture_rows = []
    source_counts = cast(Mapping[str, int], counts["by_source_authority"])
    for source in cast(Sequence[Mapping[str, object]], audit["sources"]):
        identity = str(source["id"])
        source_rows.append(
            (
                f'<code id="{_source_anchor(identity)}">{_esc(identity)}</code>',
                str(source_counts.get(identity, 0)),
                _source_locations_html(source, source_revision),
            )
        )
        for fixture in cast(Sequence[Mapping[str, object]], source.get("fixtures", ())):
            fixture_rows.append(
                (
                    _link("source-authorities.html#" + _source_anchor(identity), identity),
                    _source_html({"id": identity, "source": fixture}, source_revision),
                    f"<code>{_esc(fixture['sha256'])}</code>",
                )
            )
    page(
        "source-authorities.html",
        "Source authorities",
        _table_html(("Source identity", "Applications", "Executable locations"), source_rows),
        parent="sources.html",
    )
    page(
        "source-fixtures.html",
        "Source fixtures",
        _table_html(("Source identity", "Fixture", "SHA-256"), fixture_rows),
        parent="sources.html",
    )
    route_counts: dict[str, int] = defaultdict(int)
    for overlay in cast(Sequence[Mapping[str, object]], audit["element_overlays"]):
        for route in cast(Sequence[str], overlay["qualification_routes"]):
            route_counts[route] += 1
    witness_route_counts: dict[str, int] = defaultdict(int)
    for witness in cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"]):
        for route in cast(Sequence[str], witness["gates"]):
            witness_route_counts[route] += 1
    route_rows = [
        (
            f'<code id="{_route_anchor(route)}">{_esc(route)}</code>',
            str(route_counts.get(route, 0)),
            str(witness_route_counts.get(route, 0)),
        )
        for route in sorted(set(route_counts) | set(witness_route_counts))
    ]
    operation_rows = [
        (
            f"<code>{_esc(item['application'])}</code>",
            f"<code>{_esc(item['method'])} {_esc(item['path'])}</code>",
            _esc(item.get("classification", "")),
            _esc(item.get("client", "")),
        )
        for item in cast(
            Sequence[Mapping[str, object]],
            cast(Mapping[str, object], trace["operation_qualification"])["records"],
        )
    ]
    page(
        "source-commands.html",
        "Candidate qualification routes",
        _table_html(("Route", "Associated elements", "Candidate witness groups"), route_rows)
        + "<h3>Operation qualification bindings</h3>"
        + _table_html(("Application", "Operation", "Classification", "Client"), operation_rows),
        parent="sources.html",
    )
    qualification_rows = [
        (
            _link(_qualification_file(identity), identity),
            _esc(witness["audit_scope"]),
            str(len(cast(Sequence[str], witness["subject_pointers"]))),
        )
        for identity, witness in sorted(witnesses.items())
    ]
    read_witnesses = cast(Sequence[Mapping[str, object]], trace["read_authority_witnesses"])
    read_rows = [
        (
            f"<code>{_esc(item['id'])}</code>",
            _esc(item["scope"]),
            str(len(cast(Sequence[str], item["subject_pointers"]))),
        )
        for item in read_witnesses
    ]
    page(
        "qualifications.html",
        "Recorded qualifications",
        _table_html(("Open extent group", "Recorded scope", "Subjects"), qualification_rows)
        + "<h3>Read authority candidate associations</h3>"
        + _table_html(("Identity", "Recorded limitation", "Subjects"), read_rows),
        parent="sources.html",
    )
    page(
        "sources.html",
        "Sources and qualifications",
        "<ul>"
        + f"<li>{_link('source-authorities.html', 'Source authorities')}</li>"
        + f"<li>{_link('source-commands.html', 'Candidate qualification routes')}</li>"
        + f"<li>{_link('source-fixtures.html', 'Source fixtures')}</li>"
        + f"<li>{_link('qualifications.html', 'Recorded qualifications')}</li>"
        + "</ul>",
    )
    registry = cast(Mapping[str, object], trace["configuration_registry"])
    settings = cast(Sequence[Mapping[str, object]], registry["records"])
    setting_elements = {
        str(
            cast(Mapping[str, object], _at(closure, cast(Sequence[str], item["pointers"])[0]))["id"]
        ): str(item["id"])
        for item in elements.values()
        if item["interface"] == "configuration-environment"
    }
    setting_rows = [
        (
            _link(_element_file(setting_elements[str(item["id"])]), str(item["name"]))
            if str(item["id"]) in setting_elements
            else _esc(item["name"]),
            _esc(item["owner"]),
            _esc(", ".join(cast(Sequence[str], item["consumers"]))),
            _esc(", ".join(cast(Sequence[str], item["default_expressions"]))),
        )
        for item in settings
    ]
    page(
        "configuration-settings.html",
        "Environment setting comparison",
        _table_html(("Setting", "Owner", "Consumers", "Default expressions"), setting_rows),
        parent="configuration.html",
    )
    patterns = cast(Sequence[Mapping[str, object]], registry["patterns"])
    pattern_rows = [
        (
            _esc(item["owner"]),
            _esc(item.get("template", item["id"])),
            _esc(", ".join(cast(Sequence[str], item["consumers"]))),
            _esc(", ".join(cast(Sequence[str], item["settings"]))),
        )
        for item in patterns
    ]
    page(
        "configuration-families.html",
        "Parameterized setting families",
        _table_html(("Owner", "Template", "Consumers", "Settings"), pattern_rows),
        parent="configuration.html",
    )
    document_registry = cast(Mapping[str, object], trace["configuration_document_registry"])
    documents = cast(Sequence[Mapping[str, object]], document_registry["candidates"])
    document_elements = {
        _parts(cast(Sequence[str], item["pointers"])[0])[-1]: str(item["id"])
        for item in elements.values()
        if item["interface"] == "configuration"
    }
    document_rows = [
        (
            _link(_element_file(document_elements[str(item["id"])]), str(item["id"]))
            if str(item["id"]) in document_elements
            else _esc(item["id"]),
            _esc(item["owner"]),
            _esc(", ".join(cast(Sequence[str], item["consumers"]))),
            _esc(", ".join(cast(Sequence[str], item["input_shapes"]))),
            _source_html({"id": item["id"], "source": item["source"]}, source_revision),
        )
        for item in documents
    ]
    page(
        "configuration-documents.html",
        "Configuration document comparison",
        _table_html(("Document", "Owner", "Consumers", "Input shape", "Source"), document_rows),
        parent="configuration.html",
    )
    reconciliation = cast(Mapping[str, object], registry["coverage"])
    configuration_counts = cast(Mapping[str, object], registry["counts"])
    reconciliation_rows = [
        (_esc(key.replace("_", " ")), f"<code>{_esc(value)}</code>")
        for key, value in sorted(reconciliation.items())
    ]
    page(
        "configuration-reconciliation.html",
        "Configuration reconciliation",
        _table_html(("Check", "Recorded result"), reconciliation_rows)
        + "<details><summary>Exact registry counts and dispositions</summary>"
        + _value_html(configuration_counts, "/audit/trace/configuration_registry/counts")
        + _value_html(registry["dispositions"], "/audit/trace/configuration_registry/dispositions")
        + "</details>",
        parent="configuration.html",
    )
    page(
        "configuration.html",
        "Configuration comparison",
        "<ul>"
        + f"<li>{_link('configuration-settings.html', 'Environment settings')}</li>"
        + f"<li>{_link('configuration-families.html', 'Parameterized families')}</li>"
        + f"<li>{_link('configuration-documents.html', 'Configuration documents')}</li>"
        + f"<li>{_link('configuration-reconciliation.html', 'Discovery reconciliation')}</li>"
        + "</ul>",
    )
    boundaries = cast(Mapping[str, object], closure["boundaries"])
    components = cast(Sequence[Mapping[str, object]], boundaries["components"])
    descriptions = _authority_descriptions(closure)
    nodes: dict[str, tuple[str, str, str, str]] = {}
    edges: list[tuple[str, str, str, str]] = []
    semantic_interfaces: dict[str, tuple[tuple[str, str], ...]] = {}
    for component in components:
        name = str(component["distribution"])
        nodes[f"component:{name}"] = ("component", name, str(component["role"]), descriptions[name])
        for dependency in cast(Sequence[str], component["dependencies"]):
            edges.append((f"component:{name}", "depends on", f"component:{dependency}", "required"))
        for extra, dependencies in sorted(
            cast(Mapping[str, Sequence[str]], component["optional_dependencies"]).items()
        ):
            for dependency in dependencies:
                edges.append(
                    (
                        f"component:{name}",
                        "depends on",
                        f"component:{dependency}",
                        f"optional: {extra}",
                    )
                )
    for extension in cast(Sequence[Mapping[str, object]], boundaries["entry_point_extensions"]):
        group = str(extension["group"])
        node = f"extension-point:{group}"
        owner = str(extension["owner"])
        nodes[node] = ("extension point", group, owner, "")
        semantic_interfaces[node] = ((owner, "python"),)
        edges.append((f"component:{owner}", "owns extension point", node, ""))
        for provider in cast(Sequence[Mapping[str, object]], extension["providers"]):
            edges.append(
                (
                    f"component:{provider['distribution']}",
                    "implements extension point",
                    node,
                    str(provider["name"]),
                )
            )
    for protocol in cast(Sequence[Mapping[str, object]], boundaries["process_extensions"]):
        name = str(protocol["name"])
        node = f"process-protocol:{name}"
        owner = str(protocol["contract_owner"])
        nodes[node] = (
            "process protocol",
            name,
            owner,
            "",
        )
        support = str(protocol["binding_support"])
        semantic_interfaces[node] = (
            (owner, "python"),
            (support, "process-protocol"),
            (support, "process-protocol-operations"),
            (support, "process-protocol-schemas"),
        )
        edges.append((f"component:{owner}", "owns protocol", node, ""))
        edges.append(
            (
                f"component:{protocol['binding_support']}",
                "binds protocol",
                node,
                str(protocol["binding"]),
            )
        )
        for provider in cast(Sequence[Mapping[str, object]], protocol["providers"]):
            edges.append((f"component:{provider['distribution']}", "implements protocol", node, ""))
    runtime = cast(
        Mapping[str, Mapping[str, object]],
        cast(Mapping[str, object], boundaries["runtime_images"])["runtime"],
    )
    for name, image in sorted(runtime.items()):
        node = f"image:runtime:{name}"
        nodes[node] = ("runtime image", name, str(image["role"]), str(image["description"]))
        for distribution in cast(Sequence[str], image["distributions"]):
            edges.append((f"component:{distribution}", "packaged in", node, ""))
    publication = cast(
        Mapping[str, object],
        cast(
            Mapping[str, object],
            cast(Mapping[str, object], closure["external_contract"])["release"],
        )["publication"],
    )
    for root, unit in sorted(
        cast(Mapping[str, Mapping[str, object]], publication["installation_roots"]).items()
    ):
        method = str(unit["method"])
        node = f"installation:{method}"
        nodes[node] = (
            "installation",
            method,
            "release",
            "",
        )
        edges.append((f"component:{unit['distribution']}", "installed as", node, root))
    if any(source not in nodes or target not in nodes for source, _, target, _ in edges):
        raise ContractAtlasError("declared relationship has an unresolved endpoint")
    if len(edges) != len(set(edges)):
        raise ContractAtlasError("declared relationship repeats an exact edge")

    def node_link(identity: str) -> str:
        return _link("relationship-nodes.html#node-" + _hash(identity), nodes[identity][1])

    def edge_link(edge: tuple[str, str, str, str], label: str) -> str:
        return _link("relationship-edges.html#edge-" + _hash("\0".join(edge)), label)

    authority_names = {str(item["authority"]) for item in elements.values()}
    extension_links = []
    for identity, interfaces in sorted(semantic_interfaces.items()):
        kind, name, owner, purpose = nodes[identity]
        context_path = "extension-" + _hash(identity) + ".html"
        owner_edge = next(
            edge
            for edge in edges
            if edge[2] == identity and edge[1] in {"owns extension point", "owns protocol"}
        )
        interface_rows = []
        for authority, interface in interfaces:
            matching = [
                item
                for item in elements.values()
                if item["authority"] == authority and item["interface"] == interface
            ]
            if not matching:
                raise ContractAtlasError("extension lacks a declared semantic interface")
            inventory = _inventory_file(authority, interface)
            cue = (
                _audit_marker(inventory + "#audit-scope")
                if any(affected.get(str(item["id"])) for item in matching)
                else ""
            )
            interface_rows.append(
                (
                    _link(inventory, authority + " · " + INTERFACE_REGISTRY[interface].label) + cue,
                    str(len(matching)),
                )
            )
        providers = [
            edge
            for edge in edges
            if edge[2] == identity
            and edge[1] in {"implements extension point", "implements protocol"}
        ]
        provider_rows = [
            (
                _link(_authority_file(nodes[edge[0]][1]), nodes[edge[0]][1])
                if nodes[edge[0]][1] in authority_names
                else node_link(edge[0]),
                _esc(nodes[edge[0]][3]),
                edge_link(edge, "Exact relationship"),
            )
            for edge in sorted(providers)
        ]
        page(
            context_path,
            name,
            (f"<p>{_esc(purpose)}</p>" if purpose else "")
            + f"<p>Mechanism: {_esc(kind)}. "
            + "Owner: "
            + _link(_authority_file(owner), owner)
            + " ("
            + edge_link(owner_edge, "exact relationship")
            + "). "
            + node_link(identity)
            + ".</p>"
            + "<h2>Semantic interfaces</h2>"
            + _table_html(("Exact interface", "Elements"), interface_rows)
            + "<h2>Supplied implementations</h2>"
            + (
                _table_html(("Provider", "Maintained purpose", "Binding"), provider_rows)
                if provider_rows
                else ""
            ),
            parent="relationships.html",
        )
        extension_links.append(f"<li>{_link(context_path, name)}</li>")

    node_rows = []
    for identity, (kind, name, role, purpose) in sorted(nodes.items()):
        destination = _authority_file(name) if name in authority_names else None
        label = (
            _link("extension-" + _hash(identity) + ".html", name)
            if identity in semantic_interfaces
            else _link(destination, name)
            if destination
            else f"<code>{_esc(name)}</code>"
        )
        node_rows.append(
            (
                f'<code id="node-{_hash(identity)}">{_esc(identity)}</code>',
                _esc(kind),
                label,
                _esc(role),
                _esc(purpose),
            )
        )
    edge_rows = [
        (
            f'<span id="edge-{_hash(chr(0).join(edge))}"></span>' + node_link(edge[0]),
            _esc(edge[1]),
            node_link(edge[2]),
            _esc(edge[3]),
        )
        for edge in sorted(edges)
    ]
    page(
        "relationship-nodes.html",
        "Relationship nodes",
        _table_html(("Identity", "Kind", "Name", "Role or owner", "Maintained purpose"), node_rows),
        parent="relationships.html",
    )
    page(
        "relationship-edges.html",
        "Relationship edges",
        _table_html(("From", "Relationship", "To", "Scope or binding"), edge_rows),
        parent="relationships.html",
    )
    page(
        "relationships.html",
        "Declared relationships",
        "<ul>"
        + f"<li>{_link('relationship-nodes.html', 'Relationship nodes')} ({len(nodes)})</li>"
        + f"<li>{_link('relationship-edges.html', 'Relationship edges')} ({len(edges)})</li>"
        + "</ul><h2>Extension and process protocol contexts</h2><ul>"
        + "".join(extension_links)
        + "</ul>",
    )
    identities = cast(Mapping[str, object], audit["source_identities"])
    identity_rows = [
        (_esc(key), f"<code>{_esc(value)}</code>") for key, value in sorted(identities.items())
    ]
    identity_rows.extend(
        (
            ("Contract Closure SHA-256", f"<code>{_esc(digest)}</code>"),
            ("Audit Record SHA-256", f"<code>{_esc(canonical_sha256(audit))}</code>"),
        )
    )
    page(
        "identities.html",
        "Snapshot identities",
        _table_html(("Identity", "SHA-256"), identity_rows)
        + "<p>"
        + _link("../riverhog-v1.json", "Exact Contract Closure")
        + " · "
        + _link("../riverhog-v1-audit.json", "Exact bound Audit Record")
        + " · "
        + _link("manifest.json", "Render manifest")
        + "</p>",
    )
    for identity, witness in sorted(witnesses.items()):
        page(
            _qualification_file(identity),
            f"Extent qualification: {identity}",
            _qualification_page(closure, witness, owners, elements, source_revision),
            parent="qualifications.html",
        )
    return files


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
    documentation_available = bool(explanations or guides)
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
    descriptions = _authority_descriptions(closure)
    missing = {authority for authority, _interface in groups} - set(descriptions)
    if missing:
        raise ContractAtlasError(f"authorities lack source-backed descriptions: {sorted(missing)}")
    witnesses, affected = _extent_witness_associations(audit, owners)
    elements_by_id = {str(element["id"]): element for element in elements}
    available_authorities = {authority for authority, _interface in groups}
    files: dict[str, bytes] = {"style.css": _STYLES.encode(), "modes.js": _MODES.encode()}
    root_entries: list[str] = []
    for authority in sorted(available_authorities, key=lambda name: (name != "release", name)):
        interfaces = sorted(
            (
                (interface, groups[(authority, interface)])
                for a, interface in groups
                if a == authority
            ),
            key=lambda pair: (INTERFACE_REGISTRY[pair[0]].order, pair[0]),
        )
        authority_elements = [element for _interface, members in interfaces for element in members]
        authority_affected = [
            element for element in authority_elements if affected.get(str(element["id"]))
        ]
        authority_groups = sorted(
            {group for element in authority_affected for group in affected[str(element["id"])]}
        )
        authority_cue = (
            _audit_marker(_authority_file(authority) + "#audit-scope") if authority_groups else ""
        )
        interface_rows: list[str] = []
        root_interface_links: list[str] = []
        for interface, members in interfaces:
            inventory_path = _inventory_file(authority, interface)
            descriptor = INTERFACE_REGISTRY[interface]
            label = descriptor.label
            purpose = descriptor.purpose.format(label=label, authority=authority)
            interface_affected = [
                element for element in members if affected.get(str(element["id"]))
            ]
            interface_groups = sorted(
                {group for element in interface_affected for group in affected[str(element["id"])]}
            )
            interface_cue = (
                _audit_marker(inventory_path + "#audit-scope") if interface_groups else ""
            )
            root_interface_links.append(
                '<div class="authority-interface">'
                + _link(inventory_path, label)
                + interface_cue
                + f' <span class="meta">({len(members)})</span></div>'
            )
            interface_rows.append(
                '<tr><td data-label="Interface">'
                + _link(inventory_path, label)
                + interface_cue
                + f'</td><td data-label="Elements">{len(members)}</td>'
                + f'<td data-label="Purpose">{_esc(purpose)}</td></tr>'
            )
            rows: list[str] = []
            list_rows: list[str] = []
            fact_count = 0
            ordered = sorted(
                members, key=lambda item: (str(item["title"]).casefold(), str(item["id"]))
            )
            if interface == "python":
                exports: dict[str, Mapping[str, object]] = {}
                children: dict[str, list[Mapping[str, object]]] = defaultdict(list)
                for element in ordered:
                    value = cast(
                        Mapping[str, object],
                        _at(closure, cast(Sequence[str], element["pointers"])[0]),
                    )
                    identity = _parts(cast(Sequence[str], element["pointers"])[0])[-1]
                    if value["unit"] == "member":
                        children[str(value["owner"])].append(element)
                    else:
                        exports[identity] = element
                if set(children) - set(exports):
                    raise ContractAtlasError(
                        "Python member has no declared parent in its inventory"
                    )
                ordered = []
                for identity, element in sorted(exports.items()):
                    ordered.append(element)
                    ordered.extend(
                        sorted(children.get(identity, []), key=lambda item: str(item["title"]))
                    )
            local_names = {
                str(item["id"]): _selection_identity(
                    authority,
                    interface,
                    str(item["title"]),
                    cast(Sequence[str], item["pointers"])[0],
                    _at(closure, cast(Sequence[str], item["pointers"])[0]),
                )
                for item in ordered
            }
            name_scopes: dict[str, str] = {}
            for item in ordered:
                identity = str(item["id"])
                if interface == "python":
                    value = cast(
                        Mapping[str, object],
                        _at(closure, cast(Sequence[str], item["pointers"])[0]),
                    )
                    name_scopes[identity] = str(
                        value["owner"] if value.get("unit") == "member" else value["module"]
                    )
                else:
                    name_scopes[identity] = interface
            name_counts = Counter(
                (name_scopes[identity], name) for identity, name in local_names.items()
            )
            current_module: str | None = None
            for element in ordered:
                identity = str(element["id"])
                pointer = cast(Sequence[str], element["pointers"])[0]
                record_value = _at(closure, pointer)
                is_member = (
                    interface == "python"
                    and isinstance(record_value, Mapping)
                    and record_value.get("unit") == "member"
                )
                if interface == "python" and isinstance(record_value, Mapping):
                    module = str(record_value["module"])
                    if module != current_module:
                        rows.append(
                            '<tr class="module"><th colspan="2"><code>'
                            + _esc(module)
                            + "</code></th></tr>"
                        )
                        current_module = module
                cue = (
                    _audit_marker(_element_file(identity) + "#audit")
                    if affected.get(identity)
                    else ""
                )
                short_name = local_names[identity]
                display_name = (
                    short_name
                    if name_counts[(name_scopes[identity], short_name)] == 1
                    else str(element["title"])
                )
                name = _link(_element_file(identity), display_name) + cue
                if interface == "python":
                    kind = cast(
                        Mapping[str, object], cast(Mapping[str, object], record_value)["contract"]
                    )["kind"]
                    fact = f"<code>{_esc(kind)}</code>"
                elif interface == "compatibility-guarantees":
                    fact = f'<span class="comparison-promise">{_esc(record_value)}</span>'
                elif interface == "cli":
                    fact = ""
                else:
                    fact = inventory_fact(interface, record_value, pointer)
                fact_count += bool(fact)
                member_class = ' class="member"' if is_member else ""
                rows.append(
                    f'<tr data-element="{_esc(identity)}"{member_class}>'
                    + '<td data-label="Element">'
                    + name
                    + '<td data-label="Recorded fact">'
                    + fact
                    + "</td></tr>"
                )
                list_rows.append(f'<li data-element="{_esc(identity)}">{name}</li>')
                doc_body = ""
                if identity in explanations:
                    doc_body = (
                        '<aside class="documentation" id="documentation">'
                        "<h2>Documentation · noncontractual</h2><p>"
                        + _esc(explanations[identity])
                        + "</p></aside>"
                    )
                breadcrumbs = (
                    "<p>"
                    + _link("index.html", "All authorities")
                    + " / "
                    + _link(_authority_file(authority), authority)
                    + " / "
                    + _link(inventory_path, label)
                    + "</p>"
                )
                context = (
                    f'<p class="context">{_esc(descriptions[authority])} ' + _esc(purpose) + "</p>"
                )
                related = cast(Sequence[str], element["related_element_ids"])
                related_body = (
                    "<section><h2>Related contract elements</h2><ul>"
                    + "".join(
                        f"<li>{_link(_element_file(item), str(elements_by_id[item]['title']))}</li>"
                        for item in related
                    )
                    + "</ul></section>"
                    if related
                    else ""
                )
                files[_element_file(identity)] = _shell(
                    str(element["title"]),
                    context
                    + contract_body(closure, element, owners=owners)
                    + related_body
                    + _audit_panel(
                        closure,
                        element,
                        overlays.get(identity),
                        audit,
                        source_revision,
                        owners,
                        affected.get(identity, ()),
                    )
                    + doc_body,
                    closure_sha256,
                    audit=audit is not None,
                    documentation=documentation_available,
                    breadcrumbs=breadcrumbs,
                )
            heading = (
                "Declared kind"
                if interface == "python"
                else "Owned promise"
                if interface == "compatibility-guarantees"
                else "Recorded comparison facts"
            )
            inventory = (
                _cli_tree(members, affected)
                if interface == "cli"
                else '<ul class="selection-list">' + "".join(list_rows) + "</ul>"
                if not fact_count
                else '<div class="record-collection">'
                '<table class="selection-table inventory-table">'
                "<thead><tr><th>Element</th><th>"
                + _esc(heading)
                + "</th></tr></thead><tbody>"
                + "".join(rows)
                + "</tbody></table></div>"
            )
            files[inventory_path] = _shell(
                f"{authority} · {label}",
                f'<p class="context">{_esc(descriptions[authority])}</p>'
                + f"<p>{_esc(purpose)}</p>"
                + inventory
                + _audit_scope(
                    interface_groups, label=f"{authority} · {label}", count=len(interface_affected)
                ),
                closure_sha256,
                audit=audit is not None,
                documentation=documentation_available,
                breadcrumbs="<p>"
                + _link("index.html", "All authorities")
                + " / "
                + _link(_authority_file(authority), authority)
                + "</p>",
            )
        authority_body = (
            f'<p class="lead">{_esc(descriptions[authority])}</p>'
            + _authority_relationships(closure, authority, available_authorities)
            + '<h2>Interfaces</h2><div class="record-collection">'
            '<table class="selection-table"><thead><tr>'
            "<th>Interface</th><th>Elements</th><th>Purpose</th></tr></thead><tbody>"
            + "".join(interface_rows)
            + "</tbody></table></div>"
            + _audit_scope(authority_groups, label=authority, count=len(authority_affected))
        )
        files[_authority_file(authority)] = _shell(
            authority,
            authority_body,
            closure_sha256,
            audit=audit is not None,
            documentation=documentation_available,
            breadcrumbs="<p>" + _link("index.html", "All authorities") + "</p>",
        )
        root_entries.append(
            f'<article class="authority-card" data-authority="{_esc(authority)}"><h3>'
            + _link(_authority_file(authority), authority)
            + authority_cue
            + '</h3><p class="authority-description">'
            + _esc(descriptions[authority])
            + '</p><div class="authority-interfaces">'
            + "".join(root_interface_links)
            + "</div></article>"
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
            documentation_only=True,
            breadcrumbs="<p>" + _link("index.html", "All authorities") + "</p>",
        )
    body = ""
    if audit is not None:
        presentation = cast(Mapping[str, object], audit["presentation"])
        routes = cast(Sequence[Mapping[str, str]], presentation["reference_routes"])
        audit_references = (
            '<details class="references"><summary>Audit references</summary><ul>'
            + "".join(f"<li>{_link(route['path'], route['label'])}</li>" for route in routes)
            + "</ul></details>"
        )
        body += '<aside class="audit audit-references">' + audit_references + "</aside>"
        body += "<noscript>" + audit_references + "</noscript>"
    if guides:
        documentation_references = (
            '<details class="references"><summary>Documentation references</summary><ul>'
            + "".join(guide_links)
            + "</ul></details>"
        )
        body += (
            '<aside class="documentation documentation-references">'
            + documentation_references
            + "</aside>"
        )
        body += "<noscript>" + documentation_references + "</noscript>"
    body += (
        f'<h2>Authorities and interfaces</h2><p class="meta">{len(elements)} declared '
        'contract elements</p><div class="authority-filter">'
        '<label for="authority-filter">Filter by authority name</label>'
        '<input id="authority-filter" type="search" autocomplete="off" '
        'aria-controls="authority-cards">'
        '<span id="authority-filter-count" class="meta" role="status" aria-live="polite"></span>'
        '</div><div id="authority-cards" class="authority-cards">'
        + "".join(root_entries)
        + "</div>"
    )
    files["index.html"] = _shell(
        "Riverhog v1 Contract Render",
        body,
        closure_sha256,
        audit=audit is not None,
        documentation=documentation_available,
    )
    files.update(
        _policy_application_pages(
            audit, owners, elements_by_id, closure_sha256, documentation_available
        )
        if audit is not None
        else {}
    )
    if audit is not None:
        files.update(
            _audit_reference_pages(
                closure,
                audit,
                owners,
                elements_by_id,
                witnesses,
                affected,
                closure_sha256,
                documentation_available,
                source_revision,
            )
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
            resolved = (
                posixpath.normpath(posixpath.join(posixpath.dirname(source), target.path))
                if target.path
                else source
            )
            if resolved in allowed_machine and not target.fragment:
                continue
            if resolved not in files:
                raise ContractAtlasError(f"render has an unresolved local link: {source} -> {href}")
            if target.fragment and (
                resolved not in parsed or target.fragment not in parsed[resolved].anchors
            ):
                raise ContractAtlasError(f"render has an unresolved anchor: {source} -> {href}")
