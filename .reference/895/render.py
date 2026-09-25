"""Static HTML adapter for the #895 reference, not a second contract model."""
from __future__ import annotations

import hashlib
import html
import json
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote

from records import BoundaryError, canonical_json_bytes, digest, token, validate_closure, validate_pair, reference_targets

LABELS = {"python": "Python", "compatibility-guarantees": "Compatibility Guarantees", "http-schemas": "HTTP Schemas", "extent": "Extent Contract"}
STYLE = """
*{box-sizing:border-box}body{margin:0;background:white;color:#242424;font:16px/1.55 system-ui,sans-serif}main{max-width:1100px;margin:auto;padding:28px 28px 60px}header{border-bottom:1px solid #ccc;padding-bottom:16px;margin-bottom:24px}header p{margin:6px 0}h1{font-size:28px;overflow-wrap:anywhere}h2{font-size:21px;margin-top:28px}h3{font-size:17px}a{color:#174e72;text-underline-offset:3px}a:focus-visible,input:focus-visible{outline:3px solid #174e72;outline-offset:3px}table{border-collapse:collapse;width:100%;table-layout:fixed;margin:14px 0 22px}th,td{border-bottom:1px solid #ddd;text-align:left;vertical-align:top;padding:9px 12px;overflow-wrap:anywhere}th{background:#f5f5f5;font-size:14px}th:first-child{width:36%}td table{margin:0;font-size:14px}td table th:first-child{width:36%}code{font:0.9em/1.55 ui-monospace,monospace;white-space:pre-wrap;overflow-wrap:anywhere;tab-size:4}.prose{font:inherit}.kind{font-size:12px;color:#555;display:block}.member td:first-child{padding-left:30px}.audit-note{font-size:13px}.audit-panel{border-left:3px solid #89969f;padding:4px 18px;margin-top:30px}.audit-note,.audit-panel{display:none}html[data-audit=on] .audit-note{display:block}html[data-audit=on] .audit-panel{display:block}.review{font-size:13px;color:#555}.mode{display:none}html[data-js=yes] .mode{display:inline-block}details{margin:12px 0}summary{cursor:pointer}footer{border-top:1px solid #ddd;margin-top:32px;padding-top:16px;font-size:13px}label{cursor:pointer}p{overflow-wrap:anywhere}@media(max-width:700px){main{padding:18px 12px 40px}body{font-size:15px}th,td{padding:8px 6px}h1{font-size:24px}td table{font-size:13px}}@media print{.mode{display:none!important}main{max-width:none;padding:0}}
"""
MODES = """'use strict';
const root = document.documentElement;
const control = document.getElementById('audit-mode');
root.dataset.js = 'yes';
function displayMode(enabled) {
  root.dataset.audit = enabled && !control.disabled ? 'on' : 'off';
  control.checked = root.dataset.audit === 'on';
}
function apply() { displayMode(new URL(location.href).searchParams.get('audit') === '1'); }
control.addEventListener('change', () => {
  const url = new URL(location.href);
  if (control.checked) url.searchParams.set('audit', '1');
  else url.searchParams.delete('audit');
  history.pushState(null, '', url); apply();
});
addEventListener('popstate', apply);
document.addEventListener('click', event => {
  const link = event.target.closest('a[href]');
  if (!link) return;
  const url = new URL(link.href, location.href);
  if (url.origin !== location.origin || !url.pathname.endsWith('.html')) return;
  if (root.dataset.audit === 'on') url.searchParams.set('audit','1');
  else url.searchParams.delete('audit');
  link.href = url.href;
});
apply();
"""


def esc(text: str) -> str:
    return html.escape(text, quote=True).replace("\r", "&#13;")


def key_id(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def element_file(identity: str) -> str:
    return "e-" + key_id(identity) + ".html"


def inventory_file(authority: str, interface: str) -> str:
    return "i-" + key_id(authority + "\0" + interface) + ".html"


def link(target: str, text: str) -> str:
    return f'<a href="{esc(target)}">{esc(text)}</a>'


def literal(value: Any, pointer: str = "", *, typed: bool = True, prose: bool = False) -> str:
    if isinstance(value, str):
        # HTML cannot carry NUL literally. Show a named JSON spelling for control
        # characters rather than silently changing or dropping them.
        encoded = any(ord(c) < 32 and c not in "\n\t\r" for c in value)
        text = json.dumps(value, ensure_ascii=False) if encoded else value
        kind = "string (JSON spelling)" if encoded else "empty string" if not value else "string"
    else:
        text = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        kind = "null" if value is None else "boolean" if type(value) is bool else "integer" if type(value) is int else "number" if type(value) is float else "empty array" if value == [] else "empty object"
    cls = ' class="prose"' if prose else ''
    suffix = f'<span class="kind">{kind}</span>' if typed else ''
    return f'<code{cls} data-literal="{esc(pointer)}">{esc(text)}</code>{suffix}' 


def value_html(value: Any, pointer: str = "", references: dict[str, tuple[str, str]] | None = None) -> str:
    if not isinstance(value, (dict, list)) or not value:
        text = literal(value, pointer)
        if references is not None:
            text = '<span id="v-' + key_id(pointer) + '">' + text + '</span>'
            if pointer in references:
                target, suffix = references[pointer]
                text += ' ' + link(element_file(target) + '#v-' + key_id(suffix), 'Definition')
        return text
    items = sorted(value.items()) if isinstance(value, dict) else enumerate(value)
    rows = []
    for k, v in items:
        child = pointer + "/" + token(str(k))
        rows.append(f'<tr><td><code>{esc(str(k))}</code></td><td>{value_html(v, child, references)}</td></tr>')
    label = "Field" if isinstance(value, dict) else "Index (source order)"
    anchor = ' id="v-' + key_id(pointer) + '"' if references is not None else ''
    return f'<table{anchor} data-container="{esc(pointer)}"><thead><tr><th>{label}</th><th>Recorded value</th></tr></thead><tbody>{"".join(rows)}</tbody></table>'


def contract_body(identity: str, record: dict[str, Any], records: dict[str, Any] | None = None) -> str:
    # Does not accept Audit Record arguments; identical in both views.
    prefix = '/contract/schema' if record['interface'] == 'python' else ''
    references = {prefix + p: (target, suffix) for p, target, suffix in reference_targets(records or {identity: record}, identity)}
    if record['interface'] == 'python':
        metadata = ''.join('<p><code>' + esc(k) + '</code>: ' + literal(v, '/' + token(k), typed=False) + '</p>' for k, v in sorted(record['value'].items()) if k != 'contract')
        content = metadata + '<h3>Declared structure</h3>' + value_html(record['value']['contract'], '/contract', references)
    elif record['interface'] == 'compatibility-guarantees':
        content = '<p>' + literal(record['value'], typed=False, prose=True) + '</p>'
    else:
        content = value_html(record['value'], references=references)
    return f'<section data-contract="{esc(identity)}" id="contract"><h2>Contract</h2>{content}</section>' 



def source_links(sources: list[dict[str, Any]], revision: str) -> str:
    links = []
    for source in sources:
        location = source.get("source", {})
        path = location.get("path") if isinstance(location, dict) else None
        if path:
            posix = PurePosixPath(path)
            if posix.is_absolute() or ".." in posix.parts or "\\" in path:
                raise BoundaryError("unsafe source path")
            url = "https://github.com/nashspence/riverhog/blob/" + revision + "/" + quote(path, safe="/")
            line = location.get("line")
            if type(line) is int and line > 0:
                url += f"#L{line}"
            label = path + ("::" + str(location["symbol"]) if location.get("symbol") else "")
            links.append(link(url, label))
    return "<p>" + "<br>".join(links) + "</p>" if links else "<p>No executable source location supplied in this slice.</p>"


def audit_panel(identity: str, audit: dict[str, Any] | None, data: dict[str, Any] | None) -> str:
    if audit is None or data is None:
        return ""
    overlay = data["overlays"][identity]
    content = '<aside class="audit-panel" id="audit"><h2>Audit context — not contract</h2>'
    content += source_links(overlay["sources"], audit["source_revision"])
    if overlay["moved_fields"]:
        content += '<h3>Source annotations and authoring requirements</h3>' + value_html(overlay["moved_fields"])
    analyses = [a for a in data["extent_analysis"] if a["target"] == identity]
    if analyses:
        content += '<h3>Recorded extent analysis</h3><p>Legacy analysis is retained for review. Its labels do not supply a new limit or guarantee.</p>' + value_html(analyses)
    ids = {a["id"] for a in analyses}
    for witness in data["witnesses"]:
        if ids.intersection(witness["analysis_ids"]):
            content += '<h3>Recorded witness association</h3><p>Candidate scope, not an executed result or a new promise. Any broader group scope remains in the original record.</p>' + value_html(witness)
    content += '<details><summary>Exact source-value partition</summary>' + value_html({"source_pointer": overlay["source_pointer"], "original_value_sha256": overlay["original_value_sha256"]}) + '</details></aside>'
    return content


def shell(title: str, body: str, closure: dict[str, Any], audit: dict[str, Any] | None, breadcrumbs: str = "") -> bytes:
    checkbox = '<label class="mode"><input id="audit-mode" type="checkbox"' + ('' if audit else ' disabled') + '> Audit Mode</label>'
    availability = '' if audit else '<p class="review">Audit Record not supplied; this is not a clean-audit result.</p>'
    header = '<header><p class="review">NON-AUTHORITATIVE #895 REFERENCE · selected records, not the full contract or an accepted freeze</p>' + breadcrumbs + checkbox + availability + '<noscript><p>Contract-only reading. Audit augmentation requires JavaScript.</p></noscript></header>'
    footer = '<footer>' + link('closure.json','Exact reference Closure')
    if audit:
        footer += ' · ' + link('audit.json','Bound Audit Record')
    footer += ' · ' + link('manifest.json','Artifact identities') + '<p>Closure SHA-256: <code>' + digest(closure) + '</code></p></footer>'
    return ('<!doctype html><html lang="en" data-audit="off"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>' + esc(title) + '</title><link rel="stylesheet" href="style.css"><script src="modes.js" defer></script></head><body><main>' + header + '<h1>' + esc(title) + '</h1>' + body + footer + '</main></body></html>').encode()


def build_pages(closure: dict[str, Any], audit: dict[str, Any] | None) -> dict[str, bytes]:
    records = validate_closure(closure)
    data = validate_pair(closure, audit) if audit else None
    affected: set[str] = set()
    if data:
        analysis_targets = {a['id']: a['target'] for a in data['extent_analysis']}
        for witness in data['witnesses']:
            if witness['record'].get('unestablished_claims'):
                affected.update(analysis_targets[a] for a in witness['analysis_ids'])
    def cue(identity: str) -> str:
        if identity not in affected:
            return ''
        return '<span class="audit-note">' + link(element_file(identity) + '?audit=1#audit', 'Recorded unestablished audit claims') + '</span>'
    groups: dict[tuple[str, str], list[tuple[str, dict[str, Any]]]] = defaultdict(list)
    for identity, record in sorted(records.items()):
        if record["interface"] not in LABELS:
            raise BoundaryError("unreviewed interface renderer")
        groups[(record["authority"], record["interface"])].append((identity, record))
    files = {"style.css": STYLE.encode(), "modes.js": MODES.encode(), "closure.json": canonical_json_bytes(closure)}
    if audit:
        files["audit.json"] = canonical_json_bytes(audit)
    root_rows = []
    for (authority, interface), entries in sorted(groups.items()):
        destination = inventory_file(authority, interface)
        root_rows.append('<tr><td><code>' + esc(authority) + '</code></td><td>' + link(destination, LABELS[interface]) + f' · {len(entries)}' + ('<span class="audit-note">' + link(destination + '?audit=1', 'Contains recorded unestablished audit claims') + '</span>' if any(k in affected for k, _ in entries) else '') + '</td></tr>')
        rows = []
        if interface == "python":
            by_module = defaultdict(list)
            for identity, record in entries:
                by_module[record["value"]["module"]].append((identity, record))
            body = ''
            for module, members in sorted(by_module.items()):
                by_owner = defaultdict(list)
                for identity, record in members:
                    by_owner[record["value"].get("owner") if record["value"]["unit"] == "member" else None].append((identity, record))
                ordered = []
                for item in sorted(by_owner[None], key=lambda t:(t[1]["name"],t[0])):
                    ordered.append((item, False))
                    ordered.extend((child, True) for child in sorted(by_owner.get(item[1]["name"], []), key=lambda t:(t[1]["name"],t[0])))
                if len(ordered) != len(members):
                    raise BoundaryError("Python inventory would omit a member")
                rows = []
                for (identity, record), member in ordered:
                    rows.append('<tr' + (' class="member"' if member else '') + ' data-element="' + esc(identity) + '"><td>' + link(element_file(identity), record["name"]) + cue(identity) + '</td><td>' + literal(record["value"]["contract"]["kind"], '/contract/kind', typed=False) + '</td></tr>')
                body += '<h2><code>' + esc(module) + '</code></h2><table><thead><tr><th>Public identity</th><th>Declared kind</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table>'
        else:
            for identity, record in sorted(entries, key=lambda t:(t[1]["name"],t[0])):
                v = record["value"]
                fact = v if interface == "compatibility-guarantees" or type(v) is bool else v.get("type", v.get("policy", "not declared"))
                rows.append('<tr data-element="' + esc(identity) + '"><td>' + link(element_file(identity), record["name"]) + cue(identity) + '</td><td>' + literal(fact, typed=False, prose=interface == "compatibility-guarantees") + '</td></tr>')
            label = 'Declared promise' if interface == 'compatibility-guarantees' else 'Recorded type / policy'
            body = '<table><thead><tr><th>Element</th><th>' + label + '</th></tr></thead><tbody>' + ''.join(rows) + '</tbody></table>'
        files[destination] = shell(authority + ' · ' + LABELS[interface], body, closure, audit, link('index.html','Reference index'))
        for identity, record in entries:
            breadcrumbs = '<p>' + link('index.html','Reference index') + ' / ' + link(destination, authority + ' · ' + LABELS[interface]) + '</p>'
            files[element_file(identity)] = shell(record['name'], contract_body(identity, record, records) + audit_panel(identity, audit, data), closure, audit, breadcrumbs)
    body = f'<p>{len(records)} selected contract records. These are fixture/adapter counts, not whole-repository discovery coverage.</p><table><thead><tr><th>Authority</th><th>Interface · records</th></tr></thead><tbody>' + ''.join(root_rows) + '</tbody></table>'
    if data:
        body += '<aside class="audit-panel"><h2>Audit scope</h2>' + value_html(data['discovery_accounting']) + '</aside>'
    files['index.html'] = shell('Contract reference slice', body, closure, audit)
    return files


def emit(closure: dict[str, Any], audit: dict[str, Any] | None, output: Path) -> None:
    files = build_pages(closure, audit)
    if output.exists() and any(output.iterdir()):
        raise BoundaryError("use a new/empty output directory; no implicit artifact deletion")
    manifest = {"format": "riverhog-reference-895-artifacts/v1", "closure_sha256": digest(closure), "audit_sha256": digest(audit) if audit else None, "files": {p: hashlib.sha256(b).hexdigest() for p, b in sorted(files.items())}}
    files['manifest.json'] = canonical_json_bytes(manifest)  # Does not hash itself.
    output.mkdir(parents=True, exist_ok=True)
    for path, content in sorted(files.items()):
        (output / path).write_bytes(content)
