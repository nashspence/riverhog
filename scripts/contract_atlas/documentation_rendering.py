"""Documentation panels consume the archived audit; the shared renderer owns the UI."""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict
from collections.abc import Mapping
from typing import Any

from .documentation_audit import check_record
from .documentation_markdown import document_route
from .model import ContractAtlasError, canonical_bytes, canonical_sha256


def augment(
    files: dict[str, bytes],
    closure: Mapping[str, Any],
    document: Mapping[str, Any] | None,
    record: Mapping[str, Any] | None,
) -> None:
    from .html_rendering import _element_file, _esc, _hash, _link, _shell, _table_html, _value_html

    digest = canonical_sha256(closure)
    current_elements = {item["id"] for item in closure["elements"]}

    def subject_route(identity: str) -> str:
        return (
            _element_file(identity)
            if identity in current_elements
            else "documentation-audit-removed-" + _hash(identity) + ".html"
        )

    def guide_route(identity: str) -> str:
        return "documentation-audit-guide-" + _hash(identity) + ".html"

    compiled = None if document is None else document.get("compiled")
    if compiled is not None:
        # Markdown guides have one canonical route, not a second plaintext copy.
        for guide in compiled["guides"]:
            old = "g-" + _hash(guide["id"]) + ".html"
            new = document_route(guide["id"])
            files.pop(old, None)
            for name, raw in list(files.items()):
                files[name] = raw.replace(
                    ('href="' + old + '"').encode(), ('href="' + new + '"').encode()
                )
        for name, page in compiled["pages"].items():
            metadata = compiled["documents"][name]
            route = document_route(metadata["id"])
            files[route] = _shell(
                metadata["title"],
                (
                    '<aside class="documentation"><p class="meta">Release-au'
                    "thored reference · noncontractual</p>"
                )
                + page["html"]
                + "</aside>",
                digest,
                audit="accounting.html" in files,
                documentation=True,
                documentation_only=True,
                breadcrumbs="<p>" + _link("index.html", "All authorities") + "</p>",
            )
        references = (
            "<ul>"
            + "".join(
                "<li>" + _link(document_route(value["id"]), value["title"]) + "</li>"
                for value in compiled["documents"].values()
            )
            + "</ul>"
        )
        files["documentation.html"] = _shell(
            "Release documentation",
            references,
            digest,
            audit="accounting.html" in files,
            documentation=True,
            documentation_only=True,
        )
        for name, payload in list(files.items()):
            if name.endswith(".html"):
                files[name] = payload.replace(
                    b"</header>",
                    b'<p class="documentation"><a href="documentation.html">'
                    b"Release documentation</a></p></header>",
                    1,
                )
    if record is None:
        return
    check_record(record)
    if canonical_sha256(record["current"]["closure"]) != digest:
        raise ContractAtlasError("documentation audit belongs to a different Closure")
    by_element: dict[str, list[Any]] = defaultdict(list)
    output_by_name = {output["name"]: output for output in record["destinations"]}
    groups: dict[tuple[str, str], list[Any]] = defaultdict(list)
    for stored_delta in record["deltas"]:
        delta = {
            **stored_delta,
            "before": None
            if stored_delta["before"] is None
            else record["baseline"]["subjects"][stored_delta["subject"]],
            "after": None
            if stored_delta["after"] is None
            else record["current"]["subjects"][stored_delta["subject"]],
        }
        item = delta["after"] or delta["before"]
        by_element[item["target"]["element_id"]].append(delta)
        groups[(item["authority"], item["interface"])].append(delta)
    global_findings = [
        finding
        for finding in record["findings"]
        if finding["subject"] not in record["current"]["subjects"]
    ]
    global_body = (
        "<ul>"
        + "".join(
            "<li><strong>"
            + _esc(f["state"] + " · " + f["code"])
            + "</strong><p>"
            + _esc(f["detail"])
            + "</p></li>"
            for f in global_findings[:30]
        )
        + "</ul>"
    )
    summary = (
        f"<p><strong>Documentation {_esc(record['state'])}</strong> · {_esc(record['stage'])}</p>"
        f"<p>{record['counts']['covered']} of {record['counts']['subjects']} subjects covered. "
        f"Comparison: {_esc(record['comparison'])}; "
        f"native extraction: {_esc(record['checks']['native'])}.</p>"
        "<p>Mechanical checks do not establish prose truth or human approval.</p>"
    )
    baseline = record["baseline_custody"]
    if baseline is not None:
        summary += (
            "<details><summary>Authenticated comparison baseline</summary>"
            + _value_html(baseline, "/documentation-audit/baseline")
            + "</details>"
        )
    if record.get("policy_delta"):
        global_body += (
            "<details><summary>Requirement policy before and after</summary>"
            + _value_html(record["policy_delta"], "/documentation-audit/policy")
            + "</details>"
        )
    guide_rows = []
    for delta in record["guide_deltas"]:
        name = delta["guide_id"]
        before = None if delta["before"] is None else record["baseline"]["guides"][name]
        after = None if delta["after"] is None else record["current"]["guides"][name]
        guide = after or before
        assert guide is not None
        guide_rows.append((_link(guide_route(name), guide["title"]), _esc(delta["change"])))
        body = summary + "<p>Guide · " + _esc(name) + " · " + _esc(delta["change"]) + "</p>"
        for label, value in (("Before", before), ("Current", after)):
            if value is not None:
                body += (
                    "<details><summary>"
                    + label
                    + " guide</summary><pre>"
                    + _esc(value["markdown"])
                    + "</pre>"
                    + _value_html(value["subjects"], "/documentation-audit/guide/" + label)
                    + "</details>"
                )
        files[guide_route(name)] = _shell(
            "Documentation Audit · " + guide["title"],
            body,
            digest,
            audit="accounting.html" in files,
            documentation=document is not None,
        )
    group_rows = []
    for (authority, interface), deltas in sorted(groups.items()):
        changes = Counter(d["change"] for d in deltas)
        affected = {
            d["subject"]: d
            for d in deltas
            if d["change"] != "unchanged" or (d["after"] is not None and not d["after"]["covered"])
        }
        route = (
            "documentation-audit-"
            + hashlib.sha256((authority + "/" + interface).encode()).hexdigest()[:24]
            + ".html"
        )
        group_rows.append(
            (_link(route, authority), _esc(interface), _esc(len(deltas)), _esc(dict(changes)))
        )
        # One link per element keeps the overview bounded; member detail lives at its authority.
        elements = {
            (d["after"] or d["before"])["target"]["element_id"]: (d["after"] or d["before"])[
                "title"
            ]
            for d in affected.values()
        }
        links = (
            "<ul>"
            + "".join(
                "<li>"
                + _link(subject_route(identity) + "#documentation-audit", title)
                + (" · removed" if identity not in current_elements else "")
                + "</li>"
                for identity, title in sorted(elements.items())
            )
            + "</ul>"
        )
        files[route] = _shell(
            "Documentation Audit · " + authority + " · " + interface,
            summary
            + _value_html(dict(changes), "/documentation-audit/counts")
            + links
            + '<p><a href="documentation-audit.html">Candidate attention and full evidence</a></p>',
            digest,
            audit="accounting.html" in files,
            documentation=document is not None,
        )
    files["documentation-audit.html"] = _shell(
        "Documentation Audit",
        summary
        + global_body
        + _table_html(("Authority", "Interface", "Subjects", "Changes"), group_rows)
        + (_table_html(("Guide", "Change"), guide_rows) if guide_rows else "")
        + '<p><a href="../documentation-audit.json">Complete Documentation Audit Record</a> · '
        + (
            '<a href="../documentation-record.json">Documentation Record</a> · '
            '<a href="../documentation-source.json">Exact source ledger</a>'
            if document is not None
            else "No selected editorial corpus"
        )
        + "</p>",
        digest,
        audit="accounting.html" in files,
        documentation=document is not None,
    )
    for identity, deltas in by_element.items():
        route = subject_route(identity)
        cards = []
        for delta in deltas:
            subject_pointer = "/documentation-audit/subjects/" + delta["subject"]
            after, before = delta["after"], delta["before"]
            item = after or before
            selector = (
                item["target"].get("member", {}).get("key", item["target"].get("pointer", ""))
                or "Whole element"
            )
            body = (
                "<p>"
                + _esc(delta["change"])
                + " · "
                + _esc(item["requirement"]["rule"])
                + " · "
                + ("covered" if item["covered"] else "missing")
                + "</p>"
            )
            location = item["location"]
            if location is not None:
                source = location["path"]
                selected = compiled["documents"].get(source) if compiled is not None else None
                if selected:
                    section = location["section"] or ""
                    fragment = (
                        "#doc-" + section[1:] if section.startswith("#") and section != "#" else ""
                    )
                    body += (
                        "<p>"
                        + _link(document_route(selected["id"]) + fragment, source + " " + section)
                        + "</p>"
                    )
            for label, snapshot in (("Before", before), ("Current", after)):
                if snapshot is None:
                    continue
                entry = snapshot["prose"]["entry"]
                if entry:
                    body += (
                        "<details><summary>"
                        + label
                        + " prose</summary><p>"
                        + _esc(entry["summary"])
                        + "</p><pre>"
                        + _esc(entry["markdown"])
                        + "</pre></details>"
                    )
            guide_ids = set(after["prose"]["guides"] if after is not None else {}) | set(
                before["prose"]["guides"] if before is not None else {}
            )
            if guide_ids:
                body += (
                    "<ul>"
                    + "".join(
                        "<li>"
                        + _link(guide_route(name), name + " · retained guide comparison")
                        + "</li>"
                        for name in sorted(guide_ids)
                    )
                    + "</ul>"
                )
            if before is not None and after is None:
                body += (
                    "<details><summary>Retained removed meaning</summary>"
                    + _value_html(
                        record["baseline"]["scopes"][before["meaning"]["owned"]],
                        subject_pointer + "/meaning",
                    )
                    + "</details>"
                )
            if (
                before is not None
                and after is not None
                and delta["change"] not in {"unchanged", "prose-only"}
            ):
                from .review import _changes

                owned_changes = _changes(
                    record["baseline"]["scopes"][before["meaning"]["owned"]],
                    record["current"]["scopes"][after["meaning"]["owned"]],
                )
                body += (
                    "<details><summary>Owned meaning before and after</summary>"
                    + _value_html(owned_changes, subject_pointer + "/meaning")
                    + "</details>"
                )
                context_pointers: set[str] = set()
                for side, snapshot in (("baseline", before), ("current", after)):
                    for scope in record[side]["contexts"][snapshot["meaning"]["dependencies"]]:
                        context_pointers.update(
                            p for p in record[side]["scopes"][scope] if p.startswith("/")
                        )
                context_changes = [
                    change
                    for change in record["contract_delta"]
                    if any(
                        change["pointer"] == p or change["pointer"].startswith(p + "/")
                        for p in context_pointers
                    )
                ]
                if context_changes:
                    body += (
                        (
                            "<details><summary>Applicable parent, shared and governi"
                            "ng context changes</summary>"
                        )
                        + _value_html(context_changes[:30], subject_pointer + "/context")
                        + f"<p>{len(context_changes)} applicable changes; "
                        + "complete retained values are in the Documentation Audit Record."
                        + "</p></details>"
                    )
            destinations = []
            for destination in item["requirement"]["destinations"]:
                kind = destination["kind"]
                if kind == "cli":
                    key = (
                        "cli/"
                        + " ".join(destination["command"])
                        + (
                            "/parameter/" + destination["parameter"]
                            if "parameter" in destination
                            else "/command"
                        )
                    )
                elif kind == "openapi":
                    key = "openapi/" + destination["authority"] + destination["pointer"]
                elif kind == "python":
                    key = "python/" + destination["identity"]
                else:
                    key = kind + "/" + destination["name"]
                current = output_by_name.get(key)
                if current is not None:
                    values = {
                        "name": key,
                        "status": current["status"],
                        "observed": current["observed"],
                    }
                    if current["expected"] != current["observed"]:
                        values["expected"] = current["expected"]
                    if record["baseline"] is not None:
                        values["before_observed"] = record["baseline"]["observed"].get(key)
                        if record["baseline"]["expected"].get(key) != values["before_observed"]:
                            values["before_expected"] = record["baseline"]["expected"].get(key)
                    destinations.append(values)
            if destinations:
                body += (
                    (
                        "<details><summary>Actual native destination parity and "
                        "selected prose</summary>"
                    )
                    + _value_html(destinations, subject_pointer + "/destinations")
                    + "</details>"
                )
            body += (
                "<details><summary>Exact subject and requirement</summary>"
                + "<pre>"
                + _esc(canonical_bytes(item["target"]).decode())
                + "</pre>"
                + "<p>"
                + _esc(item["requirement"]["reason"])
                + "</p>"
                + _value_html(
                    {
                        key: item["requirement"][key]
                        for key in ("detail", "canonical")
                        if key in item["requirement"]
                    },
                    subject_pointer + "/requirement",
                )
                + "</details>"
            )
            cards.append(
                '<article class="authority-card" id="documentation-subject-'
                + hashlib.sha256(delta["subject"].encode()).hexdigest()[:24]
                + '"><h3>'
                + _esc(selector)
                + "</h3>"
                + body
                + "</article>"
            )
        panel = (
            (
                '<aside class="documentation-audit" id="documentation-au'
                'dit"><h2>Documentation Audit</h2>'
            )
            + summary
            + (
                '<p><a href="documentation-audit.html">Candidate attenti'
                'on and native parity</a></p><div class="authority-cards'
                '">'
            )
            + "".join(cards)
            + "</div></aside>"
        )
        if identity in current_elements:
            files[route] = files[route].replace(b"<footer>", panel.encode() + b"<footer>", 1)
        else:
            files[route] = _shell(
                "Documentation Audit · removed " + deltas[0]["before"]["title"],
                '<p class="meta">Removed native subject</p>'
                + panel.replace('class="documentation-audit"', ""),
                digest,
                audit="accounting.html" in files,
                documentation=document is not None,
            )
    for name, payload in list(files.items()):
        if not name.endswith(".html"):
            continue
        control = (
            '<label class="mode"><input id="documentation-audit-mode'
            '" type="checkbox"> Documentation Audit</label>'
        )
        status = (
            '<p class="documentation-attention"><a href="documentation-audit.html">Documentation '
            + _esc(record["state"])
            + " · "
            + _esc(record["stage"])
            + "</a></p>"
        )
        files[name] = payload.replace(
            b"<header>", b"<header>" + control.encode() + status.encode(), 1
        )
