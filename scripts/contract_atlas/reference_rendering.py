"""Generated audit references, canonical policy routes, and scoped evidence gaps."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import cast

from .dossier_rendering import _render_generic
from .model import (
    ATLAS_DIRECTORY,
    AUDIT_PRIMARY_CONTENT_TARGET_BYTES,
    ContractAtlasError,
    _slug,
    pointer_value,
)
from .navigation import (
    CONFIGURATION_DOCUMENTS_PATH,
    CONFIGURATION_FAMILIES_PATH,
    CONFIGURATION_SETTINGS_PATH,
    EXTENT_PRINCIPLES_PATH,
    FIXTURES_PATH,
    MACHINE_ARTIFACT_TARGET,
    QUALIFICATION_ROUTES_PATH,
    QUALIFICATIONS_PATH,
    RELATIONSHIP_EDGES_PATH,
    RELATIONSHIP_NODES_PATH,
    SOURCE_AUTHORITIES_PATH,
    _anchor_id,
    _anchor_link,
    _anchor_marker,
    _html_anchor,
    _md,
    _policy_application_anchor,
    _policy_applications_path,
    _policy_destination,
    _policy_link,
    _qualification_anchor,
    _qualification_explanation,
    _relationship_edge_anchor,
    _relationship_node_anchor,
    _relative_link,
    _repository_source_link,
    _source_anchor,
    _source_location_links,
    _witness_path,
)


def _link(document: str, target: str, label: object) -> str:
    return f"[{_md(label)}]({_relative_link(document, target)})"


def _page(title: str, path: str, parent: str, body: Sequence[str]) -> bytes:
    return (
        "\n".join(
            [
                f"# {title}",
                "",
                _link(path, f"{ATLAS_DIRECTORY}/index.md", "Atlas")
                + " · "
                + _link(path, parent, "Reference navigation"),
                "",
                *body,
            ]
        ).rstrip()
        + "\n"
    ).encode()


def _count_table(values: Mapping[str, object], label: str) -> list[str]:
    return [
        f"| {label} | Count |",
        "|---|---:|",
        *(f"| `{_md(key)}` | {value} |" for key, value in values.items()),
    ]


def _policy_application_page(
    identity: str,
    definitions: Mapping[str, Mapping[str, object]],
    path: str,
    parent: str,
    values: Sequence[Mapping[str, object]],
) -> bytes:
    application_note = (
        "These are the indexed contract-element applications. Each link reaches the policy "
        "application on its contract element page. The definition states the policy's full "
        "scope; this index is not an exhaustive interpretation of that scope or executed evidence."
    )
    lines = [
        f"Definition: {_policy_link(path, identity, definitions)}.",
        "",
        application_note,
        "",
        "| Authority | Contract element |",
        "|---|---|",
    ]
    for item in sorted(values, key=lambda item: (str(item["authority"]), str(item["title"]))):
        target = _anchor_link(
            path,
            str(item["dossier"]),
            _policy_application_anchor(str(item["id"]), identity),
        )
        lines.append(f"| `{_md(item['authority'])}` | [{_md(item['title'])}]({target}) |")
    return _page(f"Indexed applications of {identity}", path, parent, lines)


def _render_policy_references(
    elements: Sequence[Mapping[str, object]],
    policies: Mapping[str, object],
    projection: Mapping[str, object],
    definitions: Mapping[str, Mapping[str, object]],
) -> dict[str, bytes]:
    files = {}
    root = f"{ATLAS_DIRECTORY}/policies/index.md"
    intro = [
        "Follow a policy to its canonical definition and scope, or inspect its indexed "
        "contract-element applications. Definitions are not counted as applications. "
        "An application index is not an exhaustive interpretation of a policy's scope "
        "or behavioral evidence.",
        "",
    ]
    for category, values in policies.items():
        category_path = f"{ATLAS_DIRECTORY}/policies/{category}/index.md"
        records = cast(Sequence[Mapping[str, object]], values)
        title = category.replace("_", " ").title()
        intro.append(f"- {_link(root, category_path, title)} ({len(records)})")
        category_lines = [
            "Each definition has one primary location and states its own scope. "
            "The links below list indexed applications, excluding the definition itself. "
            "An absent index does not mean that a policy has no applications.",
            "",
            "| Policy definition | Scope / indexed applications |",
            "|---|---|",
        ]
        if category_path == EXTENT_PRINCIPLES_PATH:
            extents = cast(
                Mapping[str, object],
                cast(Mapping[str, object], projection["external_contract"])["extents"],
            )
            category_lines[:2] = [
                "These principles govern the whole extent contract: its "
                f"{len(cast(Mapping[str, object], extents['rules']))} rules and all "
                f"{len(cast(Sequence[object], extents['decisions']))} recorded extent decisions. "
                "Each decision appears on the contract element that owns it. "
                "The principle definition is not itself an application.",
                "",
                "Recorded scope in the machine artifact: `/external_contract/extents`. "
                + _link(
                    category_path,
                    f"{ATLAS_DIRECTORY}/policies/extent_rules/index.md",
                    "Extent rules",
                )
                + " route to the elements with decisions using each rule.",
                "",
            ]
        for policy in records:
            identity = str(policy["id"])
            applications = [
                item for item in elements if identity in cast(Sequence[str], item["policy_ids"])
            ]
            target, anchor = _policy_destination(identity, definitions)
            definition = definitions.get(identity)
            if definition is not None:
                pointers = cast(Sequence[str], definition["pointers"])
                if (
                    len(pointers) != 1
                    or pointer_value(projection, pointers[0]) != policy["meaning"]
                ):
                    raise ContractAtlasError(
                        f"policy differs from its canonical definition: {identity}"
                    )
            else:
                pointer = str(policy["definition_pointer"])
                if pointer_value(projection, pointer) != policy["meaning"]:
                    raise ContractAtlasError(f"policy differs from its owned source: {identity}")
                files[target] = _page(
                    identity,
                    target,
                    category_path,
                    [
                        _html_anchor(anchor),
                        "This publication promise is owned directly by the policy record.",
                        "",
                        *(
                            [
                                _link(
                                    target,
                                    _policy_applications_path(identity),
                                    "Indexed applications",
                                )
                            ]
                            if applications
                            else []
                        ),
                        "",
                        "## Definition",
                        "",
                        *_render_generic([pointer], [policy["meaning"]], set()),
                        "",
                        f"Definition location in the machine artifact: `{_md(pointer)}`.",
                    ],
                )
            if not applications:
                scope = (
                    "Whole extent contract"
                    if category == "extent_principles"
                    else "Scope stated in definition; no element index"
                )
                category_lines.append(
                    f"| {_policy_link(category_path, identity, definitions)} | {scope} |"
                )
                continue
            application_path = _policy_applications_path(identity)
            count_label = "contract element" if len(applications) == 1 else "contract elements"
            category_lines.append(
                f"| {_policy_link(category_path, identity, definitions)} | "
                + _link(
                    category_path, application_path, f"{len(applications)} indexed {count_label}"
                )
                + " |"
            )
            payload = _policy_application_page(
                identity, definitions, application_path, category_path, applications
            )
            owners = sorted({str(item["authority"]) for item in applications})
            if len(payload) > AUDIT_PRIMARY_CONTENT_TARGET_BYTES and len(owners) > 1:
                # Use existing ownership to keep a large inverse join navigable.
                lines = [
                    f"Definition: {_policy_link(application_path, identity, definitions)}.",
                    "",
                    "Choose an authority to inspect this policy's indexed contract-element "
                    "applications. The definition states its full scope.",
                    "",
                    "| Authority | Contract elements |",
                    "|---|---:|",
                ]
                for owner in owners:
                    values = [item for item in applications if item["authority"] == owner]
                    child = application_path.removesuffix(".md") + f"/{_slug(owner)}.md"
                    files[child] = _policy_application_page(
                        identity, definitions, child, application_path, values
                    )
                    lines.append(f"| {_link(application_path, child, owner)} | {len(values)} |")
                payload = _page(
                    f"Indexed applications of {identity}", application_path, category_path, lines
                )
            files[application_path] = payload
        files[category_path] = _page(title, category_path, root, category_lines)
    files[root] = _page("Governing policies", root, f"{ATLAS_DIRECTORY}/index.md", intro)
    return files


def _render_qualification_scope(
    title: str,
    path: str,
    parent: str,
    elements: Sequence[Mapping[str, object]],
    qualifications: Mapping[str, Sequence[str]],
    witnesses: Mapping[str, Mapping[str, object]],
) -> bytes:
    affected = [item for item in elements if qualifications[str(item["id"])]]
    ids = sorted({identity for item in affected for identity in qualifications[str(item["id"])]})
    claims = sorted(
        {
            claim
            for identity in ids
            for claim in cast(Sequence[str], witnesses[identity]["unestablished_claims"])
        }
    )
    lines = [
        f"This view covers **{len(affected)} affected contract elements** within {title}. "
        "Only their recorded evidence groups are included.",
        "",
        *_qualification_explanation(claims),
        "The table identifies the exact evidence groups for each element. Its element link "
        "opens the local explanation and governing rules; each evidence group gives its "
        "specific open guarantees and candidate tests.",
        "",
        "| Contract element | Evidence group |",
        "|---|---|",
    ]
    for item in sorted(affected, key=lambda item: (str(item["authority"]), str(item["title"]))):
        groups = "<br>".join(
            _link(path, _witness_path(identity), identity)
            for identity in qualifications[str(item["id"])]
        )
        label = f"{item['authority']}: {item['title']}"
        lines.append(
            f"| {_link(path, str(item['dossier']) + '#evidence-gaps', label)} | {groups} |"
        )
    return _page(f"{title}: evidence gaps", path, parent, lines)


def _render_evidence_references(
    elements: Sequence[Mapping[str, object]],
    projection: Mapping[str, object],
    trace: Mapping[str, object],
    identities: Mapping[str, object],
    discovery: Mapping[str, object],
    counts: Mapping[str, object],
    sources: Mapping[str, Mapping[str, object]],
    relationship: Mapping[str, object],
    qualifications: Mapping[str, Sequence[str]],
    definitions: Mapping[str, Mapping[str, object]],
) -> dict[str, bytes]:
    files = {}
    root = f"{ATLAS_DIRECTORY}/evidence/index.md"
    source_root = f"{ATLAS_DIRECTORY}/evidence/sources.md"
    configuration_root = f"{ATLAS_DIRECTORY}/evidence/configuration.md"
    relationship_root = f"{ATLAS_DIRECTORY}/evidence/relationships.md"
    source_note = (
        "Source bindings and candidate tests are audit leads. This checked-in atlas contains "
        "no executed qualification result or CI attestation. A passing run applies only to "
        "its executed checks and exact source SHA; main CI does not imply "
        "release or provider qualification."
    )
    files[source_root] = _page(
        "Sources and qualifications",
        source_root,
        root,
        [
            source_note,
            "",
            "- "
            + _link(source_root, SOURCE_AUTHORITIES_PATH, "Source authorities")
            + " — exact source-to-contract joins.",
            "- "
            + _link(source_root, QUALIFICATION_ROUTES_PATH, "Qualification commands")
            + " — candidate validation routes.",
            "- "
            + _link(source_root, FIXTURES_PATH, "Durable-state fixtures")
            + " — recorded file identities and hashes.",
            "- "
            + _link(source_root, QUALIFICATIONS_PATH, "Evidence gaps across pages or chunks")
            + " — required guarantees and their current evidence limits.",
        ],
    )
    route_counts = cast(Mapping[str, object], counts["by_qualification_route"])
    witness_route_counts = Counter(
        route
        for witness in cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"])
        for route in cast(Sequence[str], witness["gates"])
    )
    command_routes = sorted(set(route_counts) | set(witness_route_counts))
    files[QUALIFICATION_ROUTES_PATH] = _page(
        "Qualification commands",
        QUALIFICATION_ROUTES_PATH,
        source_root,
        [
            source_note,
            "",
            "Counts distinguish contract-element bindings from candidate witness-group bindings. "
            "Neither count represents an executed result.",
            "",
            "| Command | Contract-element bindings | Candidate groups |",
            "|---|---:|---:|",
            *(
                f"| {_html_anchor(_qualification_anchor(route))}`{_md(route)}` | "
                f"{route_counts.get(route, 0)} | {witness_route_counts.get(route, 0)} |"
                for route in command_routes
            ),
        ],
    )
    source_counts = cast(Mapping[str, object], counts["by_source_authority"])
    lines = [
        source_note,
        "",
        "| Source authority | Applications | Executable location |",
        "|---|---:|---|",
    ]
    for identity, source in sources.items():
        links = _source_location_links(SOURCE_AUTHORITIES_PATH, source)
        framework = source.get("framework")
        if isinstance(framework, Mapping):
            links.append(
                f"Framework mechanism: `{_md(framework['module'])}.{_md(framework['symbol'])}`; "
                "the linked application construction supplies this OpenAPI document."
            )
        lines.append(
            f"| {_html_anchor(_source_anchor(identity))}`{_md(identity)}` | "
            f"{source_counts.get(identity, 0)} | {'<br>'.join(links)} |"
        )
    files[SOURCE_AUTHORITIES_PATH] = _page(
        "Source authorities", SOURCE_AUTHORITIES_PATH, source_root, lines
    )
    lines = [
        "Fixture paths and hashes identify the recorded state baseline. They do not record "
        "executed restart or introspection results. The owning contract element and source "
        "declarations define the state structure.",
        "",
        "| State authority | Fixture | SHA-256 |",
        "|---|---|---|",
    ]
    for identity, source in sources.items():
        for fixture in cast(Sequence[Mapping[str, object]], source.get("fixtures", ())):
            source_link = _anchor_link(
                FIXTURES_PATH, SOURCE_AUTHORITIES_PATH, _source_anchor(identity)
            )
            link = _repository_source_link(FIXTURES_PATH, fixture, str(fixture["path"]))
            lines.append(f"| [{_md(identity)}]({source_link}) | {link} | `{fixture['sha256']}` |")
    files[FIXTURES_PATH] = _page("Durable-state fixtures", FIXTURES_PATH, source_root, lines)

    witnesses = cast(Sequence[Mapping[str, object]], trace["segmented_extent_witnesses"])
    decisions = {
        str(item["id"]): item
        for item in cast(
            Sequence[Mapping[str, object]],
            cast(
                Mapping[str, object],
                cast(Mapping[str, object], projection["external_contract"])["extents"],
            )["decisions"],
        )
    }
    bindings = cast(Sequence[Mapping[str, object]], trace["extent_sources"])
    open_claims = sorted(
        {
            claim
            for witness in witnesses
            for claim in cast(Sequence[str], witness["unestablished_claims"])
        }
    )
    witness_lines = [
        *(
            _qualification_explanation(open_claims)
            if open_claims
            else [
                "No open guarantees are recorded for these groups. This is not an approval claim.",
                "",
            ]
        ),
        "Each group has separate routes to its affected contracts and candidate tests. "
        "Test-symbol existence and owner/reason matching validate routing only.",
        "",
        "| Evidence group | Bound extent decisions | Candidate tests |",
        "|---|---:|---:|",
    ]
    witnesses_by_extent = {
        str(link["id"]): set(cast(Sequence[str], link.get("segmented_extent_witnesses", ())))
        for link in bindings
    }
    for witness in witnesses:
        identity = str(witness["id"])
        path = _witness_path(identity)
        affected = [
            item
            for item in elements
            if any(
                identity in witnesses_by_extent.get(extent, set())
                for extent in cast(Sequence[str], item["extent_decision_ids"])
            )
        ]
        bound = [
            decisions[str(link["id"])]
            for link in bindings
            if identity in cast(Sequence[str], link.get("segmented_extent_witnesses", ()))
        ]
        rules = sorted({f"extent-rule/{item['rule']}" for item in bound})
        tests = cast(Sequence[str], witness["test_node_ids"])
        witness_lines.append(
            f"| {_link(QUALIFICATIONS_PATH, path, identity)} | {len(bound)} | {len(tests)} |"
        )
        claims = cast(Sequence[str], witness["unestablished_claims"])
        lines = [
            _html_anchor(_anchor_id("extent-witness", identity)),
            f"Recorded owner: `{_md(witness['owner'])}`.",
            "",
            "- "
            + _link(
                path,
                _witness_path(identity, "contracts"),
                f"{'Affected' if claims else 'Bound'} contract elements ({len(affected)})",
            ),
            "- "
            + _link(
                path,
                _witness_path(identity, "tests"),
                f"Candidate tests and reviewed scopes ({len(tests)})",
            ),
            "",
            "## Guarantees still needing evidence",
            "",
            *(
                _qualification_explanation(claims)
                if claims
                else [
                    "No open guarantees are recorded for this group. "
                    "This is not an approval claim.",
                    "",
                ]
            ),
            *(
                [
                    "Recorded open-claim keys: "
                    + ", ".join(f"`{_md(claim)}`" for claim in claims)
                    + ".",
                    "",
                ]
                if claims
                else []
            ),
            "## Governing rules",
            "",
            *[f"- {_policy_link(path, rule, definitions)}" for rule in rules],
            "",
            "Recorded reasons: "
            + ", ".join(f"`{_md(reason)}`" for reason in cast(Sequence[str], witness["reasons"]))
            + ".",
            "",
            "The policy definitions explain the contract. Candidate tests describe potential "
            "evidence; their presence does not establish these group-wide guarantees.",
        ]
        files[path] = _page(identity, path, QUALIFICATIONS_PATH, lines)
        contract_path = _witness_path(identity, "contracts")
        files[contract_path] = _page(
            f"{identity}: {'affected' if claims else 'bound'} contract elements",
            contract_path,
            path,
            [
                "These exact elements own extent decisions bound to this group. "
                + (
                    "The group-wide evidence gap does not report an observed failure "
                    "in each element."
                    if claims
                    else "No open guarantees are recorded for this group; this implies no approval."
                ),
                "",
                *[
                    "- "
                    + _link(
                        contract_path,
                        str(item["dossier"]) + ("#evidence-gaps" if claims else ""),
                        str(item["authority"]) + ": " + str(item["title"]),
                    )
                    for item in sorted(
                        affected, key=lambda item: (str(item["authority"]), str(item["title"]))
                    )
                ],
            ],
        )
        test_path = _witness_path(identity, "tests")
        scopes = {
            str(item["node_id"]): item
            for item in cast(Sequence[Mapping[str, object]], witness["test_scopes"])
        }
        lines = [
            source_note,
            "",
            "A reviewed scope states what a test exercises. A candidate binding without "
            "a reviewed scope makes no behavioral proof claim.",
            "",
            "Candidate commands: "
            + ", ".join(
                _link(
                    test_path, QUALIFICATION_ROUTES_PATH + "#" + _qualification_anchor(route), route
                )
                for route in cast(Sequence[str], witness["gates"])
            )
            + ".",
            "",
        ]
        for node_id in tests:
            scope = scopes.get(node_id)
            location = (
                cast(Mapping[str, object], scope["source"])
                if scope
                else {"path": node_id.split("::", 1)[0]}
            )
            description = (
                str(scope["scope"])
                if scope
                else "Candidate binding; no reviewed behavior scope is recorded."
            )
            lines.append(
                f"- {_repository_source_link(test_path, location, node_id)} — {_md(description)}"
            )
        files[test_path] = _page(f"{identity}: candidate tests", test_path, path, lines)
    files[QUALIFICATIONS_PATH] = _page(
        "Evidence gaps across pages or chunks", QUALIFICATIONS_PATH, source_root, witness_lines
    )

    config = cast(Mapping[str, object], trace["configuration_registry"])
    documents = cast(Mapping[str, object], trace["configuration_document_registry"])
    config_counts = cast(Mapping[str, object], config["counts"])
    document_counts = cast(Mapping[str, object], documents["counts"])
    reconciliation = f"{ATLAS_DIRECTORY}/evidence/configuration/reconciliation.md"
    files[configuration_root] = _page(
        "Configuration comparison",
        configuration_root,
        root,
        [
            "Compare recorded owners, consumers, defaults, and input shapes. These are discovered "
            "source facts, not executed observations of effective configuration. Each entry links "
            "to its owning contract element.",
            "",
            "- "
            + _link(configuration_root, CONFIGURATION_SETTINGS_PATH, "Environment settings")
            + f" ({config_counts['contracts']})",
            "- "
            + _link(configuration_root, CONFIGURATION_FAMILIES_PATH, "Parameterized families")
            + f" ({config_counts['patterns']})",
            "- "
            + _link(configuration_root, CONFIGURATION_DOCUMENTS_PATH, "Configuration documents")
            + f" ({document_counts['contracts']})",
            "- "
            + _link(configuration_root, reconciliation, "Discovery and ownership reconciliation"),
        ],
    )
    files[reconciliation] = _page(
        "Configuration reconciliation",
        reconciliation,
        configuration_root,
        [
            f"Unique environment names: **{config_counts['unique_environment_names']}** · "
            f"Implementation reads: **{config_counts['detections']}** · "
            f"Explicit ambiguity resolutions: **{config_counts['resolution_exceptions']}**",
            "",
            "| Check | Result |",
            "|---|---:|",
            *(
                f"| {_md(name.replace('_', ' '))} | {'pass' if count == 0 else count} |"
                for name, count in cast(Mapping[str, object], config["coverage"]).items()
            ),
            "",
            *_count_table(cast(Mapping[str, object], config_counts["by_owner"]), "Owner"),
        ],
    )
    config_elements = {}
    document_elements = {}
    for item in elements:
        if item["interface"] == "configuration-environment":
            for pointer in cast(Sequence[str], item["pointers"]):
                value = cast(Mapping[str, object], pointer_value(projection, pointer))
                config_elements[str(value["id"])] = item
        elif item["interface"] == "configuration":
            for pointer in cast(Sequence[str], item["pointers"]):
                document_elements[
                    pointer.rsplit("/", 1)[-1].replace("~1", "/").replace("~0", "~")
                ] = item
    for path, title, records, kind in (
        (CONFIGURATION_SETTINGS_PATH, "Environment settings", config["records"], "settings"),
        (CONFIGURATION_FAMILIES_PATH, "Parameterized families", config["patterns"], "families"),
        (
            CONFIGURATION_DOCUMENTS_PATH,
            "Configuration documents",
            documents["candidates"],
            "documents",
        ),
    ):
        last_column = {
            "settings": "Default expressions",
            "families": "Settings",
            "documents": "Input shape",
        }[kind]
        lines = [
            "Recorded consumer and default facts come from discovery. Use the contract element "
            "for its complete input contract and maintained source routes.",
            "",
            f"| Owner | Contract element | Consumers | {last_column} |",
            "|---|---|---|---|",
        ]
        owners = set()
        for record in sorted(
            cast(Sequence[Mapping[str, object]], records),
            key=lambda record: (str(record["owner"]), str(record["id"])),
        ):
            identity = str(record["id"])
            item = (document_elements if kind == "documents" else config_elements)[identity]
            owner = str(record["owner"])
            marker = (
                "" if owner in owners else _html_anchor(_anchor_id("configuration-owner", owner))
            )
            owners.add(owner)
            label = record.get("name", record.get("template", identity))
            fact_key = {
                "settings": "default_expressions",
                "families": "settings",
                "documents": "input_shapes",
            }[kind]
            facts = ", ".join(cast(Sequence[str], record[fact_key]))
            consumers = ", ".join(cast(Sequence[str], record["consumers"]))
            lines.append(
                f"| {marker}`{_md(owner)}` | {_link(path, str(item['dossier']), label)} | "
                f"`{_md(consumers)}` | `{_md(facts)}` |"
            )
        files[path] = _page(title, path, configuration_root, lines)

    authorities_path = f"{ATLAS_DIRECTORY}/evidence/authorities.md"
    registry = cast(Mapping[str, object], trace["authority_registry"])
    owners = {str(item["authority"]) for item in elements}
    lines = [
        "The ordinary atlas map owns the authority/interface inventory and its scope descriptions. "
        "This reference reconciles aggregate ownership and the projection's internal bookkeeping.",
        "",
        "## Aggregate ownership",
        "",
        *_count_table(cast(Mapping[str, object], counts["by_interface"]), "Interface"),
        "",
        "## Declared aggregate scopes",
        "",
        "| Declared authority | Scope description |",
        "|---|---|",
        *(
            f"| `{_md(item['id'])}` | "
            + (
                _link(
                    authorities_path,
                    f"{ATLAS_DIRECTORY}/authorities/{_slug(str(item['id']), limit=72)}/index.md",
                    item["id"],
                )
                if item["id"] in owners
                else _md(item["meaning"]) + " No contract elements use this aggregate owner."
            )
            + " |"
            for item in cast(Sequence[Mapping[str, object]], registry["declared_authorities"])
        ),
        "",
        "## Non-contractual projection machinery",
        "",
        "These internal projection records are not exclusions of discovered external contracts.",
        "",
        "| Projection record | Machine location | Reason |",
        "|---|---|---|",
        *(
            f"| `{_md(item['id'])}` | "
            f"`{_md(', '.join(cast(Sequence[str], item['pointers'])))}` | {_md(item['reason'])} |"
            for item in cast(Sequence[Mapping[str, object]], registry["noncontractual_projection"])
        ),
    ]
    files[authorities_path] = _page("Authority reconciliation", authorities_path, root, lines)

    nodes = cast(Sequence[Mapping[str, object]], relationship["nodes"])
    edges = cast(Sequence[Mapping[str, object]], relationship["edges"])
    nodes_by_id = {str(item["id"]): item for item in nodes}
    files[relationship_root] = _page(
        "Declared relationships",
        relationship_root,
        root,
        [
            "Inspect the exact declared dependency, packaging, and extension joins. "
            "Node identities "
            "and relationship edges have separate inventories.",
            "",
            "- "
            + _link(relationship_root, RELATIONSHIP_NODES_PATH, "Relationship nodes")
            + f" ({len(nodes)})",
            "- "
            + _link(relationship_root, RELATIONSHIP_EDGES_PATH, "Relationship edges")
            + f" ({len(edges)})",
            "",
            *_count_table(
                dict(sorted(Counter(str(item["kind"]) for item in nodes).items())), "Node kind"
            ),
            "",
            *_count_table(
                dict(sorted(Counter(str(item["type"]) for item in edges).items())), "Relationship"
            ),
        ],
    )
    lines = [
        "Each row identifies one declared relationship node. Semantic contract elements "
        "remain under their owning authorities and interfaces.",
        "",
        "| Identity | Kind | Name | Role or owner | Maintained purpose |",
        "|---|---|---|---|---|",
    ]
    for node in nodes:
        name = str(node["name"])
        label = (
            _link(
                RELATIONSHIP_NODES_PATH,
                f"{ATLAS_DIRECTORY}/authorities/{_slug(name, limit=72)}/index.md",
                name,
            )
            if name in owners
            else f"`{_md(name)}`"
        )
        lines.append(
            f"| {_html_anchor(_relationship_node_anchor(str(node['id'])))}`{_md(node['id'])}` | "
            f"`{_md(node['kind'])}` | {label} | "
            f"`{_md(node.get('role', node.get('owner', '—')))}` | {_md(node['description'])} |"
        )
    files[RELATIONSHIP_NODES_PATH] = _page(
        "Relationship nodes", RELATIONSHIP_NODES_PATH, relationship_root, lines
    )
    lines = [
        "Each row is one declared relationship. Its endpoints link to their exact node records.",
        "",
        "| From | Relationship | To | Scope or binding |",
        "|---|---|---|---|",
    ]
    for edge in edges:
        links = [
            _link(
                RELATIONSHIP_EDGES_PATH,
                RELATIONSHIP_NODES_PATH + "#" + _relationship_node_anchor(str(edge[key])),
                nodes_by_id[str(edge[key])]["name"],
            )
            for key in ("source", "target")
        ]
        detail = edge.get("scope", edge.get("binding", ""))
        lines.append(
            f"| {_html_anchor(_relationship_edge_anchor(edge))}{links[0]} | "
            f"`{_md(edge['type'])}` | {links[1]} | `{_md(detail)}` |"
        )
    files[RELATIONSHIP_EDGES_PATH] = _page(
        "Relationship edges", RELATIONSHIP_EDGES_PATH, relationship_root, lines
    )

    identity_path = f"{ATLAS_DIRECTORY}/evidence/identities.md"
    files[identity_path] = _page(
        "Snapshot identities",
        identity_path,
        root,
        [
            "These independent identities distinguish contract semantics, discovery coverage, "
            "source "
            "trace, and the generated human representation.",
            "",
            "| Identity domain | SHA-256 |",
            "|---|---|",
            *(
                f"| {_anchor_marker('identity', str(name))}`{_md(name)}` | `{_md(value)}` |"
                for name, value in identities.items()
            ),
            "",
            _anchor_marker("identity", "atlas_representation_sha256")
            + "The byte-exact `atlas_representation_sha256` is recorded at "
            "`/identities/atlas_representation_sha256` in the "
            + _link(identity_path, MACHINE_ARTIFACT_TARGET, "machine artifact (raw JSON)")
            + ". It cannot be embedded inside the document bytes that it identifies.",
        ],
    )
    files[root] = _page(
        "Accounting checks",
        root,
        f"{ATLAS_DIRECTORY}/index.md",
        [
            "These checks reconcile the discovered external contract. Discovery means inclusion. "
            "Complete accounting does not establish behavioral proof, freeze approval, "
            "or release readiness.",
            "",
            f"- {_link(root, authorities_path, 'Authority reconciliation')}",
            f"- {_link(root, configuration_root, 'Configuration comparison and reconciliation')}",
            f"- {_link(root, source_root, 'Sources, fixtures, and qualifications')}",
            f"- {_link(root, relationship_root, 'Declared relationships')}",
            f"- {_link(root, identity_path, 'Snapshot identities')}",
            "",
            "## Closure checks",
            "",
            "| Check | Result |",
            "|---|---:|",
            *(
                f"| {_md(name.replace('_', ' '))} | {'pass' if count == 0 else count} |"
                for name, count in cast(Mapping[str, object], discovery["anomalies"]).items()
            ),
            "",
            "## Reconciliation totals",
            "",
            "The following stages count the same discovered universe; "
            "their equality is a closure check, "
            "not independent corroboration.",
            "",
            "| Stage | Records |",
            "|---|---:|",
            *(
                f"| {label} | {len(cast(Sequence[object], discovery[key]))} |"
                for key, label in (
                    ("detections", "Detected"),
                    ("resolutions", "Resolved"),
                    ("candidates", "Candidates"),
                    ("dispositions", "Included"),
                )
            ),
            f"| Contract elements | {counts['contract_elements']} |",
            f"| Extent decisions | {counts['extent_decisions']} |",
            f"| Source authorities | {len(sources)} |",
        ],
    )
    return files
