"""Bounded #895 reference: lossless field partition, not a production migration.

The reference formats below are deliberately not the repository's v1 formats.
No changes to current generated baselines, release policy, or accepted hashes.
"""
from __future__ import annotations

import copy
import hashlib
import re
from collections.abc import Mapping
from typing import Any

from riverhog_canonical_json import canonical_json_bytes, parse_identity_json

CLOSURE_FORMAT = "riverhog-reference-895-closure/v1"
AUDIT_FORMAT = "riverhog-reference-895-audit/v1"
SAFE = (1 << 53) - 1


class BoundaryError(ValueError):
    """A reference input has unresolved ownership, an unknown role, or bad binding."""


def token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def parts(pointer: str) -> list[str]:
    if pointer == "":
        return []
    if not pointer.startswith("/") or re.search(r"~(?![01])", pointer):
        raise BoundaryError(f"invalid JSON pointer: {pointer!r}")
    return [p.replace("~1", "/").replace("~0", "~") for p in pointer[1:].split("/")]


def at(value: Any, pointer: str) -> Any:
    for part in parts(pointer):
        try:
            if isinstance(value, list):
                if not re.fullmatch(r"0|[1-9][0-9]*", part):
                    raise BoundaryError(f"invalid array index: {pointer}")
                value = value[int(part)]
            else:
                value = value[part]
        except (KeyError, IndexError, TypeError) as exc:
            raise BoundaryError(f"unresolved pointer: {pointer}") from exc
    return value


def pack(value: Any) -> dict[str, Any]:
    """Use the existing lossless decimal-string/path convention before JCS."""
    integers: list[str] = []

    def walk(node: Any, pointer: str) -> Any:
        if type(node) is int and not -SAFE <= node <= SAFE:
            integers.append(pointer)
            return str(node)
        if isinstance(node, dict):
            if any(type(k) is not str for k in node):
                raise BoundaryError("JSON object keys must be strings")
            return {k: walk(v, pointer + "/" + token(k)) for k, v in sorted(node.items())}
        if isinstance(node, list):
            return [walk(v, f"{pointer}/{i}") for i, v in enumerate(node)]
        return copy.deepcopy(node)

    result = {"value": walk(value, ""), "integer_paths": sorted(integers)}
    canonical_json_bytes(result)  # Reject unsupported/nonfinite/invalid-Unicode inputs.
    return result


def unpack(encoded: Mapping[str, Any]) -> Any:
    if set(encoded) != {"value", "integer_paths"}:
        raise BoundaryError("invalid exact-value envelope")
    value = copy.deepcopy(encoded["value"])
    paths = encoded["integer_paths"]
    if not isinstance(paths, list) or any(type(p) is not str for p in paths):
        raise BoundaryError("integer paths must be strings")
    if paths != sorted(set(paths)):
        raise BoundaryError("integer paths must be unique and sorted")
    for pointer in paths:
        text = at(value, pointer)
        if not isinstance(text, str) or not re.fullmatch(r"0|-?[1-9][0-9]*", text):
            raise BoundaryError(f"noncanonical integer text: {pointer}")
        number = int(text)
        if -SAFE <= number <= SAFE:
            raise BoundaryError(f"unnecessary integer encoding: {pointer}")
        path = parts(pointer)
        if not path:
            value = number
        else:
            parent = at(value, "/" + "/".join(token(p) for p in path[:-1])) if len(path) > 1 else value
            parent[int(path[-1]) if isinstance(parent, list) else path[-1]] = number
    return value


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def exact_digest(value: Any) -> str:
    return digest(pack(value))


def loads(raw: bytes) -> Any:
    return parse_identity_json(raw)


def require_fields(value: Any, allowed: set[str], required: set[str], context: str) -> None:
    if not isinstance(value, dict):
        raise BoundaryError(f"expected object: {context}")
    unknown, missing = set(value) - allowed, required - set(value)
    if unknown or missing:
        raise BoundaryError(f"unreviewed field role at {context}: extra={sorted(unknown)} missing={sorted(missing)}")


SCHEMA_MAPS = {"$defs", "definitions", "properties", "patternProperties", "dependentSchemas"}
SCHEMA_LISTS = {"allOf", "anyOf", "oneOf", "prefixItems"}
SCHEMA_SINGLE = {"items", "additionalProperties", "unevaluatedProperties", "contains", "not", "if", "then", "else", "propertyNames", "contentSchema", "additionalItems", "unevaluatedItems"}
ANNOTATIONS = {"title", "description", "$comment", "examples"}
SCHEMA_ATOMS = {"$schema", "$id", "$anchor", "$ref", "$dynamicRef", "$dynamicAnchor", "type", "const", "enum", "default", "required", "format", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minLength", "maxLength", "pattern", "minItems", "maxItems", "uniqueItems", "minContains", "maxContains", "minProperties", "maxProperties", "dependentRequired", "readOnly", "writeOnly", "deprecated", "contentEncoding", "contentMediaType", "x-riverhog-encoded-bytes-max", "x-unicode-normalization"}


def partition(family: str, original: Any) -> tuple[Any, list[dict[str, Any]]]:
    """Reviewed structural roles, never substring matching or instance taxonomy.

    Only schema annotations and a contract_max rationale are moved in this slice.
    Literal instance data (default/enum/const) is never traversed as schema.
    Other extent policies deliberately fail pending substantive scope review.
    """
    moved: list[dict[str, Any]] = []

    def schema(node: Any, pointer: str) -> Any:
        if type(node) is bool:
            return node
        allowed = SCHEMA_MAPS | SCHEMA_LISTS | SCHEMA_SINGLE | SCHEMA_ATOMS | ANNOTATIONS | {"x-riverhog-extent", "dependencies"}
        require_fields(node, allowed, set(), pointer)
        result: dict[str, Any] = {}
        for key, val in node.items():
            p = pointer + "/" + token(key)
            if key in ANNOTATIONS:
                moved.append({"pointer": p, "role": "source-annotation", "value": copy.deepcopy(val)})
            elif key == "x-riverhog-extent":
                require_fields(val, {"policy", "reason"}, {"policy"}, p)
                if val["policy"] != "contract_max" or ("reason" in val and not isinstance(val["reason"], str)):
                    raise BoundaryError(f"extent meaning needs explicit review: {p}")
                if not any(k in node for k in ("maxItems", "maxProperties", "maxLength", "maximum", "exclusiveMaximum", "x-riverhog-encoded-bytes-max")):
                    raise BoundaryError("contract_max must have an actual declared constraint")
                result[key] = {"policy": val["policy"]}
                if "reason" in val:
                    moved.append({"pointer": p + "/reason", "role": "rationale", "value": val["reason"]})
            elif key in SCHEMA_MAPS:
                if not isinstance(val, dict):
                    raise BoundaryError(f"schema map expected: {p}")
                result[key] = {n: schema(v, p + "/" + token(n)) for n, v in val.items()}
            elif key in SCHEMA_LISTS or (key == "items" and isinstance(val, list)):
                if not isinstance(val, list):
                    raise BoundaryError(f"schema array expected: {p}")
                result[key] = [schema(v, f"{p}/{i}") for i, v in enumerate(val)]
            elif key in SCHEMA_SINGLE:
                result[key] = schema(val, p)
            elif key == "dependencies":
                if not isinstance(val, dict):
                    raise BoundaryError(f"dependency map expected: {p}")
                result[key] = {n: schema(v, p + "/" + token(n)) if isinstance(v, (dict, bool)) else copy.deepcopy(v) for n, v in val.items()}
            else:
                result[key] = copy.deepcopy(val)
        return result

    if family == "python":
        require_fields(original, {"distribution", "module", "name", "owner", "unit", "contract"}, {"distribution", "module", "name", "unit", "contract"}, "python")
        value = copy.deepcopy(original)
        contract = value["contract"]
        require_fields(contract, {"kind", "signature", "value", "type", "enum_values", "schema", "fields"}, {"kind"}, "python.contract")
        if not isinstance(contract["kind"], str):
            raise BoundaryError("recorded Python kind must be text")
        if value["unit"] not in {"export", "member"}:
            raise BoundaryError("unknown Python unit")
        if value["unit"] == "member" and not isinstance(value.get("owner"), str):
            raise BoundaryError("Python member requires an explicit owner")
        if "schema" in contract:
            contract["schema"] = schema(contract["schema"], "/contract/schema")
    elif family == "http-schemas":
        value = schema(original, "")
    elif family == "compatibility-guarantees":
        if not isinstance(original, str):
            raise BoundaryError("compatibility promise must be recorded text")
        value = original
    elif family == "extent":
        require_fields(original, {"policy", "authority", "exceeded", "requirement"}, {"policy", "authority", "exceeded"}, "schema-bound rule")
        if original["policy"] != "fixed-or-contract-max":
            raise BoundaryError("only the schema-bound rule is reviewed by this slice")
        value = {k: copy.deepcopy(v) for k, v in original.items() if k != "requirement"}
        if "requirement" in original:
            moved.append({"pointer": "/requirement", "role": "authoring-requirement", "value": original["requirement"]})
    else:
        raise BoundaryError(f"interface not implemented in this reference: {family}")
    return value, sorted(moved, key=lambda x: x["pointer"])


def restore(value: Any, moved: list[dict[str, Any]]) -> Any:
    result = copy.deepcopy(value)
    seen: set[str] = set()
    for field in sorted(moved, key=lambda x: len(parts(x["pointer"]))):
        pointer = field["pointer"]
        path = parts(pointer)
        if not path or pointer in seen:
            raise BoundaryError("invalid or duplicated moved field")
        parent_path = "/" + "/".join(token(p) for p in path[:-1]) if len(path) > 1 else ""
        parent = at(result, parent_path)
        if not isinstance(parent, dict) or path[-1] in parent:
            raise BoundaryError(f"audit field overwrites contract data: {pointer}")
        parent[path[-1]] = copy.deepcopy(field["value"])
        seen.add(pointer)
    return result


def schema_nodes(value: Any, pointer: str = ""):
    """Visit only schema positions, never defaults/enum instances."""
    if not isinstance(value, dict):
        return
    yield pointer, value
    for key, child in value.items():
        p = pointer + "/" + token(key)
        if key in SCHEMA_MAPS and isinstance(child, dict):
            for name, item in child.items():
                yield from schema_nodes(item, p + "/" + token(name))
        elif key in SCHEMA_LISTS or (key == "items" and isinstance(child, list)):
            for index, item in enumerate(child):
                yield from schema_nodes(item, f"{p}/{index}")
        elif key in SCHEMA_SINGLE:
            yield from schema_nodes(child, p)
        elif key == "dependencies" and isinstance(child, dict):
            for name, item in child.items():
                if isinstance(item, dict):
                    yield from schema_nodes(item, p + "/" + token(name))


def reference_targets(records: dict[str, Any], identity: str):
    record = records[identity]
    family = record["interface"]
    schema = record["value"] if family == "http-schemas" else record["value"].get("contract", {}).get("schema") if family == "python" else None
    if schema is None:
        return
    for pointer, node in schema_nodes(schema):
        for keyword in ("$ref", "$dynamicRef"):
            if keyword not in node:
                continue
            reference = node[keyword]
            if not isinstance(reference, str) or not reference.startswith("#/") or keyword != "$ref":
                raise BoundaryError("external/anchor/dynamic schema references require explicit integration; not silently resolved")
            if family == "python":
                at(schema, reference[1:])
                yield pointer + "/" + keyword, identity, "/contract/schema" + reference[1:]
            else:
                path = parts(record["address"])
                if len(path) < 5 or path[:2] != ["external_contract", "http_openapi"]:
                    raise BoundaryError("HTTP schema has no explicit document namespace")
                target = "/" + "/".join(token(x) for x in path[:3]) + reference[1:]
                matches = [(key, other) for key, other in records.items() if target == other["address"] or target.startswith(other["address"] + "/")]
                if len(matches) != 1:
                    raise BoundaryError(f"missing/ambiguous normative reference: {target}")
                key, other = matches[0]
                suffix = target[len(other["address"]):]
                at(other["value"], suffix)
                yield pointer + "/" + keyword, key, suffix


def validate_references(records: dict[str, Any]) -> None:
    addresses = [record["address"] for record in records.values()]
    for index, address in enumerate(addresses):
        for other in addresses[index+1:]:
            if address == other or address.startswith(other + "/") or other.startswith(address + "/"):
                raise BoundaryError("overlapping normative addresses")
    for identity in records:
        list(reference_targets(records, identity))


def validate_closure(closure: dict[str, Any]) -> dict[str, Any]:
    require_fields(closure, {"format", "series", "records"}, {"format", "series", "records"}, "closure")
    if closure["format"] != CLOSURE_FORMAT or not isinstance(closure["series"], str):
        raise BoundaryError("not a reference Closure")
    canonical_json_bytes(closure)  # Standalone validation rejects non-JSON values too.
    records = unpack(closure["records"])
    if not isinstance(records, dict) or not records:
        raise BoundaryError("empty reference selection")
    names: dict[tuple[str, str], str] = {}
    for identity, record in records.items():
        require_fields(record, {"authority", "interface", "name", "address", "value"}, {"authority", "interface", "name", "address", "value"}, identity)
        parts(record["address"])
        if not all(isinstance(record[k], str) and record[k] for k in ("authority", "interface", "name", "address")):
            raise BoundaryError("empty or non-text identity")
        normalized, moved = partition(record["interface"], record["value"])
        if moved or normalized != record["value"]:
            raise BoundaryError("audit fields leaked into normative Closure")
        if record["interface"] == "python":
            val = record["value"]
            key = (record["authority"], record["name"])
            if key in names:
                raise BoundaryError("ambiguous Python identity")
            names[key] = identity
            if val["distribution"] != record["authority"]:
                raise BoundaryError("Python distribution disagrees with authority")
    for identity, record in records.items():
        if record["interface"] == "python" and record["value"]["unit"] == "member":
            val = record["value"]
            parent = names.get((record["authority"], val["owner"]))
            if parent is None or parent == identity or records[parent]["value"]["unit"] != "export" or records[parent]["value"]["module"] != val["module"]:
                raise BoundaryError("member has no unique same-module export owner in selection")
    validate_references(records)
    return records


def build(source: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Source records are a selected adapter input, not a new authoring authority."""
    require_fields(source, {"series", "source_revision", "records", "extent_analysis", "witnesses", "discovery_accounting"}, {"series", "source_revision", "records"}, "input")
    if not re.fullmatch(r"[0-9a-f]{40}", source["source_revision"]):
        raise BoundaryError("an exact source revision is required")
    normative, overlays, originals = {}, {}, {}
    for record in source["records"]:
        require_fields(record, {"id", "authority", "interface", "name", "pointer", "value", "sources"}, {"id", "authority", "interface", "name", "pointer", "value", "sources"}, "source record")
        identity = record["id"]
        if identity in normative:
            raise BoundaryError("duplicate element identity")
        value, moved = partition(record["interface"], record["value"])
        if restore(value, moved) != record["value"]:
            raise BoundaryError("partition lost input information")
        normative[identity] = {k: record[k] for k in ("authority", "interface", "name")}
        normative[identity]["address"] = record["pointer"]
        normative[identity]["value"] = value
        overlays[identity] = {"source_pointer": record["pointer"], "sources": copy.deepcopy(record["sources"]), "moved_fields": moved, "original_value_sha256": exact_digest(record["value"])}
        originals[identity] = record["value"]
    closure = {"format": CLOSURE_FORMAT, "series": source["series"], "records": pack(normative)}
    validate_closure(closure)
    # Reject overlapping source ownership; longest-prefix selection must not mask it.
    pointers = [(v["source_pointer"], k) for k, v in overlays.items()]
    for i, (p, _) in enumerate(pointers):
        parts(p)
        for q, _ in pointers[i+1:]:
            if p == q or p.startswith(q + "/") or q.startswith(p + "/"):
                raise BoundaryError("overlapping source ownership")
    analyses = []
    decision_ids: set[str] = set()
    for decision in source.get("extent_analysis", []):
        identity = decision["id"]
        if identity in decision_ids:
            raise BoundaryError("duplicate extent analysis identity")
        decision_ids.add(identity)
        pointer = decision["source_pointer"]
        matches = [(p, k) for p, k in pointers if pointer == p or pointer.startswith(p + "/")]
        if len(matches) != 1:
            raise BoundaryError(f"analysis target not uniquely owned: {identity}")
        p, target = matches[0]
        relative = pointer[len(p):]
        at(originals[target], relative)
        # An analysis targeting an annotation cannot manufacture a contract subject.
        at(normative[target]["value"], relative)
        analyses.append({"id": identity, "target": target, "pointer": relative, "recorded_analysis": copy.deepcopy(decision)})
    witnesses = copy.deepcopy(source.get("witnesses", []))
    witness_ids: set[str] = set()
    for witness in witnesses:
        require_fields(witness, {"id", "analysis_ids", "record"}, {"id", "analysis_ids", "record"}, "witness")
        if witness["id"] in witness_ids:
            raise BoundaryError("duplicate witness")
        witness_ids.add(witness["id"])
        if not witness["analysis_ids"] or not set(witness["analysis_ids"]) <= decision_ids:
            raise BoundaryError("unresolved witness association")
        witness["analysis_ids"] = sorted(set(witness["analysis_ids"]))
    audit = {"format": AUDIT_FORMAT, "closure_sha256": digest(closure), "source_revision": source["source_revision"], "data": pack({"overlays": overlays, "extent_analysis": sorted(analyses, key=lambda x:x["id"]), "witnesses": sorted(witnesses, key=lambda x:x["id"]), "discovery_accounting": copy.deepcopy(source.get("discovery_accounting", {}))})}
    validate_pair(closure, audit)
    return closure, audit


def validate_analysis(entry: dict[str, Any], records: dict[str, Any]) -> None:
    """Check copied limits against exact owned fields, not the analysis policy label."""
    value = at(records[entry['target']]['value'], entry['pointer'])
    decision = entry['recorded_analysis']
    fields = {('length', 'characters'): ('minLength', 'maxLength'), ('encoded-size', 'bytes'): (None, 'x-riverhog-encoded-bytes-max'), ('value', 'schema-value'): ('minimum', 'maximum'), ('cardinality', 'items'): ('minItems', 'maxItems'), ('cardinality', 'entries'): ('minProperties', 'maxProperties')}
    selected = fields.get((decision.get('dimension'), decision.get('unit')))
    if selected is None:
        raise BoundaryError('analysis dimension needs a reviewed exact-field mapping')
    for bound, field in zip(('minimum', 'maximum'), selected):
        if bound in decision and decision[bound] is not None:
            if not isinstance(value, dict) or field is None or field not in value or value[field] != decision[bound]:
                raise BoundaryError('recorded extent bound disagrees with its owned field')


def validate_pair(closure: dict[str, Any], audit: dict[str, Any]) -> dict[str, Any]:
    records = validate_closure(closure)
    require_fields(audit, {"format", "closure_sha256", "source_revision", "data"}, {"format", "closure_sha256", "source_revision", "data"}, "audit")
    if audit["format"] != AUDIT_FORMAT or audit["closure_sha256"] != digest(closure):
        raise BoundaryError("Audit Record does not bind this Closure")
    if not re.fullmatch(r"[0-9a-f]{40}", audit["source_revision"]):
        raise BoundaryError("invalid source revision")
    data = unpack(audit["data"])
    require_fields(data, {"overlays", "extent_analysis", "witnesses", "discovery_accounting"}, {"overlays", "extent_analysis", "witnesses", "discovery_accounting"}, "audit.data")
    if set(data["overlays"]) != set(records):
        raise BoundaryError("audit ownership differs from Closure")
    for identity, record in records.items():
        overlay = data["overlays"][identity]
        if overlay["source_pointer"] != record["address"]:
            raise BoundaryError("audit source does not match normative address")
        original = restore(record["value"], overlay["moved_fields"])
        if exact_digest(original) != overlay["original_value_sha256"]:
            raise BoundaryError("lossless partition check failed")
        value, moved = partition(record["interface"], original)
        if value != record["value"] or moved != overlay["moved_fields"]:
            raise BoundaryError("field role mapping changed or was bypassed")
    ids = set()
    for entry in data["extent_analysis"]:
        if entry["id"] in ids or entry["target"] not in records:
            raise BoundaryError("bad analysis identity/target")
        ids.add(entry["id"])
        validate_analysis(entry, records)
        at(records[entry["target"]]["value"], entry["pointer"])
        expected = data["overlays"][entry["target"]]["source_pointer"] + entry["pointer"]
        if expected != entry["recorded_analysis"]["source_pointer"]:
            raise BoundaryError("analysis pointer binding changed")
    for witness in data["witnesses"]:
        if not set(witness["analysis_ids"]) <= ids:
            raise BoundaryError("witness has an unresolved analysis ID")
    return data
