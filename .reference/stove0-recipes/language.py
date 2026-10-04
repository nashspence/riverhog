"""Non-authoritative #953 source grammar and static-lint prototype.

No Riverhog imports, network access, execution engine, production compiler, or
identity encoder. JSON Schema generation is a checked authoring aid, not proof
of accepted evidence, contract digests, or runtime conformance.
"""
from __future__ import annotations

import argparse
import copy
import json
import re
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from ruamel.yaml import YAML


def obj(properties: dict, required: tuple[str, ...] = ()) -> dict:
    return {"type": "object", "properties": properties, "required": list(required),
            "additionalProperties": False}


def seq(items: dict, minimum: int = 0, unique: bool = False) -> dict:
    return {"type": "array", "items": items, "minItems": minimum, "uniqueItems": unique}


def ref(name: str) -> dict:
    return {"$ref": "#/$defs/" + name}


def named(values: dict, minimum: int = 0) -> dict:
    return {"type": "object", "propertyNames": ref("name"),
            "additionalProperties": values, "minProperties": minimum}


def schema() -> dict:
    d: dict[str, Any] = {}
    d["name"] = {"type": "string", "pattern": "^[a-z][a-z0-9_-]*$"}
    d["semantic_id"] = {"type": "string", "pattern": r"^[a-z0-9](?:[a-z0-9._/-]{0,158}[a-z0-9])?$"}
    d["pointer"] = {"type": "string", "pattern": r"^(?:/(?:[^~/]|~[01])*)*$"}
    d["view"] = {"type": "string", "pattern": "^[a-z][a-z0-9_-]*\\.[a-z][a-z0-9_-]*$"}
    d["json_object"] = {"type": "object"}
    d["quantifier"] = {"enum": ["any", "every", "none"]}
    d["test"] = obj({"path": ref("pointer"), "op": {"enum": ["eq", "ne", "in", "contains", "exists"]},
                     "value": {}}, ("path", "op", "value"))
    d["test"]["allOf"] = [
        {"if": {"properties": {"op": {"const": "exists"}}},
         "then": {"properties": {"value": {"type": "boolean"}}}},
        {"if": {"properties": {"op": {"const": "in"}}},
         "then": {"properties": {"value": seq({}, 1)}}},
    ]
    for typ in ("row", "predicate"):
        options = [{"type": "boolean"}, obj({"all": seq(ref(typ), 1)}, ("all",)),
                   obj({"any": seq(ref(typ), 1)}, ("any",)), obj({"not": ref(typ)}, ("not",))]
        if typ == "row":
            options += [obj({"test": ref("test")}, ("test",)), obj({"items": obj({
                "path": ref("pointer"), "quantifier": ref("quantifier"), "where": ref("row")},
                ("path", "quantifier", "where"))}, ("items",))]
        else:
            options += [obj({"facts": obj({"view": ref("view"),
                "scope": {"enum": ["input", "candidate", "self"]},
                "roles": seq(ref("name"), 1, True), "quantifier": ref("quantifier"),
                "where": ref("row")}, ("view", "scope", "quantifier", "where"))}, ("facts",))]
        d[typ] = {"oneOf": options}
    d["subjects"] = {"oneOf": [{"const": "all"}, obj({"roles": seq(ref("name"), 1, True)}, ("roles",))]}
    d["port"] = {"oneOf": [{"const": "all"}, obj({"roles": seq(ref("name"), 1, True)}, ("roles",)),
                                     obj({"evidence": ref("name")}, ("evidence",))]}
    d["observe"] = obj({"use": ref("name"), "executor": ref("name"), "inputs": named(ref("port"), 1),
        "after": seq(ref("name"), 0, True), "options": ref("json_object"),
        "retrieve": {"enum": ["available-only", "allow"]}}, ("use",))
    d["classification"] = obj({"cases": seq(obj({"role": ref("name"), "when": ref("predicate")},
        ("role", "when"))), "otherwise": {"oneOf": [ref("name"), {"type": "null"}]}}, ("otherwise",))
    d["group"] = obj({"primary": ref("name"), "attach": seq(ref("name"), 0, True),
                      "prefer": seq(ref("view"))}, ("primary",))
    d["binding"] = obj({"from": {"enum": ["parameters", "evaluation"]}, "path": ref("pointer"),
        "to": {"enum": ["intent", "options"]}, "at": ref("pointer"),
        "mode": {"enum": ["insert", "replace", "merge-object"]}}, ("from", "path", "to", "at", "mode"))
    d["output"] = obj({"archive_store": {"type": "string", "minLength": 1},
        "use_cache": {"type": "boolean"}, "copy_to": seq({"type": "string"}, 0, True),
        "tags": seq({"type": "string"}, 0, True)})
    shared = {"intent": ref("json_object"), "bind": seq(ref("binding"))}
    d["operation_call"] = obj({**shared, "operation": ref("name"), "executor": ref("name"),
        "options": ref("json_object"), "evidence": seq(ref("name"), 0, True),
        "retrieve": {"enum": ["available-only", "allow"]}, "output": ref("output")}, ("operation",))
    recipe_binding = copy.deepcopy(d["binding"])
    recipe_binding["properties"]["to"] = {"const": "intent"}
    d["recipe_binding"] = recipe_binding
    d["recipe_call"] = obj({"recipe": ref("name"), "intent": ref("json_object"),
                            "bind": seq(ref("recipe_binding"))}, ("recipe",))
    d["call"] = {"oneOf": [ref("operation_call"), ref("recipe_call")]}
    d["branch"] = obj({"select": {"oneOf": [{"const": "all"},
        obj({"groups": ref("name")}, ("groups",))]}, "when": ref("predicate"), "call": ref("call")}, ("call",))
    d["join"] = obj({"members": named(seq(ref("semantic_id"), 1, True), 2),
                     "call": ref("operation_call")}, ("members", "call"))
    d["loss"] = obj({"id": ref("semantic_id"), "evidence": seq(obj({
        "view": ref("view"), "verdict": obj({"path": ref("pointer"), "equals": {}}, ("path", "equals"))},
        ("view", "verdict")), 1)}, ("id", "evidence"))
    d["decision"] = obj({"when": ref("predicate"), "no_output": obj({
        "code": ref("semantic_id"), "message": {"type": "string", "minLength": 1},
        "source_loss": ref("loss")}, ("code", "message"))}, ("when", "no_output"))
    d["retirement"] = {"oneOf": [obj({"mode": {"const": "retain"}}, ("mode",)),
        obj({"mode": {"const": "after-settlement"}, "grace_seconds": {"type": "integer", "minimum": 0}}, ("mode",))]}
    d["source"] = obj({"unmatched": {"enum": ["retain-in-source", "reject-work"]},
                       "retirement": ref("retirement")})
    top = obj({"format": {"const": "stove0-recipe/v1"}, "id": ref("semantic_id"),
        "revision": {"oneOf": [{"type": "integer", "minimum": 1}, {"type": "string", "pattern": "^[1-9][0-9]*$"}]},
        "description": {"type": "string"}, "parameters": ref("json_object"),
        "roles": named(ref("semantic_id"), 1), "observe": named(ref("observe")),
        "classify": ref("classification"), "groups": named(ref("group")),
        "decisions": seq(ref("decision"), 1), "fork": named(ref("branch"), 1),
        "join": ref("join"), "export": {"oneOf": [{"const": "join"},
            obj({"branch": ref("name")}, ("branch",))]}, "source": ref("source")}, ("format", "id", "revision"))
    top.update({"$schema": "https://json-schema.org/draft/2020-12/schema", "$defs": d,
                "anyOf": [{"required": ["fork"]}, {"required": ["decisions"]}]})
    return top


def walk(value: Any):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from walk(child)
    elif isinstance(value, list):
        for child in value:
            yield from walk(child)


def schema_nodes(value: Any):
    """Visit schema positions, not literal defaults/examples that resemble schemas."""
    if not isinstance(value, dict):
        return
    yield value
    for key in ("$defs", "definitions", "properties", "patternProperties", "dependentSchemas"):
        for child in value.get(key, {}).values():
            yield from schema_nodes(child)
    for key in ("additionalProperties", "unevaluatedProperties", "propertyNames", "items", "contains", "unevaluatedItems", "if", "then", "else", "not"):
        yield from schema_nodes(value.get(key))
    for key in ("allOf", "anyOf", "oneOf", "prefixItems"):
        for child in value.get(key, []):
            yield from schema_nodes(child)


def pointer_parts(value: str) -> tuple[str, ...]:
    return tuple(p.replace("~1", "/").replace("~0", "~") for p in value.split("/")[1:])


def diagnostics(document: dict, resources: dict) -> list[str]:
    """Check grammar and selected static invariants; never call observers/targets."""
    errors = [f"/{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in
              sorted(Draft202012Validator(schema()).iter_errors(document), key=lambda e: str(e.absolute_path))]
    if errors:
        return errors
    role_map = document.get("roles", {"source": "stove0.source/v1"})
    roles = set(role_map)
    observations = document.get("observe", {})
    branches = document.get("fork", {})
    groups = document.get("groups", {})
    graph = {name: set(task.get("after", [])) for name, task in observations.items()}
    graph["$classify"] = set()

    def require(ok: bool, message: str):
        if not ok:
            errors.append(message)

    def resource(name: str, kind: str) -> dict:
        entry = resources.get(name, {})
        require(entry.get("kind") == kind, f"resource {name!r}: expected {kind}")
        return entry

    def view(name: str, kind: str = "facts") -> str:
        task_id, view_id = name.split(".")
        require(task_id in observations, f"view {name!r}: unknown observation task")
        entry = resources.get(observations.get(task_id, {}).get("use"), {})
        require(entry.get("views", {}).get(view_id) == kind, f"view {name!r}: expected {kind} view")
        return task_id

    def predicate(value: Any, contexts: set[str], candidate_roles: set[str] | None = None) -> set[str]:
        dependencies = set()
        pending = [value]
        while pending:
            node = pending.pop()
            if not isinstance(node, dict):
                continue
            if "all" in node:
                pending.extend(node["all"])
            elif "any" in node:
                pending.extend(node["any"])
            elif "not" in node:
                pending.append(node["not"])
            if "facts" not in node:
                continue
            fact = node["facts"]
            dependencies.add(view(fact["view"]))
            require(fact["scope"] in contexts, f"predicate: invalid {fact['scope']} scope here")
            require(set(fact.get("roles", [])) <= roles, "predicate: unknown role")
            if fact["scope"] == "candidate" and candidate_roles is not None:
                require(set(fact.get("roles", [])) <= candidate_roles, "predicate: role is outside the selected group")
            require(not (fact["scope"] == "self" and fact.get("roles")), "classification cannot filter its not-yet-assigned role")
        return dependencies

    require(len(set(role_map.values())) == len(role_map), "roles: duplicate semantic role IDs")
    for name, task in observations.items():
        entry = resource(task["use"], "observer")
        ports = entry.get("ports", {})
        inputs = task.get("inputs", {"subjects": "all"})
        require(set(inputs) == set(ports), f"observe.{name}: input ports differ from interface")
        for port_name, binding in inputs.items():
            if isinstance(binding, dict) and "evidence" in binding:
                graph[name].add(binding["evidence"])
                require(ports.get(port_name) == "evidence", f"observe.{name}.{port_name}: not an evidence port")
            else:
                require(ports.get(port_name) == "subjects", f"observe.{name}.{port_name}: not a subjects port")
                selected = binding
                if isinstance(selected, dict):
                    require(set(selected["roles"]) <= roles, f"observe.{name}: unknown subject role")
                    graph[name].add("$classify")
    classify = document.get("classify", {"otherwise": "source"})
    otherwise = classify["otherwise"]
    require(otherwise is None or otherwise in roles, "classify: unknown otherwise role")
    for case in classify.get("cases", []):
        require(case["role"] in roles, "classify: unknown case role")
        graph["$classify"].update(predicate(case["when"], {"input", "self"}))
    for name, group in groups.items():
        require(group["primary"] in roles, f"groups.{name}: unknown primary role")
        require(set(group.get("attach", [])) <= roles, f"groups.{name}: unknown associated role")
        require(group["primary"] not in group.get("attach", []), f"groups.{name}: primary is also associated")
        require(bool(group.get("attach")) == bool(group.get("prefer")), f"groups.{name}: associated roles and relation tiers must be paired")
        for relation in group.get("prefer", []):
            view(relation, "relation")
    for name, dependencies in graph.items():
        require(dependencies <= graph.keys(), f"dependency of {name}: unknown task")
    ready, remaining = set(), set(graph)
    while remaining:
        next_ready = {name for name in remaining if graph[name] <= ready}
        if not next_ready:
            require(False, "observation/classification dependency cycle or unresolved predecessor")
            break
        ready |= next_ready
        remaining -= next_ready

    def check_call(call: dict, join: bool = False) -> dict:
        kind = "operation" if "operation" in call else "recipe"
        entry = resource(call[kind], kind)
        if join:
            require(entry.get("result") == "collection", "join call must produce a collection")
        if entry.get("result") == "external-effect":
            require("output" not in call, "effect call cannot declare output collection policy")
        output = call.get("output", {})
        require(output.get("archive_store") not in output.get("copy_to", []), "output primary archive store cannot also be a copy destination")
        for evidence in call.get("evidence", []):
            require(evidence in observations, f"call forwards unknown task {evidence}")
        destinations = []
        for binding in call.get("bind", []):
            key = (binding["to"], pointer_parts(binding["at"]))
            for target, path in destinations:
                if target == key[0] and (path[:len(key[1])] == key[1] or key[1][:len(path)] == path):
                    require(False, "call bindings have overlapping destinations")
            destinations.append(key)
        return entry

    call_entries = {}
    for name, branch in branches.items():
        selected = branch.get("select", "all")
        if isinstance(selected, dict):
            require(selected["groups"] in groups, f"fork.{name}: unknown group")
        selected_group = groups.get(selected["groups"], {}) if isinstance(selected, dict) else {}
        candidate_roles = {selected_group.get("primary"), *selected_group.get("attach", [])}
        predicate(branch.get("when", True), {"input", "candidate"} if isinstance(selected, dict) else {"input"}, candidate_roles)
        call_entries[name] = check_call(branch["call"])
    for decision in document.get("decisions", []):
        predicate(decision["when"], {"input"})
        for slot in decision["no_output"].get("source_loss", {}).get("evidence", []):
            view(slot["view"])
    join = document.get("join")
    if join is not None:
        check_call(join["call"], join=True)
        for name in join["members"]:
            require(name in branches, f"join: unknown member {name}")
            entry = call_entries.get(name, {})
            has_collection = entry.get("result") == "collection" if entry.get("kind") == "operation" else entry.get("exports_collection", False)
            require(has_collection, f"join member {name}: no declared collection result")
    export = document.get("export")
    if export == "join":
        require(join is not None, "export refers to absent join")
    elif isinstance(export, dict):
        entry = call_entries.get(export["branch"], {})
        require(entry.get("result") == "collection" or entry.get("exports_collection", False), "export branch has no declared collection result")
    retirement = document.get("source", {}).get("retirement", {}).get("mode", "retain")
    if retirement == "after-settlement":
        for name, entry in call_entries.items():
            require(entry.get("retirement_permitted", False), f"fork.{name}: operation/descendant closure does not permit retirement")
        for decision in document.get("decisions", []):
            require("source_loss" in decision["no_output"], "no-output retirement requires source-loss evidence")
    if "parameters" in document:
        try:
            Draft202012Validator.check_schema(document["parameters"])
        except Exception as exc:
            require(False, f"parameters: invalid schema: {exc}")
            return errors
        for node in schema_nodes(document["parameters"]):
            for keyword in ("$ref", "$dynamicRef"):
                require(not (isinstance(node.get(keyword), str) and not node[keyword].startswith("#")), "parameters: remote schema references are forbidden")
    return errors


def read_documents(path: Path) -> list[dict]:
    """Safe YAML 1.2, rejecting duplicate keys and aliases; JSON-domain only."""
    text = path.read_text(encoding="utf-8")
    loader = YAML(typ="safe")
    loader.version = (1, 2)
    loader.allow_duplicate_keys = False
    from ruamel.yaml.events import AliasEvent, ScalarEvent
    for event in loader.parse(text):
        if isinstance(event, AliasEvent):
            raise ValueError("YAML aliases are not part of this source language; use named resources/subrecipes")
        if isinstance(event, ScalarEvent) and event.value == "<<" and event.style in (None, ""):
            raise ValueError("YAML merge syntax is forbidden; quote literal << data")
    documents = list(loader.load_all(text))
    for document in documents:
        def check(value):
            if isinstance(value, dict):
                if any(not isinstance(key, str) for key in value):
                    raise ValueError("object keys must be strings")
                for child in value.values(): check(child)
            elif isinstance(value, list):
                for child in value: check(child)
            elif value is not None and type(value) not in (str, int, float, bool):
                raise ValueError("non-JSON YAML value")
        check(document)
        json.dumps(document, allow_nan=False)
    return documents


def fixture_resources() -> dict:
    """Mock interface summaries only; NOT production dependency documents/pins."""
    basic = {"kind": "observer", "ports": {"subjects": "subjects"}, "views": {"records": "facts"}}
    result = {name: copy.deepcopy(basic) for name in ("metadata", "probe", "hint", "policy", "sampling")}
    result["provenance"] = {**copy.deepcopy(basic), "views": {"records": "facts", "describes": "relation"}}
    result["names"] = {"kind": "observer", "ports": {"primary": "subjects", "associated": "subjects", "provenance": "evidence"},
                       "views": {"full_leaf": "relation", "stem": "relation"}}
    for name in ("encode", "assemble", "review"):
        result[name] = {"kind": "operation", "result": "collection", "retirement_permitted": True}
    result["audio"] = {"kind": "operation", "result": "collection", "retirement_permitted": False}
    result["deliver"] = {"kind": "operation", "result": "external-effect", "retirement_permitted": True}
    result["child"] = {"kind": "recipe", "exports_collection": True, "retirement_permitted": True}
    result["no_output_child"] = {"kind": "recipe", "exports_collection": False, "retirement_permitted": True}
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("schema", "check"))
    parser.add_argument("path", nargs="?", type=Path)
    args = parser.parse_args()
    if args.command == "schema":
        print(json.dumps(schema(), indent=2))
        return 0
    if args.path is None:
        parser.error("check requires a YAML example path")
    failures = 0
    for document in read_documents(args.path):
        errors = diagnostics(document, fixture_resources())
        label = document.get("id", "<missing-id>") if isinstance(document, dict) else "<non-object>"
        print(label, "FAIL" if errors else "PASS")
        for error in errors: print("  " + error)
        failures += bool(errors)
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
