# stove0_observer_protocol.JsonSchemaDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-jsonschemadocument:8ead62f404 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a557197c15"></a>
| Field | Shape |
|---|---|
| <a id="s-85a8e0e9c2"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-a0777ab8aa"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-6396f3290b"></a>`module` | "stove0_observer_protocol" |
| <a id="s-8bf4a9dc96"></a>`name` | "JsonSchemaDocument" |
| <a id="s-9c03c2d302"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.JsonSchemaDocument.verify_digest](stove0-observer-protocol-jsonschemadocument-verify-digest.md)
- [stove0_observer_protocol.JsonSchemaDocument.from_schema](stove0-observer-protocol-jsonschemadocument-from-schema.md)

## Governing policies

- <a id="pa-79d97074d5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.JsonSchemaDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9df63af915086aa769046c8459fffb4e6f6b8c9c4e1e3b12b369804c871962d8 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "044aac56d1ab78dca2a949caf25694292222a652e8b22484284cfd16c793e05f",
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "JsonSchemaDocument",
  "unit": "export"
}
```
