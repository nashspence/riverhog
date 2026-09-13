# state_schema.assert_schema_matches_metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-assert-schema-matches-metadata:fa70aa2a81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b990d30c78"></a>
| Field | Shape |
|---|---|
| <a id="s-0ee74ddb6a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-49c9995e2b"></a>`distribution` | "state-schema" |
| <a id="s-9f355b6d06"></a>`module` | "state_schema" |
| <a id="s-2a8ba0350a"></a>`name` | "assert_schema_matches_metadata" |
| <a id="s-b58531d645"></a>`unit` | "export" |

## Governing policies

- <a id="pa-8ad23e3314"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.assert_schema_matches_metadata`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d86ee8ec4a07afc2150f419ec633a858c07220841622d58a6e20295c36b19b5 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(bind: 'Connection | Engine', metadata: 'MetaData', *, version_table: 'str') -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "assert_schema_matches_metadata",
  "unit": "export"
}
```
