# state_schema.StateEngine.engine

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-engine:2044a1d937 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fecf64cd60"></a>
| Field | Shape |
|---|---|
| <a id="s-0d6b3c9945"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-646d9ce880"></a>`distribution` | "state-schema" |
| <a id="s-1714739d3f"></a>`module` | "state_schema" |
| <a id="s-840d5b8039"></a>`name` | "engine" |
| <a id="s-8005ceaef4"></a>`owner` | "state_schema.StateEngine" |
| <a id="s-21e3a73cf3"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-ec752534ad"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.engine`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 54e7993309dfeaea9e79e34792c78efb73b522db901629d7967eccc1a30707e7 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'Engine'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "engine",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```
