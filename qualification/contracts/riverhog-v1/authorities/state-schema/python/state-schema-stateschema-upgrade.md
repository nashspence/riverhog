# state_schema.StateSchema.upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateschema-upgrade:b7b368474b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d6c1cc16d"></a>
- <a id="s-e591b18fa5"></a>`distribution`: `state-schema`
- <a id="s-c38bea37e9"></a>`module`: `state_schema`
- <a id="s-d8d5f7d177"></a>`name`: `upgrade`
- <a id="s-6b410557d5"></a>`owner`: `state_schema.StateSchema`
- <a id="s-61b0151938"></a>`unit`: `member`

### Declared structure

- <a id="s-680bc2958c"></a>`kind`: `"method"`
- <a id="s-64d15b7948"></a>`signature`: `"\"(self) -> 'StateStatus'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateSchema](state-schema-stateschema.md)

## Governing policies

- <a id="pa-b1845e1ece"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateSchema.upgrade`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 267bce0a3d19f335854a512cc4e7bf2256543fb049e1c163357e5bae50ad7227 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'StateStatus'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "upgrade",
  "owner": "state_schema.StateSchema",
  "unit": "member"
}
```
