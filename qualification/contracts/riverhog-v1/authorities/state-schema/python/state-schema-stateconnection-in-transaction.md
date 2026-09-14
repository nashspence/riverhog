# state_schema.StateConnection.in_transaction

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-in-transaction:dba84e37cf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-db9ed8c4b8"></a>
- <a id="s-c10395689b"></a>`distribution`: `state-schema`
- <a id="s-402613bd14"></a>`module`: `state_schema`
- <a id="s-90e8c6c232"></a>`name`: `in_transaction`
- <a id="s-a5abfd3bc4"></a>`owner`: `state_schema.StateConnection`
- <a id="s-440c97a4f6"></a>`unit`: `member`

### Declared structure

- <a id="s-16613ea9c7"></a>`kind`: `"method"`
- <a id="s-dca7b889bf"></a>`signature`: `"\"(self) -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-908402bf0b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.in_transaction`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 619f05da064a9538304e01f976d5bbdec2b7858b6ce628682b9ba19732a2588e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "in_transaction",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
