# state_schema.StateConnection.connection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-connection:361fcc6e81 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6161fdc6ed"></a>
- <a id="s-775e5c2f8a"></a>`distribution`: `state-schema`
- <a id="s-644b957c43"></a>`module`: `state_schema`
- <a id="s-ab0e4313b3"></a>`name`: `connection`
- <a id="s-61d814071f"></a>`owner`: `state_schema.StateConnection`
- <a id="s-df04787170"></a>`unit`: `member`

### Declared structure

- <a id="s-afa33a8c9d"></a>`kind`: `"property"`
- <a id="s-14fa9da823"></a>`signature`: `"\"(self) -> 'PoolProxiedConnection'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-91304c5226"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.connection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 18bfc6ed61a38c7ea8f4f824f3067496d9b2b0c5a2aadc7f0aa39b729c3732fd -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'PoolProxiedConnection'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "connection",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
