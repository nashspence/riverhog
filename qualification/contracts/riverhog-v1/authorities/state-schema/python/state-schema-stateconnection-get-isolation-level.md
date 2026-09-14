# state_schema.StateConnection.get_isolation_level

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-get-isolation-level:170948197f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-52eff3f9dc"></a>
- <a id="s-1879b48006"></a>`distribution`: `state-schema`
- <a id="s-c23e01fe72"></a>`module`: `state_schema`
- <a id="s-badd56a965"></a>`name`: `get_isolation_level`
- <a id="s-4f19517b86"></a>`owner`: `state_schema.StateConnection`
- <a id="s-095985e88d"></a>`unit`: `member`

### Declared structure

- <a id="s-28e3c4cfd3"></a>`kind`: `"method"`
- <a id="s-090216ef1d"></a>`signature`: `"\"(self) -> 'IsolationLevel'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-200a2d84a5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.get_isolation_level`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fd14a702cef5baf8c8917fcb0ce4bf7992410ba7386a249f820cc2bb6a64a5ee -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'IsolationLevel'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "get_isolation_level",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
