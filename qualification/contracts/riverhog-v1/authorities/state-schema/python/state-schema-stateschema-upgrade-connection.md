# state_schema.StateSchema.upgrade_connection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateschema-upgrade-connection:fc8ebc0fc5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f5e5424cc4"></a>
- <a id="s-36e6c6aa08"></a>`distribution`: `state-schema`
- <a id="s-97c5521b8a"></a>`module`: `state_schema`
- <a id="s-c77e0cb4c8"></a>`name`: `upgrade_connection`
- <a id="s-a4de25dfe6"></a>`owner`: `state_schema.StateSchema`
- <a id="s-fdfc813099"></a>`unit`: `member`

### Declared structure

- <a id="s-55eba58b1e"></a>`kind`: `"method"`
- <a id="s-579883799b"></a>`signature`: `"\"(self, connection: 'Connection') -> 'StateStatus'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateSchema](state-schema-stateschema.md)

## Governing policies

- <a id="pa-436cdd1c62"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateSchema.upgrade_connection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d8b847bfaafce1c85061c28a003cbca5c4e1a4fe9940ab0ec10a7df84b6974ba -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, connection: 'Connection') -> 'StateStatus'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "upgrade_connection",
  "owner": "state_schema.StateSchema",
  "unit": "member"
}
```
