# state_schema.StateConnection.rollback_prepared

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-rollback-prepared:152bc7b66b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c2d0b760d7"></a>
- <a id="s-156a6a2794"></a>`distribution`: `state-schema`
- <a id="s-3eed8fe585"></a>`module`: `state_schema`
- <a id="s-6ef7076abe"></a>`name`: `rollback_prepared`
- <a id="s-c850df0f6c"></a>`owner`: `state_schema.StateConnection`
- <a id="s-7493a2e404"></a>`unit`: `member`

### Declared structure

- <a id="s-c973fdfddf"></a>`kind`: `"method"`
- <a id="s-40ff27b00a"></a>`signature`: `"\"(self, xid: 'Any', recover: 'bool' = False) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-01d9353b59"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.rollback_prepared`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 832fe8b3930d432511616925d62663ab35bbc44726d8beabc1bf22dca11ebd5d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, xid: 'Any', recover: 'bool' = False) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "rollback_prepared",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
