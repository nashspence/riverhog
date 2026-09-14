# state_schema.StateConnection.default_isolation_level

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-default-isolation-level:1a1496b086 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6d8f7aa1aa"></a>
- <a id="s-28f6891c47"></a>`distribution`: `state-schema`
- <a id="s-d529883f2c"></a>`module`: `state_schema`
- <a id="s-705efbaa2c"></a>`name`: `default_isolation_level`
- <a id="s-c311b7ffbe"></a>`owner`: `state_schema.StateConnection`
- <a id="s-313ee9cf74"></a>`unit`: `member`

### Declared structure

- <a id="s-2069068a48"></a>`kind`: `"property"`
- <a id="s-6ed51013b2"></a>`signature`: `"\"(self) -> 'Optional[IsolationLevel]'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-cee0a95882"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.default_isolation_level`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5fe58ff240f12a6f1dcc7dee364d1116fd9251450288683c7f72a53ef6ae2773 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'Optional[IsolationLevel]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "default_isolation_level",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
