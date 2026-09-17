# state_schema.StateConnection.commit_prepared

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-commit-prepared:37f92edf34 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f22e8c66c2"></a>
- <a id="s-3db4d3ad58"></a>`distribution`: `state-schema`
- <a id="s-ad56219b36"></a>`module`: `state_schema`
- <a id="s-6ebdc0e49a"></a>`name`: `commit_prepared`
- <a id="s-71dea09a53"></a>`owner`: `state_schema.StateConnection`
- <a id="s-fa1d7013b5"></a>`unit`: `member`

### Declared structure

- <a id="s-667e550b7e"></a>`kind`: `"method"`
- <a id="s-452f7620c5"></a>`signature`: `"\"(self, xid: 'Any', recover: 'bool' = False) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-77725c6e0a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.commit_prepared`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4e604c8b04ae1243f6a10bf3dae038d316542b89e8401844a8fda577032f0ca1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, xid: 'Any', recover: 'bool' = False) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "commit_prepared",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
