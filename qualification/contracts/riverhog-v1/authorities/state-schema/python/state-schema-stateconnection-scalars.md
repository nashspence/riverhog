# state_schema.StateConnection.scalars

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-scalars:35b08f524b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-37243998e6"></a>
- <a id="s-e36d0b9181"></a>`distribution`: `state-schema`
- <a id="s-5dc3412484"></a>`module`: `state_schema`
- <a id="s-b593313463"></a>`name`: `scalars`
- <a id="s-687cf1776c"></a>`owner`: `state_schema.StateConnection`
- <a id="s-aaaf43667f"></a>`unit`: `member`

### Declared structure

- <a id="s-1a8256e333"></a>`kind`: `"method"`
- <a id="s-c128b298f0"></a>`signature`: `"\"(self, statement: 'Executable', parameters: 'Optional[_CoreAnyExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'ScalarResult[Any]'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-b48a3b545f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.scalars`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 39222802eebe1b25e6462effa79991c988b49805a61ddc8fa0e7b61fcefaec26 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, statement: 'Executable', parameters: 'Optional[_CoreAnyExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'ScalarResult[Any]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "scalars",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
