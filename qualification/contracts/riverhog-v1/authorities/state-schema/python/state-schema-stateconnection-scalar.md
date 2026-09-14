# state_schema.StateConnection.scalar

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-scalar:89606628cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-145ea6c783"></a>
- <a id="s-cb6fc3c8e5"></a>`distribution`: `state-schema`
- <a id="s-1cf9919c5c"></a>`module`: `state_schema`
- <a id="s-2a858273e4"></a>`name`: `scalar`
- <a id="s-49a17be3d5"></a>`owner`: `state_schema.StateConnection`
- <a id="s-82a9e08638"></a>`unit`: `member`

### Declared structure

- <a id="s-e75060bc39"></a>`kind`: `"method"`
- <a id="s-e89be21069"></a>`signature`: `"\"(self, statement: 'Executable', parameters: 'Optional[_CoreSingleExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'Any'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-ec2d462852"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.scalar`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86fd598e77bfbceb53d67ed81e3c149a3ac2ed5c9102a64f5e335b361a3e4112 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, statement: 'Executable', parameters: 'Optional[_CoreSingleExecuteParams]' = None, *, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'Any'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "scalar",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
