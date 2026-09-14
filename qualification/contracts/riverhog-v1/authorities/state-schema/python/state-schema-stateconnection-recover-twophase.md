# state_schema.StateConnection.recover_twophase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-recover-twophase:f205226113 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2757c4b336"></a>
- <a id="s-22322bc6b5"></a>`distribution`: `state-schema`
- <a id="s-099a142de1"></a>`module`: `state_schema`
- <a id="s-57d643c7ee"></a>`name`: `recover_twophase`
- <a id="s-8a1d80a8ee"></a>`owner`: `state_schema.StateConnection`
- <a id="s-4f768f03c4"></a>`unit`: `member`

### Declared structure

- <a id="s-37fdcdd289"></a>`kind`: `"method"`
- <a id="s-4e3948e57c"></a>`signature`: `"\"(self) -> 'List[Any]'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-d04d8e9fd5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.recover_twophase`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 64f891bbaf2f7d64b4979e1136976d9b6063a0d0849708437d7fe778fe946b53 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'List[Any]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "recover_twophase",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
