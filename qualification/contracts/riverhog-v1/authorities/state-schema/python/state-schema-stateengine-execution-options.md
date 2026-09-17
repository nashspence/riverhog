# state_schema.StateEngine.execution_options

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-execution-options:9c14481218 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d7ff7245e9"></a>
- <a id="s-6ec1b6103a"></a>`distribution`: `state-schema`
- <a id="s-41a17785e4"></a>`module`: `state_schema`
- <a id="s-61eade7cec"></a>`name`: `execution_options`
- <a id="s-f3120e6e25"></a>`owner`: `state_schema.StateEngine`
- <a id="s-5da14a2683"></a>`unit`: `member`

### Declared structure

- <a id="s-1e2359bad5"></a>`kind`: `"method"`
- <a id="s-c38dec8115"></a>`signature`: `"\"(self, **opt: 'Any') -> 'OptionEngine'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-5bbbbbfa2b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateEngine.execution_options`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 52ece5b2ecafc8ee027bea1f78bfae9b1ce77c4d525240e0972174004903085d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, **opt: 'Any') -> 'OptionEngine'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "execution_options",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```

</details>
