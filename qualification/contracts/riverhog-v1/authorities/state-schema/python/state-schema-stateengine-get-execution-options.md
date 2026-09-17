# state_schema.StateEngine.get_execution_options

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-get-execution-options:851e294763 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3e280cc3ec"></a>
- <a id="s-4771d99e0c"></a>`distribution`: `state-schema`
- <a id="s-96c502c2a4"></a>`module`: `state_schema`
- <a id="s-d0e8d1e314"></a>`name`: `get_execution_options`
- <a id="s-4498aaa596"></a>`owner`: `state_schema.StateEngine`
- <a id="s-e94e6bd5ac"></a>`unit`: `member`

### Declared structure

- <a id="s-19195beb16"></a>`kind`: `"method"`
- <a id="s-3b1c1d3df2"></a>`signature`: `"\"(self) -> '_ExecuteOptions'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-a6d636988f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateEngine.get_execution_options`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e473459371c6140e55ba246f0006e7fb66fd4cb370e92832a5bb61999567590b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> '_ExecuteOptions'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "get_execution_options",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```

</details>
