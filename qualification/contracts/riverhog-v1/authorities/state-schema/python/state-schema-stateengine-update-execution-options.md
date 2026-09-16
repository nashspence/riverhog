# state_schema.StateEngine.update_execution_options

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-update-execution-options:db39852da9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4939aa42b8"></a>
- <a id="s-35db1334fd"></a>`distribution`: `state-schema`
- <a id="s-ae08a589a0"></a>`module`: `state_schema`
- <a id="s-dac8c9f502"></a>`name`: `update_execution_options`
- <a id="s-0ce108bc7e"></a>`owner`: `state_schema.StateEngine`
- <a id="s-16a21a411a"></a>`unit`: `member`

### Declared structure

- <a id="s-9870a37ae5"></a>`kind`: `"method"`
- <a id="s-119a7cf94b"></a>`signature`: `"\"(self, **opt: 'Any') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-8321e3c78f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.update_execution_options`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2efd05529b67a81fb92f692182235b589849fd2af27d5bf17a29f2e079b9f59f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, **opt: 'Any') -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "update_execution_options",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```

</details>
