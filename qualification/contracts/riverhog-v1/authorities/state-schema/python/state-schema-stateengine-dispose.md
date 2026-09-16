# state_schema.StateEngine.dispose

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-dispose:54fe3f0b5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dba17a96ed"></a>
- <a id="s-d3b9f59506"></a>`distribution`: `state-schema`
- <a id="s-d00b93988d"></a>`module`: `state_schema`
- <a id="s-31414d7ecc"></a>`name`: `dispose`
- <a id="s-e9ba87a9c3"></a>`owner`: `state_schema.StateEngine`
- <a id="s-aeabb16547"></a>`unit`: `member`

### Declared structure

- <a id="s-3113668bc5"></a>`kind`: `"method"`
- <a id="s-71e5b41555"></a>`signature`: `"\"(self, close: 'bool' = True) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-355029a541"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.dispose`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e7fa519f55f9c9583b1aae8ba66b4214992f397096ae86f2aff14d48783349b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, close: 'bool' = True) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "dispose",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```

</details>
