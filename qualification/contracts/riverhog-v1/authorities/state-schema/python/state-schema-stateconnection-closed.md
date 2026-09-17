# state_schema.StateConnection.closed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-closed:83ec96581b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-217dcf6a7d"></a>
- <a id="s-f4a2df843a"></a>`distribution`: `state-schema`
- <a id="s-9f62e0ba78"></a>`module`: `state_schema`
- <a id="s-c4fb6d965f"></a>`name`: `closed`
- <a id="s-41f0d3a033"></a>`owner`: `state_schema.StateConnection`
- <a id="s-d8b023cef5"></a>`unit`: `member`

### Declared structure

- <a id="s-4c0c672405"></a>`kind`: `"property"`
- <a id="s-210b2ed0a5"></a>`signature`: `"\"(self) -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-a236a9b490"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.closed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c8f8640c4965167a346d2e1d04dfb4be7ea4dd2fb7088ec7c4e437a07f7f547c -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "closed",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
