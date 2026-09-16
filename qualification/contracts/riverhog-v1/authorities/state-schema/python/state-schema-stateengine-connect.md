# state_schema.StateEngine.connect

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-connect:3c6ad219af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-462fcbcfac"></a>
- <a id="s-9a06efeb99"></a>`distribution`: `state-schema`
- <a id="s-59daac2795"></a>`module`: `state_schema`
- <a id="s-2fc3f83477"></a>`name`: `connect`
- <a id="s-45d89eedc8"></a>`owner`: `state_schema.StateEngine`
- <a id="s-d0090c2320"></a>`unit`: `member`

### Declared structure

- <a id="s-9076fab4c9"></a>`kind`: `"method"`
- <a id="s-61dbf9572a"></a>`signature`: `"\"(self) -> 'Connection'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-bc6ed351d4"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.connect`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d77bb040066d22bb9d9cc579b2f02c5a22a4fe5ff3f82437f2d21911026c87f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Connection'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "connect",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```

</details>
