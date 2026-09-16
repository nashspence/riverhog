# state_schema.StateConnection.invalidate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-invalidate:d01d719fe4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3700250013"></a>
- <a id="s-a9da8b13d7"></a>`distribution`: `state-schema`
- <a id="s-b51a37b01f"></a>`module`: `state_schema`
- <a id="s-3d1214a62a"></a>`name`: `invalidate`
- <a id="s-5b92b0cbd4"></a>`owner`: `state_schema.StateConnection`
- <a id="s-53a6759e8c"></a>`unit`: `member`

### Declared structure

- <a id="s-b2debc5ad8"></a>`kind`: `"method"`
- <a id="s-456473f015"></a>`signature`: `"\"(self, exception: 'Optional[BaseException]' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-1fc358beae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.invalidate`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d68883a30b4faec2a26b63f4aa39dccbefde6604bf037b3b9011a4547322e669 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, exception: 'Optional[BaseException]' = None) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "invalidate",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
