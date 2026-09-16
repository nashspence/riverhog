# state_schema.StateEngine.begin

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-begin:6fe492fa0f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e0aa18abe3"></a>
- <a id="s-3934748c7f"></a>`distribution`: `state-schema`
- <a id="s-6ddf8345a4"></a>`module`: `state_schema`
- <a id="s-74f7085b69"></a>`name`: `begin`
- <a id="s-4cf4171630"></a>`owner`: `state_schema.StateEngine`
- <a id="s-cbcdd7725b"></a>`unit`: `member`

### Declared structure

- <a id="s-1ee8f895b9"></a>`kind`: `"method"`
- <a id="s-35f58ffec3"></a>`signature`: `"\"(self) -> 'Iterator[Connection]'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-a7c0eb74b0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.begin`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 89860e49c3a684578eb18448aac2fcce50f4f7a55297132209508c23721a9cd5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Iterator[Connection]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "begin",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```

</details>
