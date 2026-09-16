# stove0_protocol.TargetPlanBinding.require_plan_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-targetplanbinding-require-d46de2b597:afcab71181 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-197088c886"></a>
- <a id="s-3cd8409b89"></a>`distribution`: `stove0-protocol`
- <a id="s-7aa9c37253"></a>`module`: `stove0_protocol`
- <a id="s-b69f3d14f5"></a>`name`: `require_plan_document`
- <a id="s-218525f112"></a>`owner`: `stove0_protocol.TargetPlanBinding`
- <a id="s-bad0daf397"></a>`unit`: `member`

### Declared structure

- <a id="s-7314157658"></a>`kind`: `"classmethod"`
- <a id="s-70db3dc417"></a>`signature`: `"\"(cls, value: 'dict[str, JsonValue]') -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [TargetPlanBinding](stove0-protocol-targetplanbinding.md)

## Governing policies

- <a id="pa-476027932e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.TargetPlanBinding.require_plan_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 311dc71a900c28f5fde720fdd4a0b6c7108748e53bf23d78ba6528e1972c5b88 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'dict[str, JsonValue]') -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "require_plan_document",
  "owner": "stove0_protocol.TargetPlanBinding",
  "unit": "member"
}
```

</details>
