# stove0_target_protocol.TransformPlan.binding_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-transformplan-bind-c0640735a4:82d3041ad5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10a43e9d3c"></a>
- <a id="s-5837aa8115"></a>`distribution`: `stove0-target-protocol`
- <a id="s-191ef6d023"></a>`module`: `stove0_target_protocol`
- <a id="s-acf91c281c"></a>`name`: `binding_document`
- <a id="s-f33d1888bc"></a>`owner`: `stove0_target_protocol.TransformPlan`
- <a id="s-9a3263cea6"></a>`unit`: `member`

### Declared structure

- <a id="s-7c45c40f7f"></a>`kind`: `"method"`
- <a id="s-2a99c8fecb"></a>`signature`: `"\"(self) -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [TransformPlan](stove0-target-protocol-transformplan.md)

## Governing policies

- <a id="pa-237e877f31"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — [reference/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.TransformPlan.binding_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0132eb217ca3f23c7f32467dfa94276857387552fa5dd8d7c5875b2999dc22c5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "binding_document",
  "owner": "stove0_target_protocol.TransformPlan",
  "unit": "member"
}
```

</details>
