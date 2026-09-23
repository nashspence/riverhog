# stove0_target_protocol.EffectPlan.binding_document

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-effectplan-binding-document:8d1bf6503a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c25f5d0865"></a>
- <a id="s-249b985af3"></a>`distribution`: `stove0-target-protocol`
- <a id="s-b2f0b5cd06"></a>`module`: `stove0_target_protocol`
- <a id="s-eb6267b68f"></a>`name`: `binding_document`
- <a id="s-af1de98b5b"></a>`owner`: `stove0_target_protocol.EffectPlan`
- <a id="s-58ae5a40e4"></a>`unit`: `member`

### Declared structure

- <a id="s-b74875c752"></a>`kind`: `"method"`
- <a id="s-52ccccc402"></a>`signature`: `"\"(self) -> 'dict[str, JsonValue]'\""`

## Maintained corroboration

### Related interface records

- [EffectPlan](stove0-target-protocol-effectplan.md)

## Governing policies

- <a id="pa-a22928701b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources/authorities.md#src-f4f0b22026) — [some-implementations/stove0/packages/target-protocol/src/stove0\_target\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_protocol.EffectPlan.binding_document`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d3b1f49eb1a44f65e2d08dac1476c8f0be12c0a509b8c2f94e535c7041990f7a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, JsonValue]'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "binding_document",
  "owner": "stove0_target_protocol.EffectPlan",
  "unit": "member"
}
```

</details>
