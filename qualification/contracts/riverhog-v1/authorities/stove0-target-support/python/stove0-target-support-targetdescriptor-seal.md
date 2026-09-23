# stove0_target_support.TargetDescriptor.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptor-seal:1b22bdeee4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-81e0013122"></a>
- <a id="s-47956fdc90"></a>`distribution`: `stove0-target-support`
- <a id="s-bf19cb4ac0"></a>`module`: `stove0_target_support`
- <a id="s-40989ba17f"></a>`name`: `seal`
- <a id="s-d1c73b4725"></a>`owner`: `stove0_target_support.TargetDescriptor`
- <a id="s-91f3fc15be"></a>`unit`: `member`

### Declared structure

- <a id="s-c618ccd635"></a>`kind`: `"classmethod"`
- <a id="s-53c3625abd"></a>`signature`: `"\"(cls, payload: 'TargetDescriptorPayload') -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-support-targetdescriptor.md)

## Governing policies

- <a id="pa-9c6fcfb228"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptor.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0fef04a528d6cd6fa6cee8cfa9883d04bac0987ad82ee68f3b3625d854356f8b -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'TargetDescriptorPayload') -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "seal",
  "owner": "stove0_target_support.TargetDescriptor",
  "unit": "member"
}
```

</details>
