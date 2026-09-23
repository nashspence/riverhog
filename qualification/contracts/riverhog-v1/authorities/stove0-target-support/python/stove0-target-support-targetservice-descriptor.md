# stove0_target_support.TargetService.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetservice-descriptor:dc4b83b308 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99aa9d76ff"></a>
- <a id="s-8ff4d0e725"></a>`distribution`: `stove0-target-support`
- <a id="s-c1425a9cc7"></a>`module`: `stove0_target_support`
- <a id="s-890817a5f4"></a>`name`: `descriptor`
- <a id="s-d900a7c334"></a>`owner`: `stove0_target_support.TargetService`
- <a id="s-0034851f45"></a>`unit`: `member`

### Declared structure

- <a id="s-423f556218"></a>`kind`: `"method"`
- <a id="s-c89967b29f"></a>`signature`: `"\"(self) -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [TargetService](stove0-target-support-targetservice.md)

## Governing policies

- <a id="pa-c944e45259"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetService.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51735ac7c9f0b7b89afa56fb9b7dd90671e1ef0f81372a40b1386bf5f3de0d56 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "descriptor",
  "owner": "stove0_target_support.TargetService",
  "unit": "member"
}
```

</details>
