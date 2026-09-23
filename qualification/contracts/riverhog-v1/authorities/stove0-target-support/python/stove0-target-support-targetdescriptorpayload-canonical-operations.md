# stove0_target_support.TargetDescriptorPayload.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptorpay-00e268b280:29ee13d753 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-10d5bd787e"></a>
- <a id="s-f18f5f0d3e"></a>`distribution`: `stove0-target-support`
- <a id="s-7fd3fb5cd2"></a>`module`: `stove0_target_support`
- <a id="s-73f45f8b9c"></a>`name`: `canonical_operations`
- <a id="s-835512f357"></a>`owner`: `stove0_target_support.TargetDescriptorPayload`
- <a id="s-97c77cbaa3"></a>`unit`: `member`

### Declared structure

- <a id="s-d9573f3ac7"></a>`kind`: `"classmethod"`
- <a id="s-d4f375136e"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptorPayload](stove0-target-support-targetdescriptorpayload.md)

## Governing policies

- <a id="pa-7003aedc8f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptorPayload.canonical_operations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9b70861af6daa5541e04e1928b70bb9c53c4ef22c545b2c82cffee72dfc9d173 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_operations",
  "owner": "stove0_target_support.TargetDescriptorPayload",
  "unit": "member"
}
```

</details>
