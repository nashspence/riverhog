# stove0_target_support.TargetDescriptorPayload.bind_result_kind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptorpay-90fba218ff:3f2d6eab16 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2db5fe8c52"></a>
- <a id="s-00979040df"></a>`distribution`: `stove0-target-support`
- <a id="s-43aee369d8"></a>`module`: `stove0_target_support`
- <a id="s-cdaed85759"></a>`name`: `bind_result_kind`
- <a id="s-d9e8f8de44"></a>`owner`: `stove0_target_support.TargetDescriptorPayload`
- <a id="s-f94ba83c8a"></a>`unit`: `member`

### Declared structure

- <a id="s-18525ad80b"></a>`kind`: `"method"`
- <a id="s-a062d4bdd5"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptorPayload](stove0-target-support-targetdescriptorpayload.md)

## Governing policies

- <a id="pa-56c899b2dd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptorPayload.bind_result_kind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5510c0a8f8ff5f8b6c962cf8eedb3d2aa6c32d5cef88f48b7bf53a1f23e1d189 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "bind_result_kind",
  "owner": "stove0_target_support.TargetDescriptorPayload",
  "unit": "member"
}
```

</details>
