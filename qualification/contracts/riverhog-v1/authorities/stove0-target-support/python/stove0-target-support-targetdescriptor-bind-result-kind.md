# stove0_target_support.TargetDescriptor.bind_result_kind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptor-bi-ce86266b70:5329b0ece5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-18c9ad8164"></a>
- <a id="s-580764b59d"></a>`distribution`: `stove0-target-support`
- <a id="s-a6241efdb1"></a>`module`: `stove0_target_support`
- <a id="s-904c10826e"></a>`name`: `bind_result_kind`
- <a id="s-2eddd0fd3b"></a>`owner`: `stove0_target_support.TargetDescriptor`
- <a id="s-88e3fa3be2"></a>`unit`: `member`

### Declared structure

- <a id="s-d5aafc126a"></a>`kind`: `"method"`
- <a id="s-220280ea4b"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-support-targetdescriptor.md)

## Governing policies

- <a id="pa-e9b657b6c7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptor.bind_result_kind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 01d35a915668f194fdbe6bc259bc3179876c9c743a11ee9602454542b76cdad1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "bind_result_kind",
  "owner": "stove0_target_support.TargetDescriptor",
  "unit": "member"
}
```

</details>
