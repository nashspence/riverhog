# stove0_target_support.PersistentTargetService.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-persistenttargetser-7bbb4d3d5e:f8f2176678 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0b1262f960"></a>
- <a id="s-670e33a1a7"></a>`distribution`: `stove0-target-support`
- <a id="s-07967a9b82"></a>`module`: `stove0_target_support`
- <a id="s-7ba7c378ca"></a>`name`: `descriptor`
- <a id="s-58bf5a80ad"></a>`owner`: `stove0_target_support.PersistentTargetService`
- <a id="s-940ff134e0"></a>`unit`: `member`

### Declared structure

- <a id="s-7cbae818a9"></a>`kind`: `"method"`
- <a id="s-24cc96a337"></a>`signature`: `"\"(self) -> 'TargetDescriptor'\""`

## Maintained corroboration

### Related interface records

- [PersistentTargetService](stove0-target-support-persistenttargetservice.md)

## Governing policies

- <a id="pa-e16a0dabdd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.PersistentTargetService.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8abe6557e0d63625c1d5a7871ee8b64b9766f698a762c61feb4f16a16bd75525 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'TargetDescriptor'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "descriptor",
  "owner": "stove0_target_support.PersistentTargetService",
  "unit": "member"
}
```

</details>
