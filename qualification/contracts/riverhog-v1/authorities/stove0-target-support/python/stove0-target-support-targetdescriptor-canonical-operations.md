# stove0_target_support.TargetDescriptor.canonical_operations

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetdescriptor-ca-8d8539502f:b3214e4c0e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc1775a3e7"></a>
- <a id="s-ec6fc576d6"></a>`distribution`: `stove0-target-support`
- <a id="s-c6814298ea"></a>`module`: `stove0_target_support`
- <a id="s-00f8ee04af"></a>`name`: `canonical_operations`
- <a id="s-aebbb7c616"></a>`owner`: `stove0_target_support.TargetDescriptor`
- <a id="s-06b5ce634c"></a>`unit`: `member`

### Declared structure

- <a id="s-c15eab552d"></a>`kind`: `"classmethod"`
- <a id="s-7fc9a609e8"></a>`signature`: `"\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""`

## Maintained corroboration

### Related interface records

- [TargetDescriptor](stove0-target-support-targetdescriptor.md)

## Governing policies

- <a id="pa-85b3fbf84e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetDescriptor.canonical_operations`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e13bf442f7b9441e645c5860ec5b9288d13ebd47ed857f2a3dc84a75b14a545 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[TargetOperationSupport, ...]') -> 'tuple[TargetOperationSupport, ...]'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "canonical_operations",
  "owner": "stove0_target_support.TargetDescriptor",
  "unit": "member"
}
```

</details>
