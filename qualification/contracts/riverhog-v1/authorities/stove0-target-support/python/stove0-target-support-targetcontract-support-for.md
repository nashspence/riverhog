# stove0_target_support.TargetContract.support_for

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontract-support-for:f33ce4c350 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7bd9baeda0"></a>
- <a id="s-186f2a9621"></a>`distribution`: `stove0-target-support`
- <a id="s-c2b6a745c0"></a>`module`: `stove0_target_support`
- <a id="s-fe8289a1c9"></a>`name`: `support_for`
- <a id="s-a8e9b97c50"></a>`owner`: `stove0_target_support.TargetContract`
- <a id="s-24600c138c"></a>`unit`: `member`

### Declared structure

- <a id="s-da9234aee0"></a>`kind`: `"method"`
- <a id="s-5f21fb96bf"></a>`signature`: `"\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""`

## Maintained corroboration

### Related interface records

- [TargetContract](stove0-target-support-targetcontract.md)

## Governing policies

- <a id="pa-1178a37a0c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContract.support_for`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f699b99079d8fcc027853972b04055561e9dd383c8aa2625195747c936b7281 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, operation_id: 'str') -> 'TargetOperationSupport'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "support_for",
  "owner": "stove0_target_support.TargetContract",
  "unit": "member"
}
```

</details>
