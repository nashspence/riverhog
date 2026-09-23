# stove0_target_support.TargetContractPayload.bind_result_kind

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-targetcontractpaylo-2cb1768062:be2b339bf2 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ee03d9a3b4"></a>
- <a id="s-769b9ac8f2"></a>`distribution`: `stove0-target-support`
- <a id="s-e00b1d1570"></a>`module`: `stove0_target_support`
- <a id="s-f9515b6ac2"></a>`name`: `bind_result_kind`
- <a id="s-86452ab2c3"></a>`owner`: `stove0_target_support.TargetContractPayload`
- <a id="s-ba68ecb274"></a>`unit`: `member`

### Declared structure

- <a id="s-58c5912179"></a>`kind`: `"method"`
- <a id="s-627d50ff69"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [TargetContractPayload](stove0-target-support-targetcontractpayload.md)

## Governing policies

- <a id="pa-83c78be306"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.TargetContractPayload.bind_result_kind`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b84c87473029bf13f0ec4ea16936caef5e93c97aa724d1c42a0999235e592a10 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "bind_result_kind",
  "owner": "stove0_target_support.TargetContractPayload",
  "unit": "member"
}
```

</details>
