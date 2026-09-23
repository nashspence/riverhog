# stove0_target_support.OperationContract.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-operationcontract-seal:b8ad49feea -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab31078f00"></a>
- <a id="s-f9fdb10b71"></a>`distribution`: `stove0-target-support`
- <a id="s-9cec498d0f"></a>`module`: `stove0_target_support`
- <a id="s-87fe4c9c93"></a>`name`: `seal`
- <a id="s-a0f5401e92"></a>`owner`: `stove0_target_support.OperationContract`
- <a id="s-f73c06c145"></a>`unit`: `member`

### Declared structure

- <a id="s-b52795fa1a"></a>`kind`: `"classmethod"`
- <a id="s-1c91174a0f"></a>`signature`: `"\"(cls, payload: 'OperationContractPayload') -> 'OperationContract'\""`

## Maintained corroboration

### Related interface records

- [OperationContract](stove0-target-support-operationcontract.md)

## Governing policies

- <a id="pa-92401b1da1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.OperationContract.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d0bbae7c2593e47bc0b81de3665633b340b7a49ae76e69c4906d3a7391e84ef -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'OperationContractPayload') -> 'OperationContract'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "seal",
  "owner": "stove0_target_support.OperationContract",
  "unit": "member"
}
```

</details>
