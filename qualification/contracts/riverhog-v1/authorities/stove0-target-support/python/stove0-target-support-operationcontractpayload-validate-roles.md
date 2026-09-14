# stove0_target_support.OperationContractPayload.validate_roles

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-operationcontractpa-4b027bbe37:f7ef02d935 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7c5ce647a6"></a>
- <a id="s-46405b607a"></a>`distribution`: `stove0-target-support`
- <a id="s-fabf2136c1"></a>`module`: `stove0_target_support`
- <a id="s-1a793ae608"></a>`name`: `validate_roles`
- <a id="s-8b100e1fb4"></a>`owner`: `stove0_target_support.OperationContractPayload`
- <a id="s-600f052ba0"></a>`unit`: `member`

### Declared structure

- <a id="s-aa4ec0f461"></a>`kind`: `"method"`
- <a id="s-e133e9421e"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [stove0_target_support.OperationContractPayload](stove0-target-support-operationcontractpayload.md)

## Governing policies

- <a id="pa-f92a5dc75b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OperationContractPayload.validate_roles`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b116fbaf870350957aa761adb012f671fbfab5eda5ae51a290fd207f0e78b044 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "validate_roles",
  "owner": "stove0_target_support.OperationContractPayload",
  "unit": "member"
}
```
