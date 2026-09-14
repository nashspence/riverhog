# stove0_target_support.OperationContract.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-operationcontract-v-2da200be3c:b9f549b76c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-42ebdf68b2"></a>
- <a id="s-a926bb6f8c"></a>`distribution`: `stove0-target-support`
- <a id="s-ed4fd09822"></a>`module`: `stove0_target_support`
- <a id="s-80c9719829"></a>`name`: `verify_digest`
- <a id="s-018351b899"></a>`owner`: `stove0_target_support.OperationContract`
- <a id="s-40d4cc2df5"></a>`unit`: `member`

### Declared structure

- <a id="s-1a46085271"></a>`kind`: `"method"`
- <a id="s-6b0d781ba7"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [OperationContract](stove0-target-support-operationcontract.md)

## Governing policies

- <a id="pa-27b18dca4d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources.md#src-3c01163237) — `reference/stove0/packages/target-support/src/stove0_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_support.OperationContract.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 998bb916eabb06ab02548a88289da29af4d8c9e43668a75a87456defb373d17e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "verify_digest",
  "owner": "stove0_target_support.OperationContract",
  "unit": "member"
}
```
