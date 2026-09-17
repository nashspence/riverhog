# stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-mat-4f0dd51939:978ecf4506 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b575d68838"></a>
- <a id="s-11744af710"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-7e62f66d98"></a>`module`: `stove0_review_target_contracts`
- <a id="s-8d3865b40e"></a>`name`: `REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS`
- <a id="s-ccb84aca04"></a>`unit`: `export`

### Declared structure

- <a id="s-4b70342592"></a>`kind`: `"object"`
- <a id="s-c456ebc189"></a>`type`: `"stove0_target_protocol.conformance.SemanticIntentConformanceVectors"`

## Governing policies

- <a id="pa-1928725a39"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources/authorities.md#src-1d0886e380) — [reference/stove0/targets/review/contracts/src/stove0\_review\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5d5241f9b1c3281a3f0aa8adffa5b7fc8fd08981e4ed3aa5ac3aaf3ab2e7c395 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_target_protocol.conformance.SemanticIntentConformanceVectors"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_CONFORMANCE_VECTORS",
  "unit": "export"
}
```

</details>
