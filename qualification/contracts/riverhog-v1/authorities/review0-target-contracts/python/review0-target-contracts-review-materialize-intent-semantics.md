# review0_target_contracts.REVIEW_MATERIALIZE_INTENT_SEMANTICS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-materiali-056ce4d13d:2ce754caec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-608676e723"></a>
- <a id="s-241ff2539b"></a>`distribution`: `review0-target-contracts`
- <a id="s-64de94f023"></a>`module`: `review0_target_contracts`
- <a id="s-c84504c1d8"></a>`name`: `REVIEW_MATERIALIZE_INTENT_SEMANTICS`
- <a id="s-60679edcea"></a>`unit`: `export`

### Declared structure

- <a id="s-402463e0c0"></a>`kind`: `"object"`
- <a id="s-5d6349868a"></a>`type`: `"stove0_protocol.models.SemanticValidationProfile"`

## Governing policies

- <a id="pa-2644a56fd0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_MATERIALIZE_INTENT_SEMANTICS`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 204f6b3c73f6c490d2ae3e3efd8444b8b244017dbe205ed9444802e3439d8588 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.SemanticValidationProfile"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_SEMANTICS",
  "unit": "export"
}
```

</details>
