# review0_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-rclone-re-406fd00710:e744a400f1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3e20a5449a"></a>
- <a id="s-4cc82175b6"></a>`distribution`: `review0-target-contracts`
- <a id="s-432bac5a88"></a>`module`: `review0_target_contracts`
- <a id="s-4dae596032"></a>`name`: `REVIEW_RCLONE_RECEIPT_SCHEMA`
- <a id="s-f618433812"></a>`unit`: `export`

### Declared structure

- <a id="s-0f96839bdb"></a>`kind`: `"object"`
- <a id="s-98d0a036a6"></a>`type`: `"stove0_protocol.models.JsonSchemaValidationProfile"`

## Governing policies

- <a id="pa-ac55a79e0c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_RCLONE_RECEIPT_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a638a9b03bb0c9f51e5b550f51f75bc5e21336a23e8037bbdf7d232e0ccd3aac -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.JsonSchemaValidationProfile"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_RCLONE_RECEIPT_SCHEMA",
  "unit": "export"
}
```

</details>
