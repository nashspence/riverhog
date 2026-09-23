# review0_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-contracts:review0-target-contracts-review-materiali-8bbc6878af:2880d6c3ed -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-609287f6cd"></a>
- <a id="s-82d4506842"></a>`distribution`: `review0-target-contracts`
- <a id="s-a9c1b74c86"></a>`module`: `review0_target_contracts`
- <a id="s-ddae32532c"></a>`name`: `REVIEW_MATERIALIZE_INTENT_SCHEMA`
- <a id="s-529d57f336"></a>`unit`: `export`

### Declared structure

- <a id="s-bf24b0b859"></a>`kind`: `"object"`
- <a id="s-ff5a36b3fa"></a>`type`: `"stove0_protocol.models.JsonSchemaValidationProfile"`

## Governing policies

- <a id="pa-35c9637f4f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-contracts:review0_target_contracts](../../../evidence/sources/authorities.md#src-5615f251e5) — [some-implementations/stove0/review0/contracts/src/review0\_target\_contracts/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/contracts/src/review0_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 124f41bd2612da8dfae997de8951f12104700abe91bf32a9e6dc3be3c49e9453 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.JsonSchemaValidationProfile"
  },
  "distribution": "review0-target-contracts",
  "module": "review0_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_SCHEMA",
  "unit": "export"
}
```

</details>
