# stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-contracts:stove0-review-target-contracts-review-mat-302f820dc0:cf6cd555f1 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1bafa442ee"></a>
- <a id="s-f596e6c8fa"></a>`distribution`: `stove0-review-target-contracts`
- <a id="s-145424d765"></a>`module`: `stove0_review_target_contracts`
- <a id="s-eed2211c4f"></a>`name`: `REVIEW_MATERIALIZE_INTENT_SCHEMA`
- <a id="s-ad967fbb74"></a>`unit`: `export`

### Declared structure

- <a id="s-568b4be440"></a>`kind`: `"object"`
- <a id="s-db932c4876"></a>`type`: `"stove0_protocol.models.JsonSchemaValidationProfile"`

## Governing policies

- <a id="pa-da3290fb25"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-contracts:stove0_review_target_contracts](../../../evidence/sources/authorities.md#src-1d0886e380) — [reference/stove0/targets/review/contracts/src/stove0\_review\_target\_contracts/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/contracts/src/stove0_review_target_contracts/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_contracts.REVIEW_MATERIALIZE_INTENT_SCHEMA`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 510f0392bdb2de5a5b673f0a994238e046510754728434f2ca052ea28b6e7571 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.JsonSchemaValidationProfile"
  },
  "distribution": "stove0-review-target-contracts",
  "module": "stove0_review_target_contracts",
  "name": "REVIEW_MATERIALIZE_INTENT_SCHEMA",
  "unit": "export"
}
```

</details>
