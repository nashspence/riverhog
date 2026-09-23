# review0_target_lib.review_options_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-target-lib:review0-target-lib-review-options-schema:295b5fcec5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-target-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8c42d6a39e"></a>
- <a id="s-ca95c49855"></a>`distribution`: `review0-target-lib`
- <a id="s-e2ac58651d"></a>`module`: `review0_target_lib`
- <a id="s-ac23471cfb"></a>`name`: `review_options_schema`
- <a id="s-f114c89de2"></a>`unit`: `export`

### Declared structure

- <a id="s-f2922f492f"></a>`kind`: `"function"`
- <a id="s-196d7725e5"></a>`signature`: `"\"(schema_id: 'str', *, required: 'tuple[str, ...]' = (), properties: 'Mapping[str, JsonValue] \| None' = None) -> 'JsonSchemaValidationProfile'\""`

## Governing policies

- <a id="pa-643ed429d1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-target-lib:review0_target_lib](../../../evidence/sources/authorities.md#src-665023c8f6) — [some-implementations/stove0/review0/support/src/review0\_target\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/support/src/review0_target_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_target_lib.review_options_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d102f486565f752309c549e3790c8336a3e33c7fc39578d0d9917fb7587a939c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(schema_id: 'str', *, required: 'tuple[str, ...]' = (), properties: 'Mapping[str, JsonValue] | None' = None) -> 'JsonSchemaValidationProfile'\""
  },
  "distribution": "review0-target-lib",
  "module": "review0_target_lib",
  "name": "review_options_schema",
  "unit": "export"
}
```

</details>
