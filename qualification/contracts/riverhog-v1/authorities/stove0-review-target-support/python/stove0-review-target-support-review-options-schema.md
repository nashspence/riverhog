# stove0_review_target_support.review_options_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-review-options-schema:304e7da777 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7ebcfed7a2"></a>
- <a id="s-c06cd0bcf8"></a>`distribution`: `stove0-review-target-support`
- <a id="s-a2d9b9f394"></a>`module`: `stove0_review_target_support`
- <a id="s-ccb2a66ae1"></a>`name`: `review_options_schema`
- <a id="s-7f25895c40"></a>`unit`: `export`

### Declared structure

- <a id="s-6948f16fe8"></a>`kind`: `"function"`
- <a id="s-d7b1d2723d"></a>`signature`: `"\"(schema_id: 'str', *, required: 'tuple[str, ...]' = (), properties: 'Mapping[str, JsonValue] \| None' = None) -> 'JsonSchemaDocument'\""`

## Governing policies

- <a id="pa-cf7a0bed09"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.review_options_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 29665f4bbc8e0366d7d75343d2a58391468df895acd56a68bfef30aa684125ad -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(schema_id: 'str', *, required: 'tuple[str, ...]' = (), properties: 'Mapping[str, JsonValue] | None' = None) -> 'JsonSchemaDocument'\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "review_options_schema",
  "unit": "export"
}
```

</details>
