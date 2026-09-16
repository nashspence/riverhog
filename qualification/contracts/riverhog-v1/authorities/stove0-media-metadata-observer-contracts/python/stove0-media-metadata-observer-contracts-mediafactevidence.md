# stove0_media_metadata_observer_contracts.MediaFactEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-metadata-observer-contracts:stove0-media-metadata-observer-contracts-63d3e8c6a9:c328030851 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-metadata-observer-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc20045c1e"></a>
- <a id="s-31c1be4d47"></a>`distribution`: `stove0-media-metadata-observer-contracts`
- <a id="s-d0f0408a45"></a>`module`: `stove0_media_metadata_observer_contracts`
- <a id="s-a8d8acdcdc"></a>`name`: `MediaFactEvidence`
- <a id="s-fcd05f3741"></a>`unit`: `export`

### Declared structure

- <a id="s-9a7c8bac29"></a>`kind`: `"class"`
- <a id="s-28a890b041"></a>`signature`: `"'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], field: Annotated[str, MinLen(min_length=1), MaxLen(max_length=240)]) -> None'"`

#### Validated model schema

<a id="s-3efc7d4cd1"></a>

- <a id="s-0eee201dc4"></a>`type`: `"object"`
- <a id="s-395b28a353"></a>`additionalProperties`: `false`
- <a id="s-c23e6201f2"></a>`required`: `["artifact_id","field"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7237631024"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-df5f38ad8a"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

## Governing policies

- <a id="pa-a4747953d1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-metadata-observer-contracts:stove0_media_metadata_observer_contracts](../../../evidence/sources.md#src-8d1649a0f1) — `reference/stove0/observers/contracts/media-metadata/src/stove0_media_metadata_observer_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_metadata_observer_contracts.MediaFactEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 13d8a3b156c3a6cf3adea32cb7682a2a8c80c4b60d1ac71ca2e38b4f45d14c7f -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "artifact_id": {
          "maxLength": 160,
          "minLength": 1,
          "type": "string"
        },
        "field": {
          "maxLength": 240,
          "minLength": 1,
          "type": "string"
        }
      },
      "required": [
        "artifact_id",
        "field"
      ],
      "type": "object"
    },
    "signature": "'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], field: Annotated[str, MinLen(min_length=1), MaxLen(max_length=240)]) -> None'"
  },
  "distribution": "stove0-media-metadata-observer-contracts",
  "module": "stove0_media_metadata_observer_contracts",
  "name": "MediaFactEvidence",
  "unit": "export"
}
```

</details>
