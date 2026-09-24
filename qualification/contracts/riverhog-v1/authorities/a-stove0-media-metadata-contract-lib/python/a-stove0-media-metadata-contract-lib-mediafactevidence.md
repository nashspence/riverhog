# a_stove0_media_metadata_contract_lib.MediaFactEvidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-metadata-contract-lib:a-stove0-media-metadata-contract-lib-medi-aa57bdd837:6e367bccab -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-metadata-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5cbef58e06"></a>
- <a id="s-b7de6fe10b"></a>`distribution`: `a-stove0-media-metadata-contract-lib`
- <a id="s-864235b957"></a>`module`: `a_stove0_media_metadata_contract_lib`
- <a id="s-398a5f6bb9"></a>`name`: `MediaFactEvidence`
- <a id="s-bb3b5079b9"></a>`unit`: `export`

### Declared structure

- <a id="s-13ceed329e"></a>`kind`: `"class"`
- <a id="s-936bbd93a0"></a>`signature`: `"'(*, artifact_id: Annotated[str, MinLen(min_length=1), MaxLen(max_length=160)], field: Annotated[str, MinLen(min_length=1), MaxLen(max_length=240)]) -> None'"`

#### Validated model schema

<a id="s-bf928c807c"></a>

- <a id="s-10cda2170e"></a>`type`: `"object"`
- <a id="s-663c31875e"></a>`additionalProperties`: `false`
- <a id="s-e0164a7c60"></a>`required`: `["artifact_id","field"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9b3328a13e"></a>`artifact_id` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-fcc8051276"></a>`field` | yes | type="string"; maxLength=240; minLength=1 |  |

## Governing policies

- <a id="pa-2af9ce6b96"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-metadata-contract-lib:a_stove0_media_metadata_contract_lib](../../../evidence/sources/authorities.md#src-9ad9aed69d) — [some-implementations/stove0/observers/contracts/media-metadata/src/a\_stove0\_media\_metadata\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/observers/contracts/media-metadata/src/a_stove0_media_metadata_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_metadata_contract_lib.MediaFactEvidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d86dac98f57cf54db0a7de429d5edc27dfdda1d6a02937ecf72bc89306151b9b -->

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
  "distribution": "a-stove0-media-metadata-contract-lib",
  "module": "a_stove0_media_metadata_contract_lib",
  "name": "MediaFactEvidence",
  "unit": "export"
}
```

</details>
