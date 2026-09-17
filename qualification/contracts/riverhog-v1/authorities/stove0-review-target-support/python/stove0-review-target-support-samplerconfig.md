# stove0_review_target_support.SamplerConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-samplerconfig:1d8a4f3b6a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ec7dd37bb4"></a>
- <a id="s-64576c57cd"></a>`distribution`: `stove0-review-target-support`
- <a id="s-5fa4ba6f2f"></a>`module`: `stove0_review_target_support`
- <a id="s-3c67516718"></a>`name`: `SamplerConfig`
- <a id="s-6e71f4a193"></a>`unit`: `export`

### Declared structure

- <a id="s-49214ed10f"></a>`kind`: `"class"`
- <a id="s-1574f9be1e"></a>`signature`: `"\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$')], base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token_file: pathlib.Path, allow_insecure_http: bool = False, descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""`

#### Validated model schema

<a id="s-9bc151343f"></a>

- <a id="s-971ab32c45"></a>`type`: `"object"`
- <a id="s-6a2731b98e"></a>`additionalProperties`: `false`
- <a id="s-1068d7f97b"></a>`required`: `["id","base_url","token_file","descriptor_sha256","image_digest"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7fcd57f9c8"></a>`allow_insecure_http` | no | type="boolean"; default=false |  |
| <a id="s-72fae61c7e"></a>`base_url` | yes | type="string"; maxLength=2048; minLength=1 |  |
| <a id="s-63506a3da1"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-63eed70b10"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-04a91eb91f"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-552d896fe1"></a>`token_file` | yes | type="string"; format="path" |  |

## Maintained corroboration

### Related interface records

- [absolute_token_file](stove0-review-target-support-samplerconfig-absolute-token-file.md)

## Governing policies

- <a id="pa-fb626c40e8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources/authorities.md#src-2a89a71c41) — [reference/stove0/targets/review/support/src/stove0\_review\_target\_support/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_target_support.SamplerConfig`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 91bf018bfe5c27d4874200e423f6cef80f0055c237d7eea2ad8f2940fcd8e68e -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "type": "boolean"
        },
        "base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "type": "string"
        },
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
          "type": "string"
        },
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "token_file": {
          "format": "path",
          "type": "string"
        }
      },
      "required": [
        "id",
        "base_url",
        "token_file",
        "descriptor_sha256",
        "image_digest"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, _PydanticGeneralMetadata(pattern='^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$')], base_url: Annotated[str, MinLen(min_length=1), MaxLen(max_length=2048)], token_file: pathlib.Path, allow_insecure_http: bool = False, descriptor_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')], image_digest: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-review-target-support",
  "module": "stove0_review_target_support",
  "name": "SamplerConfig",
  "unit": "export"
}
```

</details>
