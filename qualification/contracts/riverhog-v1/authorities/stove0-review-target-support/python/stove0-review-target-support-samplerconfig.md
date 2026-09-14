# stove0_review_target_support.SamplerConfig

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-target-support:stove0-review-target-support-samplerconfig:1d8a4f3b6a -->

Exact externally visible contract owned by this semantic dossier.

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
- <a id="s-3d3d03abb8"></a>`title`: SamplerConfig
- <a id="s-971ab32c45"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7fcd57f9c8"></a>`allow_insecure_http` | no | type="boolean" |  |
| <a id="s-72fae61c7e"></a>`base_url` | yes | type="string"; minLength=1; maxLength=2048 |  |
| <a id="s-63506a3da1"></a>`descriptor_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-63eed70b10"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._-]{0,118}[a-z0-9])?$" |  |
| <a id="s-04a91eb91f"></a>`image_digest` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-552d896fe1"></a>`token_file` | yes | type="string"; format="path" |  |

## Maintained corroboration

### Related interface records

- [stove0_review_target_support.SamplerConfig.absolute_token_file](stove0-review-target-support-samplerconfig-absolute-token-file.md)

## Governing policies

- <a id="pa-fb626c40e8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-review-target-support:stove0_review_target_support](../../../evidence/sources.md#src-2a89a71c41) — `reference/stove0/targets/review/support/src/stove0_review_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_review_target_support.SamplerConfig`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba9abb5229f383d9fd45a7128a64af2c10ceeedb64e35eb467416f1bbb4d70b0 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "additionalProperties": false,
      "properties": {
        "allow_insecure_http": {
          "default": false,
          "title": "Allow Insecure Http",
          "type": "boolean"
        },
        "base_url": {
          "maxLength": 2048,
          "minLength": 1,
          "title": "Base Url",
          "type": "string"
        },
        "descriptor_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Descriptor Sha256",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._-]{0,118}[a-z0-9])?$",
          "title": "Id",
          "type": "string"
        },
        "image_digest": {
          "pattern": "^[0-9a-f]{64}$",
          "title": "Image Digest",
          "type": "string"
        },
        "token_file": {
          "format": "path",
          "title": "Token File",
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
      "title": "SamplerConfig",
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
