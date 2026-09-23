# stove0_protocol.JsonSchemaValidationProfile

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-jsonschemavalidationprofile:95c4ca9413 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b474adfb9f"></a>
- <a id="s-2f7420c9d9"></a>`distribution`: `stove0-protocol`
- <a id="s-2f0f6bd2e8"></a>`module`: `stove0_protocol`
- <a id="s-3561e7d467"></a>`name`: `JsonSchemaValidationProfile`
- <a id="s-5c08d6ec75"></a>`unit`: `export`

### Declared structure

- <a id="s-d8ceb85f45"></a>`kind`: `"class"`
- <a id="s-42487dea54"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], profile_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-30cfb71492"></a>

- <a id="s-d3bb774361"></a>`type`: `"object"`
- <a id="s-0ad3cea595"></a>`additionalProperties`: `false`
- <a id="s-1538c8f0ab"></a>`required`: `["id","profile_sha256","schema"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0bff13eb5a"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-e68a5054c1"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-eaf13afbae"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-e96b4e0bea"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f49c71d0a7"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-8a1545179b)) |  |

##### Definitions

- [JsonValue](#s-8a1545179b)

##### <a id="s-8a1545179b"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [verify_digest](stove0-protocol-jsonschemavalidationprofile-verify-digest.md)
- [from_schema](stove0-protocol-jsonschemavalidationprofile-from-schema.md)

## Governing policies

- <a id="pa-1583e5d165"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources/authorities.md#src-084138045e) — [some-implementations/stove0/packages/protocol/src/stove0\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/protocol/src/stove0_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_protocol.JsonSchemaValidationProfile`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7326185082cf73aa068284cf6d1a27aa8b9eb6475f90372cc5e7d96e2665c6ad -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "JsonValue": {}
      },
      "additionalProperties": false,
      "properties": {
        "dialect": {
          "const": "https://json-schema.org/draft/2020-12/schema",
          "default": "https://json-schema.org/draft/2020-12/schema",
          "type": "string"
        },
        "format_policy": {
          "const": "annotation-only",
          "default": "annotation-only",
          "type": "string"
        },
        "id": {
          "pattern": "^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$",
          "type": "string"
        },
        "profile_sha256": {
          "pattern": "^[0-9a-f]{64}$",
          "type": "string"
        },
        "schema": {
          "additionalProperties": {
            "$ref": "#/$defs/JsonValue"
          },
          "type": "object"
        }
      },
      "required": [
        "id",
        "profile_sha256",
        "schema"
      ],
      "type": "object"
    },
    "signature": "\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]\u0028?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], profile_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "JsonSchemaValidationProfile",
  "unit": "export"
}
```

</details>
