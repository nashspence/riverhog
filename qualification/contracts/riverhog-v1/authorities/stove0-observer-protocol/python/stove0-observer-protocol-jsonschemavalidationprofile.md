# stove0_observer_protocol.JsonSchemaValidationProfile

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-jsonschemavalidationprofile:1189e3c1e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5905665a2"></a>
- <a id="s-9d8101848a"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-0b041df951"></a>`module`: `stove0_observer_protocol`
- <a id="s-01a5152a14"></a>`name`: `JsonSchemaValidationProfile`
- <a id="s-6352944db8"></a>`unit`: `export`

### Declared structure

- <a id="s-7b84896e65"></a>`kind`: `"class"`
- <a id="s-c9f6388880"></a>`signature`: `"\"(*, id: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$', ascii_only=None)], profile_sha256: Annotated[str, StringConstraints(strip_whitespace=None, to_upper=None, to_lower=None, strict=None, min_length=None, max_length=None, pattern='^[0-9a-f]{64}$', ascii_only=None)], dialect: Literal['https://json-schema.org/draft/2020-12/schema'] = 'https://json-schema.org/draft/2020-12/schema', format_policy: Literal['annotation-only'] = 'annotation-only', schema: dict[str, JsonValue]) -> None\""`

#### Validated model schema

<a id="s-a2ec783617"></a>

- <a id="s-1d85e1eefc"></a>`type`: `"object"`
- <a id="s-c08d36d524"></a>`additionalProperties`: `false`
- <a id="s-ef2c696735"></a>`required`: `["id","profile_sha256","schema"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-15e0462cd2"></a>`dialect` | no | type="string"; const="https://json-schema.org/draft/2020-12/schema"; default="https://json-schema.org/draft/2020-12/schema" |  |
| <a id="s-223635128f"></a>`format_policy` | no | type="string"; const="annotation-only"; default="annotation-only" |  |
| <a id="s-65f3ce04a1"></a>`id` | yes | type="string"; pattern="^[a-z0-9]&#40;?:[a-z0-9._/-]{0,158}[a-z0-9])?$" |  |
| <a id="s-7923c7dbd6"></a>`profile_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-d8fe9392a5"></a>`schema` | yes | type="object"; additionalProperties=([JsonValue](#s-1a2620c6e1)) |  |

##### Definitions

- [JsonValue](#s-1a2620c6e1)

##### <a id="s-1a2620c6e1"></a>definition `JsonValue`

- Accepts: any JSON value.

## Maintained corroboration

### Related interface records

- [from_schema](stove0-observer-protocol-jsonschemavalidationprofile-from-schema.md)
- [verify_digest](stove0-observer-protocol-jsonschemavalidationprofile-verify-digest.md)

## Governing policies

- <a id="pa-dd4b6a2c53"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [reference/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.JsonSchemaValidationProfile`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79e769bafe898b959b93aed77fac577b3761acf6c6cf5ee289370bba3c8a00e0 -->

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
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "JsonSchemaValidationProfile",
  "unit": "export"
}
```

</details>
